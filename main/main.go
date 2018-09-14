package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventstore-models/bootstrap"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-eventstore-query/ioutil"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"
)

// kafkaIO is a convenient function to initialize KafkaIO.
func kafkaIO() (*KafkaIO, error) {
	brokers := os.Getenv("KAFKA_BROKERS")
	consumerGroupName := os.Getenv("KAFKA_REQUEST_SINK_CONSUMER_GROUP_NAME")
	consumerTopics := os.Getenv("KAFKA_REQUEST_SINK_TOPIC")
	responseTopic := os.Getenv("KAFKA_EVENT_RESPONSE_TOPIC")

	config := KafkaConfig{
		Brokers:           *commonutil.ParseHosts(brokers),
		ConsumerGroupName: consumerGroupName,
		ConsumerTopics:    *commonutil.ParseHosts(consumerTopics),
		ResponseTopic:     responseTopic,
	}

	return InitKafkaIO(config)
}

func main() {
	// Load environment-file.
	// Env vars will be read directly from environment if this file fails loading
	err := godotenv.Load()
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"CASSANDRA_HOSTS",
		"CASSANDRA_DATA_CENTERS",
		"CASSANDRA_KEYSPACE",
		"CASSANDRA_EVENT_TABLE",
		"CASSANDRA_EVENT_META_TABLE",
		"KAFKA_BROKERS",
		"KAFKA_REQUEST_SINK_CONSUMER_GROUP_NAME",
		"KAFKA_REQUEST_SINK_TOPIC",
		"KAFKA_EVENT_BATCH_SIZE",
		"KAFKA_EVENT_RESPONSE_TOPIC",
	)
	// Some env-var is missing. Error-msg will be printed by util-lib itself.
	if err != nil {
		log.Fatalf(
			"Error: Environment variable %s is required but was not found", missingVar,
		)
	}

	kio, err := kafkaIO()
	if err != nil {
		err = errors.Wrap(err, "Failed Initializing KafkaIO")
		log.Fatalln(err)
	}

	go func() {
		for err := range kio.consumerErrChan {
			err = errors.Wrap(err, "EventsQuery Consumer Error")
			log.Println(
				"Error in events-query consumer. " +
					"The events cannot be consumed without a working Kafka Consumer. " +
					"The service will now exit.",
			)
			log.Fatalln(err)
		}
	}()
	go func() {
		for err := range kio.producerErrChan {
			wrapErr := errors.Wrap(err, "Kafka Producer Error")
			log.Println(
				"Error in response producer. " +
					"The responses cannot be produced without a working Kafka Producer. " +
					"The service will now exit.",
			)
			log.Fatalln(wrapErr)
		}
	}()

	eventTable, err := bootstrap.Event()
	if err != nil {
		err = errors.Wrap(err, "Error Bootstrapping Event-Table")
		log.Fatalln(err)
	}
	log.Println("Bootstrapped Event-Table")
	log.Println("Bootstrapping EventMeta-Table")
	eventMetaTable, err := bootstrap.EventMeta()
	if err != nil {
		err = errors.Wrap(err, "Error Bootstrapping EventMeta-Table")
		log.Fatalln(err)
	}
	log.Println("Bootstrapped EventMeta-Table")

	log.Println("Bootstrapping Event-Table")

	dbUtil := &ioutil.DBUtil{
		EventMetaTable: eventMetaTable,
		EventTable:     eventTable,
	}
	queryUtil := &ioutil.QueryUtil{
		DBUtil:       dbUtil,
		ResponseChan: kio.ProducerInput(),
	}
	for queryMsg := range kio.consumerMsgChan {
		go processQuery(queryUtil, queryMsg, kio.MarkOffset())
	}
}

// processQuery handles the request-query from Kafka consumer.
// "Query" here refers to the request by an Aggregate to get new events.
func processQuery(
	queryUtil *ioutil.QueryUtil,
	queryMsg *sarama.ConsumerMessage,
	markOffset chan<- *sarama.ConsumerMessage,
) {
	// Whether the processing fails or succeeds, we mark this request as resolved
	// The service-user can examine the response and proceed as required
	markOffset <- queryMsg

	// Unmarshal received query
	query := &model.EventStoreQuery{}
	err := json.Unmarshal(queryMsg.Value, query)
	if err != nil {
		err = errors.Wrap(err, "Error Unmarshalling EventQuery")
		// We don't send message over Kafka producer here since we don't know AggregateID
		log.Println(err)
		log.Println("The query-offset will be marked as committed.")
		return
	}

	events, err := queryUtil.QueryHandler(query)
	if err != nil {
		err = errors.Wrap(err, "Error Processing Query")
		queryUtil.ResponseChan <- &model.KafkaResponse{
			AggregateID: query.AggregateID,
			Error:       err.Error(),
		}
		log.Println(err)
		return
	}
	queryUtil.BatchProduce(query.AggregateID, events)
}
