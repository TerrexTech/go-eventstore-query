package main

import (
	"context"
	"log"
	"os"
	"strconv"

	"github.com/TerrexTech/go-eventstore-query/ioutil"
	"github.com/TerrexTech/go-kafkautils/kafka"

	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventstore-models/bootstrap"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"
)

// validateEnv checks if all required environment-variables are set.
func validateEnv() {
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
		"CASSANDRA_EVENT_META_PARTITION_KEY",

		"KAFKA_BROKERS",
		"KAFKA_CONSUMER_GROUP",
		"KAFKA_CONSUMER_TOPICS",
		"KAFKA_RESPONSE_TOPIC",
	)

	if err != nil {
		err = errors.Wrapf(err, "Env-var %s is required, but is not set", missingVar)
		log.Fatalln(err)
	}
}

func main() {
	validateEnv()

	// ======> Setup Kafka
	brokersStr := os.Getenv("KAFKA_BROKERS")
	brokers := *commonutil.ParseHosts(brokersStr)

	consumerGroup := os.Getenv("KAFKA_CONSUMER_GROUP")
	esQueryTopicStr := os.Getenv("KAFKA_CONSUMER_TOPICS")
	esQueryTopic := *commonutil.ParseHosts(esQueryTopicStr)
	// EventStoreQuery Consumer
	esQueryConsumer, err := kafka.NewConsumer(&kafka.ConsumerConfig{
		GroupName:    consumerGroup,
		KafkaBrokers: brokers,
		Topics:       esQueryTopic,
	})
	if err != nil {
		err = errors.Wrap(err, "Error creating Kafka Event-Consumer")
		log.Fatalln(err)
	}

	// Handle Consumer Errors
	go func() {
		for err := range esQueryConsumer.Errors() {
			err = errors.Wrap(err, "Kafka Consumer Error")
			log.Println(
				"Error in event consumer. " +
					"The queries cannot be consumed without a working Kafka Consumer. " +
					"The service will now exit.",
			)
			log.Fatalln(err)
		}
	}()

	// Response Producer
	responseProducer, err := kafka.NewProducer(&kafka.ProducerConfig{
		KafkaBrokers: brokers,
	})
	if err != nil {
		err = errors.Wrap(err, "Error creating Kafka Response-Producer")
		log.Fatalln(err)
	}

	// Handle Producer Errors
	go func() {
		for prodErr := range responseProducer.Errors() {
			err = errors.Wrap(prodErr.Err, "Kafka Producer Error")
			log.Println(
				"Error in response producer. " +
					"The responses cannot be produced without a working Kafka Producer. " +
					"The service will now exit.",
			)
			log.Fatalln(err)
		}
	}()

	// ======> Create/Load Cassandra Keyspace/Tables
	log.Println("Bootstrapping Event table")
	eventTable, err := bootstrap.Event()
	if err != nil {
		err = errors.Wrap(err, "EventTable: Error getting Table")
		log.Fatalln(err)
	}
	log.Println("Bootstrapping EventMeta table")
	eventMetaTable, err := bootstrap.EventMeta()
	if err != nil {
		err = errors.Wrap(err, "EventMetaTable: Error getting Table")
		log.Fatalln(err)
	}

	// ======> Setup EventStore
	pKeyVar := "CASSANDRA_EVENT_META_PARTITION_KEY"
	metaPartitionKey, err := strconv.Atoi(os.Getenv(pKeyVar))
	if err != nil {
		err = errors.Wrap(err, pKeyVar+" must be a valid integer")
		log.Fatalln(err)
	}

	eventStore, err := ioutil.NewEventStore(eventTable, eventMetaTable, int8(metaPartitionKey))
	if err != nil {
		err = errors.Wrap(err, "Error while initializing EventStore")
		log.Fatalln(err)
	}

	// ======> Setup EventHandler
	defaultSize := 6
	batchSize, err := strconv.Atoi(os.Getenv("KAFKA_EVENT_BATCH_SIZE"))
	if err != nil {
		err = errors.Wrap(err, "Error Getting Event-Producer BatchSize")
		log.Println(err)
		log.Printf("A default batch-size of %d will be used", defaultSize)
		batchSize = defaultSize
	}

	queryUtil := &ioutil.QueryUtil{
		EventStore: eventStore,
		BatchSize:  batchSize,
	}
	responseTopic := os.Getenv("KAFKA_RESPONSE_TOPIC")

	handler, err := NewEventHandler(EventHandlerConfig{
		EventStore:       eventStore,
		QueryUtil:        queryUtil,
		ResponseProducer: responseProducer,
		ResponseTopic:    responseTopic,
	})
	if err != nil {
		err = errors.Wrap(err, "Error while initializing EventHandler")
		log.Fatalln(err)
	}

	log.Println("EventStoreQuery Service Initialized")

	err = esQueryConsumer.Consume(context.Background(), handler)
	if err != nil {
		err = errors.Wrap(err, "Error while attempting to consume Events")
		log.Fatalln(err)
	}
}
