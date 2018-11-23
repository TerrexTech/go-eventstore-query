package test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/uuuid"

	"github.com/TerrexTech/go-common-models/bootstrap"
	"github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/joho/godotenv"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// This suite only tests if the kafka-response appears on the response-topic
// after the request is processed by the service from request-topic.
// The actual validation of data is done by the included unit tests themselves.
func TestEventQuery(t *testing.T) {
	// Load environment-file.
	// Env vars will be read directly from environment if this file fails loading
	err := godotenv.Load("../.env")
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
		"KAFKA_CONSUMER_TOPIC",
	)

	if err != nil {
		err = errors.Wrapf(err, "Env-var %s is required, but is not set", missingVar)
		log.Fatalln(err)
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "EventQuery Suite")
}

var _ = Describe("EventQuery", func() {
	var (
		brokers           *[]string
		consumerGroupName string
		responseTopic     string

		mockEventStoreQuery *model.EventStoreQuery
	)

	BeforeSuite(func() {
		Describe("A request is consumed", func() {
			log.Println("Reading environment file")
			err := godotenv.Load("../.env")
			Expect(err).ToNot(HaveOccurred())

			// Add some test-data
			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			eventTable, err := bootstrap.Event()
			Expect(err).ToNot(HaveOccurred())
			mockEvent := &model.Event{
				AggregateID: 1,
				Version:     10,
				YearBucket:  2018,
				Data:        []byte("test"),
				UserUUID:    uuid,
				NanoTime:    time.Now().UnixNano(),
				UUID:        uuid,
				Action:      "insert-test",
			}
			err = <-eventTable.AsyncInsert(mockEvent)
			Expect(err).ToNot(HaveOccurred())
			// Insert some varied versions for testing
			uuid, err = uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockEvent.UUID = uuid
			mockEvent.Version = 11
			err = <-eventTable.AsyncInsert(&mockEvent)
			Expect(err).ToNot(HaveOccurred())

			uuid, err = uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockEvent.UUID = uuid
			mockEvent.Version = 13
			err = <-eventTable.AsyncInsert(&mockEvent)
			Expect(err).ToNot(HaveOccurred())

			eventMetaTable, err := bootstrap.EventMeta()
			query := &model.EventMeta{
				AggregateID:      1,
				AggregateVersion: 20,
				PartitionKey:     0,
			}
			err = <-eventMetaTable.AsyncInsert(query)
			Expect(err).ToNot(HaveOccurred())

			brokers = commonutil.ParseHosts(os.Getenv("KAFKA_BROKERS"))
			consumerGroupName = "test-consumer"
			consumerTopic := os.Getenv("KAFKA_CONSUMER_TOPIC")

			config := &kafka.ProducerConfig{
				KafkaBrokers: *brokers,
			}

			log.Println("Creating Kafka mock-event Producer")
			kafkaProducer, err := kafka.NewProducer(config)
			Expect(err).ToNot(HaveOccurred())

			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			var aggID int8 = 1
			responseTopic = fmt.Sprintf("test.%d", aggID)
			mockEventStoreQuery = &model.EventStoreQuery{
				AggregateID:      aggID,
				AggregateVersion: 1,
				CorrelationID:    cid,
				Topic:            responseTopic,
				UUID:             uuid,
				YearBucket:       mockEvent.YearBucket,
			}

			metaAggVersion := int64(50)

			// Create event-meta table for controlling event-versions
			metaTable, err := bootstrap.EventMeta()
			Expect(err).ToNot(HaveOccurred())
			metaData := model.EventMeta{
				AggregateVersion: metaAggVersion,
				AggregateID:      mockEventStoreQuery.AggregateID,
				PartitionKey:     0,
			}
			err = <-metaTable.AsyncInsert(metaData)
			Expect(err).ToNot(HaveOccurred())

			// Produce event on Kafka topic
			log.Println("Marshalling mock-eventstore-query to json")
			testEventQuery, err := json.Marshal(mockEventStoreQuery)
			Expect(err).ToNot(HaveOccurred())

			log.Println("Fetching input-channel from mock-eventstore-query producer")
			mockEventQueryInput := kafkaProducer.Input()

			mockEventQueryInput <- kafka.CreateMessage(consumerTopic, testEventQuery)
			log.Println("Produced mock-eventstore-query on consumer-topic")
		})
	})

	Context("An event is produced", func() {
		var responseConsumer *kafka.Consumer

		BeforeEach(func() {
			var err error

			if responseConsumer == nil {
				consumerConfig := &kafka.ConsumerConfig{
					GroupName:    consumerGroupName,
					KafkaBrokers: *brokers,
					Topics:       []string{responseTopic},
				}
				responseConsumer, err = kafka.NewConsumer(consumerConfig)
				Expect(err).ToNot(HaveOccurred())
			}
		})

		// This test will run in a go-routine, and must succeed within 15 seconds
		Specify(
			"the mock-event processing-result should appear on Kafka response-topic",
			func(done Done) {
				log.Println(
					"Checking if the Kafka response-topic received the event, " +
						"with timeout of 15 seconds",
				)

				// No errors should appear on response-consumer
				go func() {
					defer GinkgoRecover()
					for consumerErr := range responseConsumer.Errors() {
						Expect(consumerErr).ToNot(HaveOccurred())
					}
				}()

				msgCallback := func(msg *sarama.ConsumerMessage) bool {
					defer GinkgoRecover()
					// Unmarshal the Kafka-Response
					log.Println("An Event was received, now verifying")
					response := &model.Document{}
					err := json.Unmarshal(msg.Value, response)
					Expect(err).ToNot(HaveOccurred())

					if response.UUID == mockEventStoreQuery.CorrelationID {
						Expect(response.Error).To(BeEmpty())
						log.Println("Event matched expectation")

						// Unmarshal the Result from Kafka-Response
						events := []model.Event{}
						err = json.Unmarshal(response.Data, &events)
						Expect(err).ToNot(HaveOccurred())
						return true
					}
					return false
				}
				handler := &msgHandler{msgCallback}
				ctx, cancel := context.WithTimeout(
					context.Background(),
					time.Duration(15)*time.Second,
				)
				defer cancel()
				responseConsumer.Consume(ctx, handler)
				close(done)
			}, 15,
		)
	})
})
