package test

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/TerrexTech/go-commonutils/commonutil"
	cql "github.com/gocql/gocql"
	"github.com/TerrexTech/go-eventstore-models/bootstrap"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/consumer"
	"github.com/TerrexTech/go-kafkautils/producer"
	"github.com/joho/godotenv"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// This suite only tests if the kafka-response appears on the response-topic
// after the request is processed by the service from request-topic.
// The actual validation of data is done by the included unit tests themselves.
func TestEventQuery(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EventQuery Suite")
}

var _ = Describe("EventQuery", func() {
	var (
		brokers           *[]string
		consumerGroupName string
		responseTopic     string
	)

	BeforeSuite(func() {
		Describe("A request is consumed", func() {
			log.Println("Reading environment file")
			err := godotenv.Load("../.env")
			Expect(err).ToNot(HaveOccurred())

			// Add some test-data
			uuid, err := cql.RandomUUID()
			Expect(err).ToNot(HaveOccurred())

			eventTable, err := bootstrap.Event()
			Expect(err).ToNot(HaveOccurred())
			mockEvent := &model.Event{
				AggregateID: 1,
				Version:     10,
				YearBucket:  2019,
				Data:        "test",
				UserID:      3,
				Timestamp:   time.Now(),
				UUID:        uuid,
				Action:      "insert",
			}
			err = <-eventTable.AsyncInsert(mockEvent)
			Expect(err).ToNot(HaveOccurred())
			// Insert some varied versions for testing
			mockEvent.Version = 11
			err = <-eventTable.AsyncInsert(&mockEvent)
			Expect(err).ToNot(HaveOccurred())
			mockEvent.Version = 13
			err = <-eventTable.AsyncInsert(&mockEvent)
			Expect(err).ToNot(HaveOccurred())

			eventMetaTable, err := bootstrap.EventMeta()
			query := &model.EventMeta{
				AggregateID:      1,
				AggregateVersion: 20,
				YearBucket:       2019,
			}
			err = <-eventMetaTable.AsyncInsert(query)
			Expect(err).ToNot(HaveOccurred())

			brokers = commonutil.ParseHosts(os.Getenv("KAFKA_BROKERS"))
			consumerGroupName = "test-consumer"
			consumerTopic := os.Getenv("KAFKA_REQUEST_SINK_TOPIC")

			config := &producer.Config{
				KafkaBrokers: *brokers,
			}

			log.Println("Creating Kafka mock-event Producer")
			kafkaProducer, err := producer.New(config)
			Expect(err).ToNot(HaveOccurred())

			mockEventStoreQuery := &model.EventStoreQuery{
				AggregateID:      1,
				AggregateVersion: 1,
				YearBucket:       2019,
			}
			responseTopic = fmt.Sprintf(
				"%s.%d",
				os.Getenv("KAFKA_EVENT_RESPONSE_TOPIC"),
				mockEventStoreQuery.AggregateID,
			)
			metaAggVersion := int64(50)

			// Create event-meta table for controlling event-versions
			metaTable, err := bootstrap.EventMeta()
			Expect(err).ToNot(HaveOccurred())
			metaData := model.EventMeta{
				AggregateVersion: metaAggVersion,
				AggregateID:      mockEventStoreQuery.AggregateID,
				YearBucket:       mockEventStoreQuery.YearBucket,
			}
			err = <-metaTable.AsyncInsert(metaData)
			Expect(err).ToNot(HaveOccurred())

			// Produce event on Kafka topic
			log.Println("Marshalling mock-eventstore-query to json")
			testEventQuery, err := json.Marshal(mockEventStoreQuery)
			Expect(err).ToNot(HaveOccurred())

			log.Println("Fetching input-channel from mock-eventstore-query producer")
			mockEventQueryInput, err := kafkaProducer.Input()
			Expect(err).ToNot(HaveOccurred())

			mockEventQueryInput <- producer.CreateMessage(consumerTopic, testEventQuery)
			log.Println("Produced mock-eventstore-query on consumer-topic")
		})
	})

	Context("An event is produced", func() {
		var responseConsumer *consumer.Consumer

		BeforeEach(func() {
			var err error

			if responseConsumer == nil {
				consumerConfig := &consumer.Config{
					ConsumerGroup: consumerGroupName,
					KafkaBrokers:  *brokers,
					Topics:        []string{responseTopic},
				}
				responseConsumer, err = consumer.New(consumerConfig)
				Expect(err).ToNot(HaveOccurred())
			}
		})

		Specify("no errors should appear on response-consumer", func() {
			go func() {
				defer GinkgoRecover()
				for consumerErr := range responseConsumer.Errors() {
					Expect(consumerErr).ToNot(HaveOccurred())
				}
			}()
		})

		// This test will run in a go-routine, and must succeed within 10 seconds
		Specify(
			"the mock-event processing-result should appear on Kafka response-topic",
			func(done Done) {
				log.Println(
					"Checking if the Kafka response-topic received the event, " +
						"with timeout of 10 seconds",
				)

				for msg := range responseConsumer.Messages() {
					// Mark the message-offset since we do not want the
					// same message to appear again in later tests.
					responseConsumer.MarkOffset(msg, "")
					err := responseConsumer.SaramaConsumerGroup().CommitOffsets()
					Expect(err).ToNot(HaveOccurred())

					// Unmarshal the Kafka-Response
					log.Println("An Event was received, now verifying")
					response := &model.KafkaResponse{}
					err = json.Unmarshal(msg.Value, response)

					Expect(err).ToNot(HaveOccurred())
					Expect(response.Error).To(BeEmpty())

					// Unmarshal the Result from Kafka-Response
					events := &[]model.Event{}
					err = json.Unmarshal([]byte(response.Result), events)
					Expect(err).ToNot(HaveOccurred())

					close(done)
				}
			}, 10)
	})
})
