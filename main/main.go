package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/TerrexTech/go-common-models/model"

	"github.com/TerrexTech/go-common-models/bootstrap"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventstore-query/ioutil"
	"github.com/TerrexTech/go-kafkautils/kafka"
	tlog "github.com/TerrexTech/go-logtransport/log"
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
		"SERVICE_NAME",
		"KAFKA_LOG_PRODUCER_TOPIC",

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
}

func main() {
	validateEnv()

	// ======> Setup Kafka
	brokersStr := os.Getenv("KAFKA_BROKERS")
	brokers := *commonutil.ParseHosts(brokersStr)

	logTopic := os.Getenv("KAFKA_LOG_PRODUCER_TOPIC")
	serviceName := os.Getenv("SERVICE_NAME")

	prodConfig := &kafka.ProducerConfig{
		KafkaBrokers: brokers,
	}
	logger, err := tlog.Init(nil, serviceName, prodConfig, logTopic)
	if err != nil {
		err = errors.Wrap(err, "Error initializing Logger")
		log.Fatalln(err)
	}

	consumerGroup := os.Getenv("KAFKA_CONSUMER_GROUP")
	esQueryTopicStr := os.Getenv("KAFKA_CONSUMER_TOPIC")
	esQueryTopic := *commonutil.ParseHosts(esQueryTopicStr)

	// EventStoreQuery Consumer
	esQueryConsumer, err := kafka.NewConsumer(&kafka.ConsumerConfig{
		GroupName:    consumerGroup,
		KafkaBrokers: brokers,
		Topics:       esQueryTopic,
	})
	if err != nil {
		err = errors.Wrap(err, "Error creating Kafka Event-Consumer")
		logger.F(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   model.InternalError,
		})
	}

	// Handle Consumer Errors
	go func() {
		for err := range esQueryConsumer.Errors() {
			err = errors.Wrap(err, "Kafka Consumer Error")
			logger.I(tlog.Entry{
				Description: "Error in event consumer. " +
					"The queries cannot be consumed without a working Kafka Consumer. " +
					"The service will now exit.",
			})
			logger.F(tlog.Entry{
				Description: err.Error(),
				ErrorCode:   model.InternalError,
			})
		}
	}()

	// Response Producer
	responseChan := make(chan *model.Document)
	responseProducer, err := kafka.NewProducer(prodConfig)
	if err != nil {
		err = errors.Wrap(err, "Error creating Kafka Response-Producer")
		logger.F(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   model.InternalError,
		})
	}

	// Handle Producer Errors
	go func() {
		for prodErr := range responseProducer.Errors() {
			err = errors.Wrap(prodErr.Err, "Kafka Producer Error")
			logger.E(tlog.Entry{
				Description: err.Error(),
				ErrorCode:   model.InternalError,
			})
		}
	}()

	go func() {
		for doc := range responseChan {
			resp, err := json.Marshal(doc)
			if err != nil {
				err = errors.Wrap(err, "ServiceResponse: Error Marshalling Document")
				logger.E(tlog.Entry{
					Description: err.Error(),
					ErrorCode:   model.InternalError,
				}, doc)
				continue
			}
			respMsg := kafka.CreateMessage(doc.Topic, resp)
			logger.D(tlog.Entry{
				Description: fmt.Sprintf(
					"Produced ESQuery-Response with ID: %s on Topic: %s", doc.UUID, doc.Topic,
				),
			}, doc)
			responseProducer.Input() <- respMsg
		}
	}()

	// ======> Create/Load Cassandra Keyspace/Tables
	logger.I(tlog.Entry{
		Description: "Bootstrapping Event table",
	})
	eventTable, err := bootstrap.Event()
	if err != nil {
		err = errors.Wrap(err, "EventTable: Error getting Table")
		logger.F(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   model.InternalError,
		})
	}
	logger.I(tlog.Entry{
		Description: "Bootstrapping EventMeta table",
	})
	eventMetaTable, err := bootstrap.EventMeta()
	if err != nil {
		err = errors.Wrap(err, "EventMetaTable: Error getting Table")
		logger.F(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   model.InternalError,
		})
	}

	// ======> Setup EventStore
	pKeyVar := "CASSANDRA_EVENT_META_PARTITION_KEY"
	metaPartitionKey, err := strconv.Atoi(os.Getenv(pKeyVar))
	if err != nil {
		err = errors.Wrap(err, pKeyVar+" must be a valid integer")
		logger.F(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   model.InternalError,
		})
	}

	eventStore, err := ioutil.NewEventStore(
		eventTable,
		eventMetaTable,
		logger,
		int8(metaPartitionKey),
	)
	if err != nil {
		err = errors.Wrap(err, "Error while initializing EventStore")
		logger.F(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   model.InternalError,
		})
	}

	// ======> Setup ESQueryHandler
	defaultSize := 6
	batchSizeEnv := os.Getenv("KAFKA_EVENT_BATCH_SIZE")
	batchSize, err := strconv.Atoi(batchSizeEnv)
	if err != nil {
		err = errors.Wrap(err, "Error Getting Event-Producer BatchSize")
		logger.E(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   model.InternalError,
		})
		logger.D(tlog.Entry{
			Description: fmt.Sprintf("Batch-Size used: %d", defaultSize),
		})
		batchSize = defaultSize
	}

	queryUtil, err := ioutil.NewQueryUtil(eventStore, batchSize, logger)
	if err != nil {
		err = errors.Wrap(err, "Error creating QueryUtil")
		logger.F(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   model.InternalError,
		})
	}

	handler, err := NewESQueryHandler(ESQueryHandlerConfig{
		EventStore:   eventStore,
		Logger:       logger,
		QueryUtil:    queryUtil,
		ResponseChan: (chan<- *model.Document)(responseChan),
		ServiceName:  serviceName,
	})
	if err != nil {
		err = errors.Wrap(err, "Error while initializing ESQueryHandler")
		logger.F(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   model.InternalError,
		})
	}

	logger.I(tlog.Entry{
		Description: "EventStoreQuery Service Initialized",
	})

	err = esQueryConsumer.Consume(context.Background(), handler)
	if err != nil {
		err = errors.Wrap(err, "Error while attempting to consume Events")
		logger.F(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   model.InternalError,
		})
	}
}
