package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/TerrexTech/go-eventstore-models/model"

	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventstore-models/bootstrap"
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
			ErrorCode:   1,
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
				ErrorCode:   1,
			})
		}
	}()

	// Response Producer
	responseChan := make(chan *model.KafkaResponse)
	responseProducer, err := kafka.NewProducer(prodConfig)
	if err != nil {
		err = errors.Wrap(err, "Error creating Kafka Response-Producer")
		logger.F(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		})
	}

	// Handle Producer Errors
	go func() {
		for prodErr := range responseProducer.Errors() {
			err = errors.Wrap(prodErr.Err, "Kafka Producer Error")
			logger.E(tlog.Entry{
				Description: err.Error(),
				ErrorCode:   1,
			})
		}
	}()

	go func() {
		for kr := range responseChan {
			resp, err := json.Marshal(kr)
			if err != nil {
				err = errors.Wrap(err, "ServiceResponse: Error Marshalling KafkaResponse")
				logger.E(tlog.Entry{
					Description: err.Error(),
					ErrorCode:   1,
				})
				logger.D(tlog.Entry{
					Description: "KafkaResponse received was:",
					ErrorCode:   1,
				}, kr)
			} else {
				respMsg := kafka.CreateMessage(kr.Topic, resp)
				logger.D(tlog.Entry{
					Description: fmt.Sprintf("Produced ESQuery-Response with ID: %s", kr.UUID),
				}, kr)
				responseProducer.Input() <- respMsg
			}
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
			ErrorCode:   1,
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
			ErrorCode:   1,
		})
	}

	// ======> Setup EventStore
	pKeyVar := "CASSANDRA_EVENT_META_PARTITION_KEY"
	metaPartitionKey, err := strconv.Atoi(os.Getenv(pKeyVar))
	if err != nil {
		err = errors.Wrap(err, pKeyVar+" must be a valid integer")
		logger.F(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		})
	}

	eventStore, err := ioutil.NewEventStore(
		eventTable,
		eventMetaTable,
		int8(metaPartitionKey),
	)
	if err != nil {
		err = errors.Wrap(err, "Error while initializing EventStore")
		logger.F(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		})
	}

	// ======> Setup EventHandler
	defaultSize := 6
	batchSizeEnv := os.Getenv("KAFKA_EVENT_BATCH_SIZE")
	batchSize, err := strconv.Atoi(batchSizeEnv)
	if err != nil {
		err = errors.Wrap(err, "Error Getting Event-Producer BatchSize")
		logger.E(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		})
		logger.D(tlog.Entry{
			Description: fmt.Sprintf("Batch-Size used: %d", defaultSize),
		})
		batchSize = defaultSize
	}

	queryUtil := &ioutil.QueryUtil{
		EventStore: eventStore,
		BatchSize:  batchSize,
	}
	responseTopic := os.Getenv("KAFKA_RESPONSE_TOPIC")

	handler, err := NewEventHandler(EventHandlerConfig{
		EventStore:    eventStore,
		Logger:        logger,
		QueryUtil:     queryUtil,
		ResponseChan:  (chan<- *model.KafkaResponse)(responseChan),
		ResponseTopic: responseTopic,
	})
	if err != nil {
		err = errors.Wrap(err, "Error while initializing EventHandler")
		logger.F(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
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
			ErrorCode:   1,
		})
	}
}
