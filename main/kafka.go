package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/consumer"
	"github.com/TerrexTech/go-kafkautils/producer"
	"github.com/pkg/errors"
)

// KafkaConfig represents the Kafka's connection, publisher, and consumer configs.
type KafkaConfig struct {
	Brokers           []string
	ConsumerGroupName string
	ConsumerTopics    []string
	ResponseTopic     string
}

// KafkaIO provides channels for interacting with Kafka.
// Note: All receive-channels must be read from to prevent deadlock.
type KafkaIO struct {
	consumerErrChan    <-chan error
	consumerMsgChan    <-chan *sarama.ConsumerMessage
	consumerOffsetChan chan<- *sarama.ConsumerMessage
	producerErrChan    <-chan *sarama.ProducerError
	producerInputChan  chan<- *model.KafkaResponse
}

// ConsumerErrors returns send-channel where consumer errors are published.
func (kio *KafkaIO) ConsumerErrors() <-chan error {
	return kio.consumerErrChan
}

// ConsumerMessages returns send-channel where consumer messages are published.
func (kio *KafkaIO) ConsumerMessages() <-chan *sarama.ConsumerMessage {
	return kio.consumerMsgChan
}

// MarkOffset marks the consumer message-offset to be committed.
// This should be used once a message has done its job.
func (kio *KafkaIO) MarkOffset() chan<- *sarama.ConsumerMessage {
	return kio.consumerOffsetChan
}

// ProducerErrors returns send-channel where producer errors are published.
func (kio *KafkaIO) ProducerErrors() <-chan *sarama.ProducerError {
	return kio.producerErrChan
}

// ProducerInput returns receive-channel where kafka-responses can be produced.
func (kio *KafkaIO) ProducerInput() chan<- *model.KafkaResponse {
	return (chan<- *model.KafkaResponse)(kio.producerInputChan)
}

// InitKafkaIO initializes KafkaIO from the configuration provided.
// It is necessary that both consumer and producer are properly setup, to enable
// responses for requests. Else this operation will be marked as failed, and the
// service won't run.
func InitKafkaIO(config KafkaConfig) (*KafkaIO, error) {
	log.Println("Initializing KafkaIO")

	log.Println("Creating Kafka Response-Producer")
	// Create Kafka Response-Producer
	prodConfig := producer.Config{
		KafkaBrokers: config.Brokers,
	}
	resProducer, err := producer.New(&prodConfig)
	if err != nil {
		err = errors.Wrap(err, "Error Creating EventsResponse Producer")
		return nil, err
	}

	resProducerInput, err := resProducer.Input()
	if err != nil {
		err = errors.Wrap(err, "Error Getting Input-Channel from Producer")
		return nil, err
	}

	producerInputChan := make(chan *model.KafkaResponse)
	// Setup Producer I/O channels
	kio := &KafkaIO{
		producerInputChan: (chan<- *model.KafkaResponse)(producerInputChan),
		producerErrChan:   resProducer.Errors(),
	}

	// The Kafka-Response post-processing the consumed events
	go func() {
		for msg := range producerInputChan {
			msgJSON, err := json.Marshal(msg)
			if err != nil {
				// Something went severely wrong
				err = errors.Wrap(err, "Error Marshalling KafkaResponse")
				log.Fatalln(err)
			}

			resTopic := fmt.Sprintf("%s.%d", config.ResponseTopic, msg.AggregateID)
			producerMsg := producer.CreateMessage(resTopic, msgJSON)
			resProducerInput <- producerMsg
		}
	}()

	log.Println("Created Kafka Response-Channel")

	log.Println("Creating Kafka EventQuery-Consumer")

	// Create Kafka Event-Consumer
	consConfig := &consumer.Config{
		ConsumerGroup: config.ConsumerGroupName,
		KafkaBrokers:  config.Brokers,
		Topics:        config.ConsumerTopics,
	}

	eventConsumer, err := consumer.New(consConfig)
	if err != nil {
		err = errors.Wrap(err, "Error Creating ConsumerGroup for Events")
		return nil, err
	}
	log.Println("Created Kafka Event-Consumer Group")

	// A channel which receives consumer-messages to be committed
	consumerOffsetChan := make(chan *sarama.ConsumerMessage)
	kio.consumerOffsetChan = (chan<- *sarama.ConsumerMessage)(consumerOffsetChan)
	go func() {
		for msg := range consumerOffsetChan {
			eventConsumer.MarkOffset(msg, "")
		}
	}()
	log.Println("Created Kafka Event Offset-Commit Channel")

	// Setup Consumer I/O channels
	kio.consumerErrChan = eventConsumer.Errors()
	kio.consumerMsgChan = eventConsumer.Messages()
	log.Println("KafkaIO Ready")

	return kio, nil
}
