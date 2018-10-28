package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/TerrexTech/uuuid"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-eventstore-query/ioutil"
	"github.com/pkg/errors"
)

// EventHandlerConfig is the configuration for EventsConsumer.
type EventHandlerConfig struct {
	EventStore    ioutil.EventStore
	QueryUtil     *ioutil.QueryUtil
	ResponseChan  chan<- *model.KafkaResponse
	ResponseTopic string
}

// eventHandler handler for Consumer Messages
type eventHandler struct {
	config EventHandlerConfig
}

// NewEventHandler creates a new handler for ConsumerEvents.
func NewEventHandler(config EventHandlerConfig) (sarama.ConsumerGroupHandler, error) {
	if config.EventStore == nil {
		return nil, errors.New("invalid config: EventStore cannot be nil")
	}
	if config.ResponseChan == nil {
		return nil, errors.New("invalid config: ResponseChan cannot be nil")
	}
	if config.ResponseTopic == "" {
		return nil, errors.New("invalid config: ResponseTopic cannot be blank")
	}

	return &eventHandler{config}, nil
}

func (*eventHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Initializing Kafka EventHandler")
	return nil
}

func (*eventHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Closing Kafka EventHandler")
	return nil
}

func (e *eventHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	log.Println("Listening for new Events...")
	for msg := range claim.Messages() {
		go func(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) {
			esQuery := &model.EventStoreQuery{}
			err := json.Unmarshal(msg.Value, esQuery)
			if err != nil {
				err = errors.Wrap(err, "Error: unable to Unmarshal Event")
				log.Println(err)

				session.MarkMessage(msg, "")
				return
			}
			log.Printf("Received Query with ID: %s", esQuery.UUID)

			// =====> Validate Query
			if esQuery.AggregateID == 0 {
				err = errors.New("received a Query with missing AggregateID")
				log.Println(err)

				session.MarkMessage(msg, "")
				return
			}
			if esQuery.UUID.String() == (uuuid.UUID{}).String() {
				session.MarkMessage(msg, "")
				err = errors.New("received a Query with missing UUID")
				log.Println(err)

				kr := &model.KafkaResponse{
					AggregateID:   esQuery.AggregateID,
					CorrelationID: esQuery.CorrelationID,
					Input:         msg.Value,
					Error:         err.Error(),
					UUID:          esQuery.UUID,
				}
				kr.Topic = fmt.Sprintf("%s.%d", e.config.ResponseTopic, esQuery.AggregateID)
				e.config.ResponseChan <- kr
				return
			}

			events, err := e.config.QueryUtil.QueryHandler(esQuery)
			if err != nil {
				err = errors.Wrap(err, "Error Processing Query")
				log.Println(err)
				kr := &model.KafkaResponse{
					AggregateID:   esQuery.AggregateID,
					CorrelationID: esQuery.CorrelationID,
					Error:         err.Error(),
				}
				kr.Topic = fmt.Sprintf("%s.%d", e.config.ResponseTopic, esQuery.AggregateID)
				e.config.ResponseChan <- kr
				return
			}
			batch := e.config.QueryUtil.BatchProduce(
				esQuery.CorrelationID, esQuery.AggregateID,
				events,
			)
			for _, kr := range batch {
				kr.UUID = esQuery.UUID
				kr.CorrelationID = esQuery.CorrelationID

				kr.Topic = fmt.Sprintf("%s.%d", e.config.ResponseTopic, esQuery.AggregateID)
				e.config.ResponseChan <- &kr
			}
		}(session, msg)
	}
	return nil
}
