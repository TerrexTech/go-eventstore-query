package main

import (
	"encoding/json"
	"fmt"

	tlog "github.com/TerrexTech/go-logtransport/log"

	"github.com/TerrexTech/uuuid"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-eventstore-query/ioutil"
	"github.com/pkg/errors"
)

// EventHandlerConfig is the configuration for EventsConsumer.
type EventHandlerConfig struct {
	EventStore    ioutil.EventStore
	Logger        tlog.Logger
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
	if config.Logger == nil {
		return nil, errors.New("invalid config: Logger cannot be nil")
	}
	if config.ResponseChan == nil {
		return nil, errors.New("invalid config: ResponseChan cannot be nil")
	}
	if config.ResponseTopic == "" {
		return nil, errors.New("invalid config: ResponseTopic cannot be blank")
	}

	return &eventHandler{config}, nil
}

func (e *eventHandler) Setup(sarama.ConsumerGroupSession) error {
	logDesc := "Initializing Kafka EventHandler"
	e.config.Logger.I(tlog.Entry{
		Description: logDesc,
	})
	return nil
}

func (e *eventHandler) Cleanup(sarama.ConsumerGroupSession) error {
	logDesc := "Closing Kafka EventHandler"
	e.config.Logger.I(tlog.Entry{
		Description: logDesc,
	})
	return nil
}

func (e *eventHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	logger := e.config.Logger

	logger.I(tlog.Entry{
		Description: "Listening for new Events...",
	})

	for msg := range claim.Messages() {
		go func(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) {
			esQuery := &model.EventStoreQuery{}
			err := json.Unmarshal(msg.Value, esQuery)
			if err != nil {
				err = errors.Wrap(err, "Error: unable to Unmarshal Event")
				logger.E(tlog.Entry{
					Description: err.Error(),
					ErrorCode:   1,
				})

				session.MarkMessage(msg, "")
				return
			}

			logger.I(tlog.Entry{
				Description: fmt.Sprintf("Received Query with ID: %s", esQuery.UUID),
			})

			// =====> Validate Query
			if esQuery.AggregateID == 0 {
				err = errors.New("received a Query with missing AggregateID")
				logger.E(tlog.Entry{
					Description: err.Error(),
					ErrorCode:   1,
				})

				session.MarkMessage(msg, "")
				return
			}
			if esQuery.UUID == (uuuid.UUID{}) {
				session.MarkMessage(msg, "")
				err = errors.New("received a Query with missing UUID")
				logger.E(tlog.Entry{
					Description: err.Error(),
					ErrorCode:   1,
				})

				kr := &model.KafkaResponse{
					AggregateID:   esQuery.AggregateID,
					CorrelationID: esQuery.CorrelationID,
					Input:         msg.Value,
					Error:         err.Error(),
					UUID:          esQuery.UUID,
				}
				kr.Topic = fmt.Sprintf("%s.%d", e.config.ResponseTopic, esQuery.AggregateID)
				e.config.ResponseChan <- kr
				logger.D(tlog.Entry{
					Description: "Produced response on topic",
				}, kr.Topic, kr)
				return
			}

			events, err := e.config.QueryUtil.QueryHandler(esQuery)
			if err != nil {
				err = errors.Wrap(err, "Error Processing Query")
				logger.E(tlog.Entry{
					Description: err.Error(),
					ErrorCode:   1,
				})
				kr := &model.KafkaResponse{
					AggregateID:   esQuery.AggregateID,
					CorrelationID: esQuery.CorrelationID,
					Error:         err.Error(),
				}
				kr.Topic = fmt.Sprintf("%s.%d", e.config.ResponseTopic, esQuery.AggregateID)
				e.config.ResponseChan <- kr
				logger.D(tlog.Entry{
					Description: "Produced response on topic",
				}, kr.Topic, kr)
				return
			}
			batch := e.config.QueryUtil.CreateBatch(
				esQuery.CorrelationID,
				esQuery.AggregateID,
				events,
			)
			for _, kr := range batch {
				kr.UUID = esQuery.UUID
				kr.CorrelationID = esQuery.CorrelationID

				kr.Topic = fmt.Sprintf("%s.%d", e.config.ResponseTopic, esQuery.AggregateID)
				e.config.ResponseChan <- &kr

				if kr.Error != "" {
					logger.E(tlog.Entry{
						Description:   kr.Error,
						ErrorCode:     int(kr.ErrorCode),
						EventAction:   kr.EventAction,
						ServiceAction: kr.ServiceAction,
					})
				}

				logger.D(tlog.Entry{
					Description: fmt.Sprintf(
						`Produced batch-response with UUID: "%s" on Topic: "%s"`,
						kr.UUID,
						kr.Topic,
					),
				}, kr)
			}
		}(session, msg)
	}
	return nil
}
