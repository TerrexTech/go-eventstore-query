package main

import (
	"encoding/json"
	"fmt"

	tlog "github.com/TerrexTech/go-logtransport/log"

	"github.com/TerrexTech/uuuid"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-eventstore-query/ioutil"
	"github.com/pkg/errors"
)

// EventHandlerConfig is the configuration for EventsConsumer.
type EventHandlerConfig struct {
	EventStore    ioutil.EventStore
	Logger        tlog.Logger
	QueryUtil     *ioutil.QueryUtil
	ResponseChan  chan<- *model.Document
	ResponseTopic string
	ServiceName   string
}

// eventHandler handler for Consumer Messages
type eventHandler struct {
	EventHandlerConfig
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
	if config.ServiceName == "" {
		return nil, errors.New("invalid config: ServiceName cannot be blank")
	}

	return &eventHandler{config}, nil
}

func (e *eventHandler) Setup(sarama.ConsumerGroupSession) error {
	logDesc := "Initializing Kafka EventHandler"
	e.Logger.I(tlog.Entry{
		Description: logDesc,
	})
	return nil
}

func (e *eventHandler) Cleanup(sarama.ConsumerGroupSession) error {
	logDesc := "Closing Kafka EventHandler"
	e.Logger.I(tlog.Entry{
		Description: logDesc,
	})
	return nil
}

func (e *eventHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	logger := e.Logger

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

			docCID, err := uuuid.NewV4()
			if err != nil {
				err = errors.Wrap(err, "Error generating UUID for Document-response")
				logger.E(tlog.Entry{
					Description: err.Error(),
					ErrorCode:   1,
				})
			}

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
				doc := &model.Document{
					CorrelationID: docCID,
					Data:          msg.Value,
					Error:         err.Error(),
					ErrorCode:     1,
					Source:        e.ServiceName,
					Topic:         fmt.Sprintf("%s.%d", e.ResponseTopic, esQuery.AggregateID),
					UUID:          esQuery.CorrelationID,
				}
				e.ResponseChan <- doc
				logger.D(tlog.Entry{
					Description: "Produced response on topic",
				}, doc.Topic, doc)
				return
			}

			// Get events
			events, err := e.QueryUtil.QueryHandler(esQuery)
			if err != nil {
				err = errors.Wrap(err, "Error Processing Query")
				logger.E(tlog.Entry{
					Description: err.Error(),
					ErrorCode:   1,
				})

				doc := &model.Document{
					CorrelationID: docCID,
					Data:          msg.Value,
					Error:         err.Error(),
					ErrorCode:     1,
					Source:        e.ServiceName,
					Topic:         fmt.Sprintf("%s.%d", e.ResponseTopic, esQuery.AggregateID),
					UUID:          esQuery.CorrelationID,
				}
				doc.Topic = fmt.Sprintf("%s.%d", e.ResponseTopic, esQuery.AggregateID)
				e.ResponseChan <- doc
				logger.D(tlog.Entry{
					Description: "Produced response on topic",
				}, doc.Topic, doc)
				return
			}
			batch := e.QueryUtil.CreateBatch(
				esQuery.AggregateID,
				esQuery.CorrelationID,
				events,
			)
			for _, doc := range batch {
				uuid, err := uuuid.NewV4()
				if err != nil {
					err = errors.Wrap(err, "Error generating UUID for batch")
					logger.E(tlog.Entry{
						Description: err.Error(),
						ErrorCode:   1,
					}, doc)
				}
				doc.UUID = esQuery.CorrelationID
				doc.CorrelationID = uuid

				doc.Topic = fmt.Sprintf("%s.%d", e.ResponseTopic, esQuery.AggregateID)
				e.ResponseChan <- &doc

				if doc.Error != "" {
					logger.E(tlog.Entry{
						Description: doc.Error,
						ErrorCode:   int(doc.ErrorCode),
					}, doc)
				}

				logger.D(tlog.Entry{
					Description: fmt.Sprintf(
						`Produced batch-response with UUID: "%s" on Topic: "%s"`,
						doc.UUID,
						doc.Topic,
					),
				}, doc)
			}
		}(session, msg)
	}
	return nil
}
