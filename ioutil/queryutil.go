package ioutil

import (
	"encoding/json"
	"sort"

	"github.com/TerrexTech/go-common-models/model"
	tlog "github.com/TerrexTech/go-logtransport/log"
	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"
)

// QueryUtil handles the EventStore-Query.
// This fetches the new Aggregate-Events, updates the Aggregate version in Events-Meta
// table, and sends those events in batches to the Kafka response-topic.
type QueryUtil struct {
	// EventStore provides convenient functions to get Aggregate data.
	EventStore EventStore
	BatchSize  int
	Logger     tlog.Logger
}

// NewQueryUtil creates an QueryUtil.
func NewQueryUtil(es EventStore, batchSize int, logger tlog.Logger) (*QueryUtil, error) {
	if es == nil {
		return nil, errors.New("config error: EventStore cannot be nil")
	}
	if batchSize == 0 {
		return nil, errors.New("config error: BatchSize must be >0")
	}
	if logger == nil {
		return nil, errors.New("config error: Logger cannot be nil")
	}

	return &QueryUtil{
		EventStore: es,
		BatchSize:  batchSize,
		Logger:     logger,
	}, nil
}

// QueryHandler gets the Aggregate-Events.
func (qu *QueryUtil) QueryHandler(
	query *model.EventStoreQuery,
) ([]model.Event, error) {
	// Get Aggregate Meta-Version
	aggMetaVersion, err := qu.EventStore.GetAggMetaVersion(query.AggregateID)
	if err != nil {
		err = errors.Wrap(err, "Error Getting Aggregate Meta-Version")
		return nil, err
	}

	events, err := qu.EventStore.GetAggEvents(
		query.AggregateID,
		query.AggregateVersion,
		query.YearBucket,
		aggMetaVersion,
	)
	if err != nil {
		err = errors.Wrap(err, "Error While Fetching AggregateEvents")
		return nil, err
	}
	return events, nil
}

// CreateBatch produces the provided events in chunks of sizes set as per the env-var
// "KAFKA_EVENT_BATCH_SIZE". If this value is missing, the default batch value is 6.
func (qu *QueryUtil) CreateBatch(
	aggID int8,
	correlationID uuuid.UUID,
	events []model.Event,
) []model.Document {
	batches := make([]model.Document, 0)

	qu.Logger.D(tlog.Entry{
		Description: "Sorting events. Events before sort:",
	}, events)
	// Sort events in ascending order of their Versions and Nano-Times
	sort.Slice(events, func(i, j int) bool {
		ti := events[i].Version
		tj := events[j].Version
		if ti == tj {
			ti = events[i].NanoTime
			tj = events[j].NanoTime
		}
		return ti < tj
	})
	qu.Logger.D(tlog.Entry{
		Description: "Sorting events. Events after sort:",
	}, events)

	for i := 0; i < len(events); i += qu.BatchSize {
		batchEndIndex := i + qu.BatchSize
		if batchEndIndex > len(events) {
			batchEndIndex = len(events)
		}
		eventsBatch := events[i:batchEndIndex]

		errStr := ""
		var errCode int16
		batchJSON, err := json.Marshal(eventsBatch)
		if err != nil {
			err = errors.Wrapf(err, "Error Marshalling EventsBatch at index: %d", i)
			errStr = err.Error()
			errCode = 1
		}

		cid, err := uuuid.NewV4()
		if err != nil {
			err = errors.Wrap(err, "Error generating document-uuid")
			errStr = err.Error()
			errCode = 1
			qu.Logger.E(tlog.Entry{
				ErrorCode:   int(errCode),
				Description: errStr,
			})
		}
		batches = append(batches, model.Document{
			CorrelationID: cid,
			Data:          batchJSON,
			Error:         errStr,
			ErrorCode:     errCode,
			UUID:          correlationID,
		})
	}
	return batches
}
