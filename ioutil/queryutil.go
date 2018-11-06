package ioutil

import (
	"encoding/json"
	"sort"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"
)

// QueryUtil handles the EventStore-Query.
// This fetches the new Aggregate-Events, updates the Aggregate version in Events-Meta
// table, and sends those events in batches to the Kafka response-topic.
type QueryUtil struct {
	// We don't keep these as method parameters due to convenience.
	// Since these will not change once set, we only set them once.

	// EventStore provides convenient functions to get Aggregate data.
	EventStore EventStore
	BatchSize  int
}

// QueryHandler gets the Aggregate-Events.
func (qu *QueryUtil) QueryHandler(
	query *model.EventStoreQuery,
) ([]model.Event, error) {
	// Get Aggregate Meta-Version
	aggMetaVersion, err := qu.EventStore.GetAggMetaVersion(query)
	if err != nil {
		err = errors.Wrap(err, "Error Getting Aggregate Meta-Version")
		return nil, err
	}

	events, err := qu.EventStore.GetAggEvents(query, aggMetaVersion)
	if err != nil {
		err = errors.Wrap(err, "Error While Fetching AggregateEvents")
		return nil, err
	}
	return events, nil
}

// CreateBatch produces the provided events in chunks of sizes set as per the env-var
// "KAFKA_EVENT_BATCH_SIZE". If this value is missing, the default batch value if 6.
func (qu *QueryUtil) CreateBatch(
	correlationID uuuid.UUID,
	aggID int8,
	events []model.Event,
) []model.KafkaResponse {
	batches := make([]model.KafkaResponse, 0)

	// Sort events in scending order of their Nano-Timestamps
	sort.Slice(events, func(i, j int) bool {
		ti := events[i].NanoTime
		tj := events[j].NanoTime
		return ti < tj
	})

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

		batches = append(batches, model.KafkaResponse{
			AggregateID:   aggID,
			CorrelationID: correlationID,
			ErrorCode:     errCode,
			Error:         errStr,
			Result:        batchJSON,
		})
	}
	return batches
}
