package ioutil

import (
	"encoding/json"
	"log"
	"sort"

	"github.com/TerrexTech/uuuid"

	"github.com/TerrexTech/go-eventstore-models/model"
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

// BatchProduce produces the provided events in chunks of sizes set as per the env-var
// "KAFKA_EVENT_BATCH_SIZE". If this value is missing, the default batch value if 6.
func (qu *QueryUtil) BatchProduce(
	CorrelationID uuuid.UUID,
	aggID int8,
	events []model.Event,
) []model.KafkaResponse {
	batches := make([]model.KafkaResponse, 0)

	// Sort events in scending order of their UUID-Timestamps
	sort.Slice(events, func(i, j int) bool {
		ti, err := uuuid.TimestampFromV1(events[i].TimeUUID)
		if err != nil {
			err = errors.Wrap(err, "Error sorting Events: Error getting T1")
			log.Println(err)
		}
		tj, err := uuuid.TimestampFromV1(events[j].TimeUUID)
		if err != nil {
			err = errors.Wrap(err, "Error sorting Events: Error getting T2")
			log.Println(err)
		}
		return ti < tj
	})

	for i := 0; i < len(events); i += qu.BatchSize {
		batchEndIndex := i + qu.BatchSize
		if batchEndIndex > len(events) {
			batchEndIndex = len(events)
		}
		eventsBatch := events[i:batchEndIndex]

		errStr := ""
		batchJSON, err := json.Marshal(eventsBatch)
		if err != nil {
			err = errors.Wrapf(err, "Error Marshalling EventsBatch at index: %d", i)
			errStr = err.Error()
			log.Println(err)
			return nil
		}

		batches = append(batches, model.KafkaResponse{
			AggregateID:   aggID,
			CorrelationID: CorrelationID,
			Error:         errStr,
			Result:        batchJSON,
		})
	}
	return batches
}
