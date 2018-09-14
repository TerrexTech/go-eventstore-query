package ioutil

import (
	"encoding/json"
	"log"
	"os"
	"strconv"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/pkg/errors"
)

// QueryUtil handles the EventStore-Query.
// This fetches the new Aggregate-Events, updates the Aggregate version in Events-Meta
// table, and sends those events in batches to the Kafka response-topic.
type QueryUtil struct {
	// We don't keep these as method parameters due to convenience.
	// Since these will not change once set, we only set them once.

	// DBUtil provides convenient functions to get Aggregate data.
	DBUtil DBUtilI
	// ResponseChan is used to publish the processing response to Kafka response-topic.
	ResponseChan chan<- *model.KafkaResponse
}

// QueryHandler gets the Aggregate-Events.
func (qu *QueryUtil) QueryHandler(query *model.EventStoreQuery) (*[]model.Event, error) {
	// Get Aggregate Meta-Version
	aggMetaVersion, err := qu.DBUtil.GetAggMetaVersion(query)
	if err != nil {
		err = errors.Wrap(err, "Error Getting Aggregate Meta-Version")
		return nil, err
	}

	events, err := qu.DBUtil.GetAggEvents(query, aggMetaVersion)
	if err != nil {
		err = errors.Wrap(err, "Error While Fetching AggregateEvents")
		return nil, err
	}
	return events, nil
}

// BatchProduce produces the provided events in chunks of sizes set as per the env-var
// "KAFKA_EVENT_BATCH_SIZE". If this value is missing, the default batch value if 6.
func (qu *QueryUtil) BatchProduce(aggID int8, events *[]model.Event) {
	defaultSize := 6
	batchSize, err := strconv.Atoi(os.Getenv("KAFKA_EVENT_BATCH_SIZE"))
	if err != nil {
		err = errors.Wrap(err, "Error Getting Event-Producer BatchSize")
		log.Println(err)
		log.Printf("A default batch-size of %d will be used", defaultSize)
		batchSize = defaultSize
	}

	for i := 0; i < len(*events); i += batchSize {
		batchIndex := i + batchSize
		if batchIndex > len(*events) {
			batchIndex = len(*events)
		}

		eventsBatch := (*events)[i:batchIndex]
		batchJSON, err := json.Marshal(eventsBatch)
		errStr := ""
		if err != nil {
			err = errors.Wrapf(err, "Error Marshalling EventsBatch at index: %d", i)
			errStr = err.Error()
			log.Println(err)
		}

		kr := &model.KafkaResponse{
			AggregateID: aggID,
			Error:       errStr,
			Result:      string(batchJSON),
		}
		qu.ResponseChan <- kr
	}
}
