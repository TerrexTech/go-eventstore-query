package ioutil

import (
	"fmt"

	csndra "github.com/TerrexTech/go-cassandrautils/cassandra"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/pkg/errors"
)

// EventStore provides convenience functions for getting Aggregate data.
type EventStore interface {
	GetAggMetaVersion(eventStoreQuery *model.EventStoreQuery) (int64, error)
	GetAggEvents(
		eventStoreQuery *model.EventStoreQuery,
		eventMetaVersion int64,
	) ([]model.Event, error)
}

// eventStore is the implementation for EventStore.
type eventStore struct {
	eventMetaTable   *csndra.Table
	eventTable       *csndra.Table
	metaPartitionKey int8
}

// NewEventStore creates an EventStore.
func NewEventStore(
	eventTable *csndra.Table,
	eventMetaTable *csndra.Table,
	metaPartitionKey int8,
) (EventStore, error) {
	if eventTable == nil {
		return nil, errors.New("eventTable cannot be nil")
	}
	if eventMetaTable == nil {
		return nil, errors.New("eventMetaTable cannot be nil")
	}

	return &eventStore{
		eventTable:       eventTable,
		eventMetaTable:   eventMetaTable,
		metaPartitionKey: metaPartitionKey,
	}, nil
}

// GetAggMetaVersion gets the AggregateVersion from Events-Meta table.
func (es *eventStore) GetAggMetaVersion(
	eventStoreQuery *model.EventStoreQuery,
) (int64, error) {
	aggID := eventStoreQuery.AggregateID
	if aggID == 0 {
		return -1, errors.New("AggregateID not specified")
	}

	resultsBind := []model.EventMeta{}
	partnCol, err := es.eventMetaTable.Column("partitionKey")
	if err != nil {
		return -1, fmt.Errorf(
			"Error getting PartitionKey column from MetaTable for AggregateID %d",
			aggID,
		)
	}
	aggIDCol, err := es.eventMetaTable.Column("aggregateID")
	if err != nil {
		return -1, fmt.Errorf(
			"Error getting AggregateID column from MetaTable for AggregateID %d",
			aggID,
		)
	}

	_, err = es.eventMetaTable.Select(csndra.SelectParams{
		ResultsBind: &resultsBind,
		ColumnValues: []csndra.ColumnComparator{
			csndra.Comparator(partnCol, es.metaPartitionKey).Eq(),
			csndra.Comparator(aggIDCol, aggID).Eq(),
		},
		SelectColumns: es.eventMetaTable.Columns(),
	})

	if err != nil {
		err = errors.Wrap(err, "Error Fetching AggregateVersion from EventMeta")
		return -1, err
	}

	if len(resultsBind) == 0 {
		err = fmt.Errorf("no Aggregates found with ID: %d", aggID)
		err = errors.Wrap(err, "Error Fetching AggregateVersion from EventMeta")
		return -1, err
	}

	metaVersion := resultsBind[0].AggregateVersion

	// Increment aggregate-version in event-meta table
	result := resultsBind[0]
	result.AggregateVersion++
	err = <-es.eventMetaTable.AsyncInsert(&result)
	if err != nil {
		return -1, err
	}

	return metaVersion, nil
}

// GetAggEvents gets the AggregateEvents from Events table.
func (es *eventStore) GetAggEvents(
	eventStoreQuery *model.EventStoreQuery,
	eventMetaVersion int64,
) ([]model.Event, error) {
	aggID := eventStoreQuery.AggregateID
	if aggID == 0 {
		return nil, errors.New("AggregateID not specified")
	}
	aggVersion := eventStoreQuery.AggregateVersion
	if aggVersion == 0 {
		return nil, fmt.Errorf("AggregateVersion not specified for AggregateID: %d", aggID)
	}

	events := []model.Event{}
	yearBucketCol, _ := es.eventTable.Column("yearBucket")
	aggIDCol, _ := es.eventTable.Column("aggregateID")
	versionCol, _ := es.eventTable.Column("version")

	sp := csndra.SelectParams{
		ResultsBind: &events,
		ColumnValues: []csndra.ColumnComparator{
			csndra.Comparator(yearBucketCol, eventStoreQuery.YearBucket).Eq(),
			csndra.Comparator(aggIDCol, aggID).Eq(),
			csndra.Comparator(versionCol, aggVersion).Gt(),
			csndra.Comparator(versionCol, eventMetaVersion).LtOrEq(),
		},
		SelectColumns: es.eventTable.Columns(),
	}

	_, err := es.eventTable.Select(sp)
	err = errors.Wrap(err, "Error in GetAggEvents")
	return events, err
}
