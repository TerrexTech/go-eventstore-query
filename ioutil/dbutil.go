package ioutil

import (
	"fmt"

	"github.com/TerrexTech/go-cassandrautils/cassandra"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/pkg/errors"
)

// DBUtilI provides convenience functions for getting Aggregate data
type DBUtilI interface {
	GetAggMetaVersion(eventStoreQuery *model.EventStoreQuery) (int64, error)
	GetAggEvents(
		eventStoreQuery *model.EventStoreQuery,
		eventMetaVersion int64,
	) (*[]model.Event, error)
}

// DBUtil implements DBUtilI.
// DBUtilI provides convenience functions for getting Aggregate data.
type DBUtil struct {
	EventMetaTable *cassandra.Table
	EventTable     *cassandra.Table
}

// GetAggMetaVersion gets the AggregateVersion from Events-Meta table.
func (dbu *DBUtil) GetAggMetaVersion(
	eventStoreQuery *model.EventStoreQuery,
) (int64, error) {
	aggID := eventStoreQuery.AggregateID
	if aggID == 0 {
		return 0, errors.New("AggregateID not specified")
	}
	yearBucket := eventStoreQuery.YearBucket
	if yearBucket == 0 {
		return 0, fmt.Errorf("YearBucket not specified for AggregateID: %d", aggID)
	}

	resultsBind := []model.EventMeta{}
	yearBucketCol, _ := dbu.EventTable.Column("yearBucket")
	aggIDCol, _ := dbu.EventMetaTable.Column("aggregateID")

	_, err := dbu.EventMetaTable.Select(cassandra.SelectParams{
		ResultsBind: &resultsBind,
		ColumnValues: []cassandra.ColumnComparator{
			cassandra.Comparator(yearBucketCol, yearBucket).Eq(),
			cassandra.Comparator(aggIDCol, aggID).Eq(),
		},
		SelectColumns: dbu.EventMetaTable.Columns(),
	})

	if err != nil {
		err = errors.Wrap(err, "Error Fetching AggregateVersion from EventMeta")
		return 0, err
	}

	if len(resultsBind) == 0 {
		err = fmt.Errorf("No Aggregates found with ID: %d", aggID)
		err = errors.Wrap(err, "Error Fetching AggregateVersion from EventMeta")
		return 0, err
	}

	metaVersion := resultsBind[0].AggregateVersion

	// Increment aggregate-version in event-meta table
	result := resultsBind[0]
	result.AggregateVersion++
	err = <-dbu.EventMetaTable.AsyncInsert(&result)
	if err != nil {
		return 0, err
	}

	return metaVersion, nil
}

// GetAggEvents gets the AggregateEvents from Events table.
func (dbu *DBUtil) GetAggEvents(
	eventStoreQuery *model.EventStoreQuery,
	eventMetaVersion int64,
) (*[]model.Event, error) {
	aggID := eventStoreQuery.AggregateID
	if aggID == 0 {
		return nil, errors.New("AggregateID not specified")
	}
	aggVersion := eventStoreQuery.AggregateVersion
	if aggVersion == 0 {
		return nil, fmt.Errorf("AggregateVersion not specified for AggregateID: %d", aggID)
	}
	yearBucket := eventStoreQuery.YearBucket
	if yearBucket == 0 {
		return nil, fmt.Errorf("YearBucket not specified for AggregateID: %d", aggID)
	}

	events := &[]model.Event{}
	yearBucketCol, _ := dbu.EventTable.Column("yearBucket")
	aggIDCol, _ := dbu.EventTable.Column("aggregateID")
	versionCol, _ := dbu.EventTable.Column("version")

	sp := cassandra.SelectParams{
		ResultsBind: events,
		ColumnValues: []cassandra.ColumnComparator{
			cassandra.Comparator(yearBucketCol, yearBucket).Eq(),
			cassandra.Comparator(aggIDCol, aggID).Eq(),
			cassandra.Comparator(versionCol, aggVersion).Gt(),
			cassandra.Comparator(versionCol, eventMetaVersion).Lt(),
		},
		SelectColumns: dbu.EventTable.Columns(),
	}

	_, err := dbu.EventTable.Select(sp)
	return events, err
}
