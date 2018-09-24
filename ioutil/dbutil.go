package ioutil

import (
	"fmt"

	csndra "github.com/TerrexTech/go-cassandrautils/cassandra"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/pkg/errors"
)

// DBUtilI provides convenience functions for getting Aggregate data
type DBUtilI interface {
	GetAggMetaVersion(
		eventMetaPartnKey int8,
		eventStoreQuery *model.EventStoreQuery,
	) (int64, error)
	GetAggEvents(
		eventStoreQuery *model.EventStoreQuery,
		eventMetaVersion int64,
	) (*[]model.Event, error)
}

// DBUtil implements DBUtilI.
// DBUtilI provides convenience functions for getting Aggregate data.
type DBUtil struct {
	EventMetaTable *csndra.Table
	EventTable     *csndra.Table
}

// GetAggMetaVersion gets the AggregateVersion from Events-Meta table.
func (dbu *DBUtil) GetAggMetaVersion(
	eventMetaPartnKey int8,
	eventStoreQuery *model.EventStoreQuery,
) (int64, error) {
	aggID := eventStoreQuery.AggregateID
	if aggID == 1 {
		return -1, errors.New("AggregateID not specified")
	}

	resultsBind := []model.EventMeta{}
	partnCol, err := dbu.EventMetaTable.Column("partitionKey")
	if err != nil {
		return -1, fmt.Errorf(
			"Error getting PartitionKey column from MetaTable for AggregateID %d",
			aggID,
		)
	}
	aggIDCol, err := dbu.EventMetaTable.Column("aggregateID")
	if err != nil {
		return -1, fmt.Errorf(
			"Error getting AggregateID column from MetaTable for AggregateID %d",
			aggID,
		)
	}

	_, err = dbu.EventMetaTable.Select(csndra.SelectParams{
		ResultsBind: &resultsBind,
		ColumnValues: []csndra.ColumnComparator{
			csndra.Comparator(partnCol, eventMetaPartnKey).Eq(),
			csndra.Comparator(aggIDCol, aggID).Eq(),
		},
		SelectColumns: dbu.EventMetaTable.Columns(),
	})

	if err != nil {
		err = errors.Wrap(err, "Error Fetching AggregateVersion from EventMeta")
		return -1, err
	}

	if len(resultsBind) == 0 {
		err = fmt.Errorf("No Aggregates found with ID: %d", aggID)
		err = errors.Wrap(err, "Error Fetching AggregateVersion from EventMeta")
		return -1, err
	}

	metaVersion := resultsBind[0].AggregateVersion

	// Increment aggregate-version in event-meta table
	result := resultsBind[0]
	result.AggregateVersion++
	err = <-dbu.EventMetaTable.AsyncInsert(&result)
	if err != nil {
		return -1, err
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

	events := &[]model.Event{}
	yearBucketCol, _ := dbu.EventTable.Column("yearBucket")
	aggIDCol, _ := dbu.EventTable.Column("aggregateID")
	versionCol, _ := dbu.EventTable.Column("version")

	sp := csndra.SelectParams{
		ResultsBind: events,
		ColumnValues: []csndra.ColumnComparator{
			csndra.Comparator(yearBucketCol, eventStoreQuery.YearBucket).Eq(),
			csndra.Comparator(aggIDCol, aggID).Eq(),
			csndra.Comparator(versionCol, aggVersion).Gt(),
			csndra.Comparator(versionCol, eventMetaVersion).Lt(),
		},
		SelectColumns: dbu.EventTable.Columns(),
	}

	_, err := dbu.EventTable.Select(sp)
	return events, err
}
