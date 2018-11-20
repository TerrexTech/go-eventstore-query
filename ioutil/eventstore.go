package ioutil

import (
	"fmt"

	csndra "github.com/TerrexTech/go-cassandrautils/cassandra"
	"github.com/TerrexTech/go-common-models/model"
	tlog "github.com/TerrexTech/go-logtransport/log"
	"github.com/pkg/errors"
)

// EventStore provides convenience functions for getting Aggregate data.
type EventStore interface {
	GetAggMetaVersion(aggregateID int8) (int64, error)
	GetAggEvents(
		aggID int8,
		aggVersion int64,
		yearBucket int16,
		eventMetaVersion int64,
	) ([]model.Event, error)
}

// eventStore is the implementation for EventStore.
type eventStore struct {
	eventMetaTable   *csndra.Table
	eventTable       *csndra.Table
	metaPartitionKey int8
	logger           tlog.Logger
}

// NewEventStore creates an EventStore.
func NewEventStore(
	eventTable *csndra.Table,
	eventMetaTable *csndra.Table,
	logger tlog.Logger,
	metaPartitionKey int8,
) (EventStore, error) {
	if eventTable == nil {
		return nil, errors.New("eventTable cannot be nil")
	}
	if eventMetaTable == nil {
		return nil, errors.New("eventMetaTable cannot be nil")
	}
	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}

	return &eventStore{
		eventTable:       eventTable,
		eventMetaTable:   eventMetaTable,
		logger:           logger,
		metaPartitionKey: metaPartitionKey,
	}, nil
}

// GetAggMetaVersion gets the AggregateVersion from Events-Meta table.
func (es *eventStore) GetAggMetaVersion(aggID int8) (int64, error) {
	if aggID == 0 {
		return -1, errors.New("AggregateID not specified")
	}

	es.logger.D(tlog.Entry{
		Description: fmt.Sprintf("Getting meta-version for AggregateID: %d", aggID),
	})
	resultsBind := []model.EventMeta{}
	partnCol, err := es.eventMetaTable.Column("partitionKey")
	if err != nil {
		err = errors.Wrapf(
			err,
			"Error getting PartitionKey column from MetaTable for AggregateID: %d", aggID,
		)
		es.logger.E(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		})
		return -1, err
	}
	aggIDCol, err := es.eventMetaTable.Column("aggregateID")
	if err != nil {
		err = errors.Wrapf(
			err,
			"Error getting AggregateID column from MetaTable for AggregateID: %d", aggID,
		)
		es.logger.E(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		})
		return -1, err
	}

	_, err = es.eventMetaTable.Select(csndra.SelectParams{
		ResultsBind: &resultsBind,
		ColumnValues: []csndra.ColumnComparator{
			csndra.Comparator(partnCol, es.metaPartitionKey).Eq(),
			csndra.Comparator(aggIDCol, aggID).Eq(),
		},
		SelectColumns: es.eventMetaTable.Columns(),
	})
	es.logger.D(tlog.Entry{
		Description: fmt.Sprintf(
			"Getting MetaVersion using params: PrtnKey: %d | AggID: %d",
			es.metaPartitionKey,
			aggID,
		),
	}, resultsBind)

	if err != nil {
		err = errors.Wrap(err, "Error Fetching AggregateVersion from EventMeta")
		return -1, err
	}

	if len(resultsBind) == 0 {
		err = fmt.Errorf("no Aggregates found with ID: %d", aggID)
		err = errors.Wrap(err, "Error Fetching AggregateVersion from EventMeta")
		es.logger.E(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		})
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

	es.logger.D(tlog.Entry{
		Description: fmt.Sprintf("Updated MetaVersion as: %d", result.AggregateVersion),
	})
	return metaVersion, nil
}

// GetAggEvents gets the AggregateEvents from Events table.
func (es *eventStore) GetAggEvents(
	aggID int8,
	aggVersion int64,
	yearBucket int16,
	eventMetaVersion int64,
) ([]model.Event, error) {
	if aggID == 0 {
		err := errors.New("AggregateID not specified")
		es.logger.E(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		})
		return nil, err
	}
	if aggVersion == 0 {
		err := fmt.Errorf("AggregateVersion not specified for AggregateID: %d", aggID)
		es.logger.E(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		})
		return nil, err
	}

	events := []model.Event{}
	yearBucketCol, _ := es.eventTable.Column("yearBucket")
	aggIDCol, _ := es.eventTable.Column("aggregateID")
	versionCol, _ := es.eventTable.Column("version")

	sp := csndra.SelectParams{
		ResultsBind: &events,
		ColumnValues: []csndra.ColumnComparator{
			csndra.Comparator(yearBucketCol, yearBucket).Eq(),
			csndra.Comparator(aggIDCol, aggID).Eq(),
			csndra.Comparator(versionCol, aggVersion).Gt(),
			csndra.Comparator(versionCol, eventMetaVersion).LtOrEq(),
		},
		SelectColumns: es.eventTable.Columns(),
	}
	es.logger.D(tlog.Entry{
		Description: "Getting events using parameters",
	}, sp)

	_, err := es.eventTable.Select(sp)
	if err != nil {
		err = errors.Wrap(err, "Error in GetAggEvents")
		return nil, err
	}
	es.logger.D(tlog.Entry{
		Description: "Got events from EventStore",
	}, events)
	return events, nil
}
