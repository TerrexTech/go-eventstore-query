package mock

import (
	"github.com/TerrexTech/go-common-models/model"
)

// MEventStore is a mock for ioutil.EventStore.
type MEventStore struct {
	MockEvents func(
		aggID int8,
		aggVersion int64,
		yearBucket int16,
		eventMetaVersion int64,
	) ([]model.Event, error)

	MockMetaVersion func(aggregateID int8) (int64, error)
}

// GetAggMetaVersion mocks the #GetAggMetaVersion function of ioutil.EventStore.
func (dbu *MEventStore) GetAggMetaVersion(aggregateID int8) (int64, error) {
	if dbu.MockMetaVersion != nil {
		return dbu.MockMetaVersion(aggregateID)
	}

	return 1, nil
}

// GetAggEvents mocks the #GetAggEvents function of ioutil.EventStore.
func (dbu *MEventStore) GetAggEvents(
	aggID int8,
	aggVersion int64,
	yearBucket int16,
	eventMetaVersion int64,
) ([]model.Event, error) {
	if dbu.MockMetaVersion != nil {
		return dbu.MockEvents(
			aggID,
			aggVersion,
			yearBucket,
			eventMetaVersion,
		)
	}

	return []model.Event{}, nil
}
