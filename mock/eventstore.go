package mock

import (
	"github.com/TerrexTech/go-eventstore-models/model"
)

// MEventStore is a mock for ioutil.EventStore.
type MEventStore struct {
	MockEvents      func(*model.EventStoreQuery, int64) (*[]model.Event, error)
	MockMetaVersion func(*model.EventStoreQuery) (int64, error)
}

// GetAggMetaVersion mocks the #GetAggMetaVersion function of ioutil.EventStore.
func (dbu *MEventStore) GetAggMetaVersion(
	eventStoreQuery *model.EventStoreQuery,
) (int64, error) {
	if dbu.MockMetaVersion != nil {
		return dbu.MockMetaVersion(eventStoreQuery)
	}

	return 1, nil
}

// GetAggEvents mocks the #GetAggEvents function of ioutil.EventStore.
func (dbu *MEventStore) GetAggEvents(
	eventStoreQuery *model.EventStoreQuery,
	eventMetaVersion int64,
) (*[]model.Event, error) {
	if dbu.MockMetaVersion != nil {
		return dbu.MockEvents(eventStoreQuery, eventMetaVersion)
	}

	return &[]model.Event{}, nil
}
