package mock

import (
	"github.com/TerrexTech/go-eventstore-models/model"
)

// DBUtil is a mock for DBUtilI
type DBUtil struct {
	MockEvents      func(*model.EventStoreQuery, int64) (*[]model.Event, error)
	MockMetaVersion func(int8, *model.EventStoreQuery) (int64, error)
}

// GetAggMetaVersion mocks the #GetAggMetaVersion function of DBUtilI.
func (dbu *DBUtil) GetAggMetaVersion(
	eventMetaPartnKey int8,
	eventStoreQuery *model.EventStoreQuery,
) (int64, error) {
	if dbu.MockMetaVersion != nil {
		return dbu.MockMetaVersion(eventMetaPartnKey, eventStoreQuery)
	}

	return 1, nil
}

// GetAggEvents mocks the #GetAggEvents function of DBUtilI.
func (dbu *DBUtil) GetAggEvents(
	eventStoreQuery *model.EventStoreQuery,
	eventMetaVersion int64,
) (*[]model.Event, error) {
	if dbu.MockMetaVersion != nil {
		return dbu.MockEvents(eventStoreQuery, eventMetaVersion)
	}

	return &[]model.Event{}, nil
}
