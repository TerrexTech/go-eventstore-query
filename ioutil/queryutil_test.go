package ioutil

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-eventstore-query/mock"
	cql "github.com/gocql/gocql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

var _ = Describe("QueryUtil", func() {
	var (
		dbUtil              *mock.DBUtil
		mockEvents          []model.Event
		mockMetaVer         int64
		mockEventStoreQuery *model.EventStoreQuery
	)
	var genEvent = func(version int64) *model.Event {
		uuid, err := cql.RandomUUID()
		Expect(err).ToNot(HaveOccurred())

		return &model.Event{
			AggregateID: mockEventStoreQuery.AggregateID,
			Version:     version,
			YearBucket:  mockEventStoreQuery.YearBucket,
			Data:        "test",
			UserID:      3,
			Timestamp:   time.Now(),
			UUID:        uuid,
			Action:      "insert",
		}
	}

	BeforeEach(func() {
		mockEventStoreQuery = &model.EventStoreQuery{
			AggregateID:      12,
			AggregateVersion: 3,
			YearBucket:       2019,
		}

		mockEvents = []model.Event{
			*genEvent(2),
			*genEvent(4),
			*genEvent(7),
			*genEvent(9),
			*genEvent(10),
			*genEvent(12),
			*genEvent(13),
			*genEvent(20),
			*genEvent(21),
			*genEvent(22),
			*genEvent(23),
			*genEvent(24),
			*genEvent(40),
			*genEvent(43),
			*genEvent(50),
		}
		mockMetaVer = 16
	})

	Describe("QueryHandler", func() {
		It("should return events", func() {
			dbUtil = &mock.DBUtil{
				MockMetaVersion: func(q *model.EventStoreQuery) (int64, error) {
					Expect(reflect.DeepEqual(q, mockEventStoreQuery)).To(BeTrue())
					return mockMetaVer, nil
				},
				MockEvents: func(q *model.EventStoreQuery, mv int64) (*[]model.Event, error) {
					Expect(q.AggregateID).To(Equal(mockEventStoreQuery.AggregateID))
					Expect(q.AggregateVersion).To(Equal(mockEventStoreQuery.AggregateVersion))
					Expect(q.YearBucket).To(Equal(mockEventStoreQuery.YearBucket))

					Expect(mv).To(Equal(mockMetaVer))
					return &mockEvents, nil
				},
			}

			qu := &QueryUtil{
				DBUtil: dbUtil,
			}
			qu.QueryHandler(mockEventStoreQuery)
		})

		It("should return any errors that occur while getting AggregateMetaVersion", func() {
			dbUtil = &mock.DBUtil{
				MockMetaVersion: func(q *model.EventStoreQuery) (int64, error) {
					return 0, errors.New("some-error")
				},
			}

			qu := &QueryUtil{
				DBUtil: dbUtil,
			}
			_, err := qu.QueryHandler(mockEventStoreQuery)
			Expect(err).To(HaveOccurred())
		})

		It("should return any errors that occur while getting AggregateEvents", func() {
			dbUtil = &mock.DBUtil{
				MockMetaVersion: func(q *model.EventStoreQuery) (int64, error) {
					Expect(reflect.DeepEqual(q, mockEventStoreQuery)).To(BeTrue())
					return mockMetaVer, nil
				},
				MockEvents: func(q *model.EventStoreQuery, mv int64) (*[]model.Event, error) {
					return nil, errors.New("some-error")
				},
			}

			qu := &QueryUtil{
				DBUtil: dbUtil,
			}
			_, err := qu.QueryHandler(mockEventStoreQuery)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("BatchProduce", func() {
		var responseChan = make(chan *model.KafkaResponse)

		Context("batch events are processed", func() {
			Specify(
				"the Kafka-Response should appear on response-channel correctly "+
					"after batch-events are processed",
				func(done Done) {
					err := os.Setenv("KAFKA_EVENT_BATCH_SIZE", "4")
					Expect(err).ToNot(HaveOccurred())

					qu := &QueryUtil{
						ResponseChan: (chan<- *model.KafkaResponse)(responseChan),
					}

					var wg sync.WaitGroup
					wg.Add(1)
					// Here we get the KafkaResponse from the response channel, unmarshal events array
					// from it, and then match the resulting events with mock-events slice
					go func() {
						defer GinkgoRecover()
						matchCount := 0
						batchSizeMaintained := true
						batchSizeEnv := os.Getenv("KAFKA_EVENT_BATCH_SIZE")
						for res := range responseChan {
							resEvents := []model.Event{}
							err := json.Unmarshal([]byte(res.Result), &resEvents)
							Expect(err).ToNot(HaveOccurred())

							// Check that events are received in correct batch-size
							// Only last events-batch can have incorrect batch-size,
							// so we check before the last value is assigned
							Expect(batchSizeMaintained).To(BeTrue())
							eventsSize := fmt.Sprintf("%d", len(resEvents))
							batchSizeMaintained = eventsSize == batchSizeEnv

							matches := false
							// Ensure every events from mock-events is matched
							for _, re := range resEvents {
								for _, e := range mockEvents {
									// Convert timestamps to consistent formats
									re.Timestamp = time.Unix(re.Timestamp.Unix(), 0)
									e.Timestamp = time.Unix(e.Timestamp.Unix(), 0)
									matches = reflect.DeepEqual(re, e)
									if matches {
										matchCount++
									}
								}
							}
							// All events matched, good stuff
							if matchCount == len(mockEvents) {
								close(responseChan)
							}
						}
						wg.Done()
					}()

					qu.BatchProduce(37, &mockEvents)
					wg.Wait()
					close(done)
				}, 2,
			)
		})
	})
})
