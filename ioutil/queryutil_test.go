package ioutil

import (
	"encoding/json"
	"log"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"

	"github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-eventstore-query/mock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("QueryUtil", func() {
	var (
		eventStore          *mock.MEventStore
		mockEvents          []model.Event
		mockMetaVer         int64
		mockEventStoreQuery *model.EventStoreQuery
	)
	var genEvent = func(version int64) *model.Event {
		uuid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())

		return &model.Event{
			AggregateID: mockEventStoreQuery.AggregateID,
			Version:     version,
			YearBucket:  2018,
			Data:        []byte("test"),
			UserUUID:    uuid,
			NanoTime:    time.Now().UnixNano(),
			UUID:        uuid,
			Action:      "insert-test",
		}
	}

	BeforeEach(func() {
		mockEventStoreQuery = &model.EventStoreQuery{
			AggregateID:      12,
			AggregateVersion: 3,
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
			eventStore = &mock.MEventStore{
				MockMetaVersion: func(aggID int8) (int64, error) {
					Expect(aggID).To(Equal(mockEventStoreQuery.AggregateID))
					return mockMetaVer, nil
				},
				MockEvents: func(
					aggID int8,
					aggVersion int64,
					yearBucket int16,
					eventMetaVersion int64,
				) ([]model.Event, error) {
					Expect(aggID).To(Equal(mockEventStoreQuery.AggregateID))
					Expect(aggVersion).To(Equal(mockEventStoreQuery.AggregateVersion))
					Expect(yearBucket).To(Equal(mockEventStoreQuery.YearBucket))
					Expect(eventMetaVersion).To(Equal(mockMetaVer))
					return mockEvents, nil
				},
			}

			qu, err := NewQueryUtil(eventStore, 6, &mock.Logger{})
			Expect(err).ToNot(HaveOccurred())
			// 0 is the partition-key. It is usually set from env var,
			// but here we just hard-code for convenience.
			qu.QueryHandler(mockEventStoreQuery)
		})

		It("should return any errors that occur while getting AggregateMetaVersion", func() {
			eventStore = &mock.MEventStore{
				MockMetaVersion: func(_ int8) (int64, error) {
					return 0, errors.New("some-error")
				},
			}

			qu, err := NewQueryUtil(eventStore, 6, &mock.Logger{})
			Expect(err).ToNot(HaveOccurred())
			_, err = qu.QueryHandler(mockEventStoreQuery)
			Expect(err).To(HaveOccurred())
		})

		It("should return any errors that occur while getting AggregateEvents", func() {
			eventStore = &mock.MEventStore{
				MockMetaVersion: func(v int8) (int64, error) {
					Expect(v).To(Equal(mockEventStoreQuery.AggregateID))
					return mockMetaVer, nil
				},
				MockEvents: func(
					aggID int8,
					aggVersion int64,
					yearBucket int16,
					eventMetaVersion int64,
				) ([]model.Event, error) {
					return nil, errors.New("some-error")
				},
			}

			qu, err := NewQueryUtil(eventStore, 6, &mock.Logger{})
			Expect(err).ToNot(HaveOccurred())
			_, err = qu.QueryHandler(mockEventStoreQuery)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("BatchProduce", func() {
		var responseChan = make(chan *model.Document)

		Context("batch events are processed", func() {
			Specify(
				"the Kafka-Response should appear on response-channel correctly "+
					"after batch-events are processed",
				func(done Done) {
					err := os.Setenv("KAFKA_EVENT_BATCH_SIZE", "4")
					Expect(err).ToNot(HaveOccurred())

					batchSizeStr := os.Getenv("KAFKA_EVENT_BATCH_SIZE")
					batchSize, err := strconv.Atoi(batchSizeStr)
					Expect(err).ToNot(HaveOccurred())

					qu, err := NewQueryUtil(eventStore, batchSize, &mock.Logger{})
					Expect(err).ToNot(HaveOccurred())
					// Listener for batch events (batch events generated below)
					var wg sync.WaitGroup
					wg.Add(1)
					// Here we get the Document from the response channel, unmarshal events array
					// from it, and then match the resulting events with mock-events slice
					go func() {
						defer GinkgoRecover()
						matchCount := 0
						batchSizeMaintained := true
						for res := range responseChan {
							resEvents := []model.Event{}
							err := json.Unmarshal(res.Data, &resEvents)
							Expect(err).ToNot(HaveOccurred())

							log.Printf("Found batch size: %d", len(resEvents))
							log.Printf("Expected batch size: %d", batchSize)
							// Check that events are received in correct batch-size
							// Only last events-batch can have incorrect batch-size,
							// so we check before the last value is assigned
							Expect(batchSizeMaintained).To(BeTrue())
							batchSizeMaintained = len(resEvents) == batchSize

							// Ensure every events from mock-events is matched
							for _, re := range resEvents {
								matches := false
								for _, e := range mockEvents {
									// Convert NanoTimes to consistent formats
									matches = reflect.DeepEqual(re, e)
									if matches {
										matchCount++
										break
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

					// BatchEvents produced here
					docs := qu.CreateBatch(37, uuuid.UUID{}, mockEvents)
					// Pass Document
					for _, doc := range docs {
						responseChan <- &doc
					}
					wg.Wait()
					close(done)
				}, 20,
			)
		})
	})
})
