package ioutil

import (
	"log"
	"os"
	"reflect"
	"time"

	csndra "github.com/TerrexTech/go-cassandrautils/cassandra"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventstore-models/bootstrap"
	"github.com/TerrexTech/go-eventstore-models/model"
	cql "github.com/gocql/gocql"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("DBUtil", func() {
	var (
		aggID          int8
		eventMetaTable *csndra.Table
		eventTable     *csndra.Table
	)

	BeforeSuite(func() {
		hosts := os.Getenv("CASSANDRA_HOSTS")
		username := os.Getenv("CASSANDRA_USERNAME")
		password := os.Getenv("CASSANDRA_PASSWORD")
		keyspace := os.Getenv("CASSANDRA_KEYSPACE")
		testKeyspace := os.Getenv("CASSANDRA_KEYSPACE_TEST")

		// Backup current keyspace-name to another env var and switch current keyspace with test
		err := os.Setenv("CASSANDRA_KEYSPACE_PRETEST", keyspace)
		Expect(err).ToNot(HaveOccurred())
		err = os.Setenv("CASSANDRA_KEYSPACE", testKeyspace)
		Expect(err).ToNot(HaveOccurred())
		keyspace = os.Getenv("CASSANDRA_KEYSPACE")

		_, err = commonutil.ValidateEnv(
			"CASSANDRA_HOSTS",
			"CASSANDRA_USERNAME",
			"CASSANDRA_PASSWORD",
			"CASSANDRA_KEYSPACE",
			"CASSANDRA_KEYSPACE_TEST",
			"CASSANDRA_KEYSPACE_PRETEST",
		)
		Expect(err).ToNot(HaveOccurred())

		cluster := cql.NewCluster(*commonutil.ParseHosts(hosts)...)
		cluster.ConnectTimeout = time.Millisecond * 10000
		cluster.Timeout = time.Millisecond * 10000
		cluster.ProtoVersion = 4
		cluster.RetryPolicy = &cql.ExponentialBackoffRetryPolicy{
			NumRetries: 5,
		}

		if username != "" && password != "" {
			cluster.Authenticator = cql.PasswordAuthenticator{
				Username: username,
				Password: password,
			}
		}
		session, err := csndra.GetSession(cluster)
		Expect(err).ToNot(HaveOccurred())

		log.Println("Dropping Keyspace")
		q := session.Query("DROP KEYSPACE IF EXISTS " + keyspace)
		err = q.Exec()
		Expect(err).ToNot(HaveOccurred())
		q.Release()

		keyspaceConfig := csndra.KeyspaceConfig{
			Name:                keyspace,
			ReplicationStrategy: "NetworkTopologyStrategy",
			ReplicationStrategyArgs: map[string]int{
				"datacenter1": 1,
			},
		}
		_, err = csndra.NewKeyspace(session, keyspaceConfig)
		Expect(err).ToNot(HaveOccurred())

		eventMetaTable, err = bootstrap.EventMeta()
		Expect(err).ToNot(HaveOccurred())
		eventTable, err = bootstrap.Event()
		Expect(err).ToNot(HaveOccurred())
	})

	AfterSuite(func() {
		// Set the CASSANDRA_KEYSPACE value to CASSANDRA_KEYSPACE_PRETEST
		// and unset CASSANDRA_KEYSPACE_PRETEST
		keyspacePretest := os.Getenv("CASSANDRA_KEYSPACE_PRETEST")
		err := os.Setenv("CASSANDRA_KEYSPACE", keyspacePretest)
		Expect(err).ToNot(HaveOccurred())

		err = os.Unsetenv("CASSANDRA_KEYSPACE_PRETEST")
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("GetAggMetaVersion", func() {
		var dbUtil *DBUtil

		BeforeEach(func() {
			// This resets the event-meta row to initial value
			aggID = 12
			eventMeta := &model.EventMeta{
				AggregateID:      aggID,
				AggregateVersion: 43,
				YearBucket:       2019,
			}
			err := <-eventMetaTable.AsyncInsert(eventMeta)
			Expect(err).ToNot(HaveOccurred())

			dbUtil = &DBUtil{
				EventMetaTable: eventMetaTable,
				EventTable:     eventTable,
			}
		})

		It("should return aggregate meta-version", func() {
			query := &model.EventStoreQuery{
				AggregateID:      aggID,
				AggregateVersion: 43,
				YearBucket:       2019,
			}
			ver, err := dbUtil.GetAggMetaVersion(query)
			Expect(err).ToNot(HaveOccurred())

			Expect(ver).To(Equal(query.AggregateVersion))
		})

		It("should return error if AggregateID is not specified", func() {
			query := &model.EventStoreQuery{
				AggregateVersion: 43,
				YearBucket:       2019,
			}
			ver, err := dbUtil.GetAggMetaVersion(query)
			Expect(err).To(HaveOccurred())
			Expect(ver).To(Equal(int64(0)))
		})

		It("should return error if YearBucket is not specified", func() {
			query := &model.EventStoreQuery{
				AggregateID:      aggID,
				AggregateVersion: 98,
			}
			ver, err := dbUtil.GetAggMetaVersion(query)
			Expect(err).To(HaveOccurred())
			Expect(ver).To(Equal(int64(0)))
		})

		It(
			"should return error if no matching record were found with secified AggregateID",
			func() {
				query := &model.EventStoreQuery{
					AggregateID:      43,
					AggregateVersion: 98,
					YearBucket:       2019,
				}
				ver, err := dbUtil.GetAggMetaVersion(query)
				Expect(err).To(HaveOccurred())
				Expect(ver).To(Equal(int64(0)))
			},
		)

		It(
			"should increment aggregate meta-version after retrieving it",
			func() {
				query := &model.EventStoreQuery{
					AggregateID:      aggID,
					AggregateVersion: 43,
					YearBucket:       2019,
				}
				ver, err := dbUtil.GetAggMetaVersion(query)
				Expect(err).ToNot(HaveOccurred())
				Expect(ver).To(Equal(query.AggregateVersion))

				aggIDCol, err := eventMetaTable.Column("aggregateID")
				Expect(err).ToNot(HaveOccurred())
				yearBucketCol, err := eventMetaTable.Column("yearBucket")
				Expect(err).ToNot(HaveOccurred())

				bind := []model.EventMeta{}
				sp := csndra.SelectParams{
					ColumnValues: []csndra.ColumnComparator{
						csndra.Comparator(yearBucketCol, query.YearBucket).Eq(),
						csndra.Comparator(aggIDCol, query.AggregateID).Eq(),
					},
					ResultsBind:   &bind,
					SelectColumns: eventMetaTable.Columns(),
				}

				_, err = eventMetaTable.Select(sp)
				Expect(bind).To(HaveLen(1))
				meta := bind[0]

				Expect(err).ToNot(HaveOccurred())
				Expect(meta.AggregateID).To(Equal(query.AggregateID))
				Expect(meta.YearBucket).To(Equal(query.YearBucket))
				Expect(meta.AggregateVersion).To(Equal(query.AggregateVersion + 1))
			},
		)
	})

	Describe("GetAggEvents", func() {
		var dbUtil *DBUtil
		var mockEvent model.Event

		BeforeEach(func() {
			// This resets the event-meta row to initial value
			aggID = 12
			uuid, err := cql.RandomUUID()
			Expect(err).ToNot(HaveOccurred())

			mockEvent = model.Event{
				AggregateID: aggID,
				Version:     10,
				YearBucket:  2019,
				Data:        "test",
				UserID:      3,
				Timestamp:   time.Now(),
				UUID:        uuid,
				Action:      "insert",
			}
			err = <-eventTable.AsyncInsert(&mockEvent)
			Expect(err).ToNot(HaveOccurred())
			// Insert some varied versions for testing
			mockEvent.Version = 11
			err = <-eventTable.AsyncInsert(&mockEvent)
			Expect(err).ToNot(HaveOccurred())
			mockEvent.Version = 13
			err = <-eventTable.AsyncInsert(&mockEvent)
			Expect(err).ToNot(HaveOccurred())

			dbUtil = &DBUtil{
				EventMetaTable: eventMetaTable,
				EventTable:     eventTable,
			}
		})

		It("should return new events for Aggregate", func() {
			query := &model.EventStoreQuery{
				AggregateID:      aggID,
				AggregateVersion: 10,
				YearBucket:       2019,
			}
			events, err := dbUtil.GetAggEvents(query, 40)
			Expect(err).ToNot(HaveOccurred())

			Expect(*events).To(HaveLen(2))

			e0 := (*events)[0]
			mockEvent.Version = 13
			// Convert timestamps to consistent formats
			mockEvent.Timestamp = time.Unix(mockEvent.Timestamp.Unix(), 0)
			e0.Timestamp = time.Unix(e0.Timestamp.Unix(), 0)
			Expect(reflect.DeepEqual(e0, mockEvent)).To(BeTrue())

			e1 := (*events)[1]
			mockEvent.Version = 11
			e1.Timestamp = time.Unix(e1.Timestamp.Unix(), 0)
			Expect(reflect.DeepEqual(e1, mockEvent)).To(BeTrue())
		})

		It("should throw error if AggregateID is not specified", func() {
			query := &model.EventStoreQuery{
				AggregateVersion: 10,
				YearBucket:       2019,
			}
			_, err := dbUtil.GetAggEvents(query, 40)
			Expect(err).To(HaveOccurred())
		})

		It("should throw error if AggregateVersion is not specified", func() {
			query := &model.EventStoreQuery{
				AggregateID: aggID,
				YearBucket:  2019,
			}
			_, err := dbUtil.GetAggEvents(query, 40)
			Expect(err).To(HaveOccurred())
		})

		It("should throw error if YearBucket is not specified", func() {
			query := &model.EventStoreQuery{
				AggregateID:      aggID,
				AggregateVersion: 10,
			}
			_, err := dbUtil.GetAggEvents(query, 40)
			Expect(err).To(HaveOccurred())
		})
	})
})
