EventStoreQuery
---

This service provides Aggregates with new events.

This is achieved by comparing Aggregate's current version in its database to the version in EventStore.

### How the service Works:

* The `events_meta` table contains the aggregate-version for new events. New [events][0] will refer version from this table. Check [Go-EventPersistence][1] service.

* The actual events are stored in `events` table (Event-Store).

* Every Aggregate maintains its own version. The Aggregate-ID and its current version is sent as request via Kafka-topic `esquery.request` to this service.

* This service compares the Aggregate's version with the version in `events_meta`. The version in `events_meta` is then incremented by 1, so the new events will get the new version.

* All the events from event-store (`events` table) with version > Aggregate-version and < event_meta version are batched into chunks and sent via [Kafka-response][2] on the topic provided in [EventStoreQuery][3].

* The end of Event-Stream is signalled using an extra Event containing `Action` as `__eos__`, and `UUID` matching the provided `CorrelationID` in `EventStoreQuery`.

* These events are to be consumed by the Aggregate service and processed, and the Aggregate will make required changes in its projection. The Aggregate must stop listening for messages when the end-of-stream Event is received, since that signals completion of that request.

Check [.env][4] and [docker-compose.yaml][5] (docker-compose is only used in tests as of yet) files for default configurations (including the Cassandra Keyspace/Table used).

  [0]: https://github.com/TerrexTech/go-common-models/blob/master/models/event.go
  [1]: https://github.com/TerrexTech/go-eventpersistence/
  [2]: https://github.com/TerrexTech/go-common-models/blob/master/models/kafka_response.go
  [3]: https://github.com/TerrexTech/go-common-models/blob/master/model/eventstore_query.go
  [4]: https://github.com/TerrexTech/go-eventstore-query/blob/master/.env
  [5]: https://github.com/TerrexTech/go-eventstore-query/blob/master/test/docker-compose.yaml
