# Redis Streams Deep Dive

Redis Streams is the backbone of Buraq. This document explains the specific
Redis commands Buraq uses, why consumer groups matter, and how to reason about
message lifecycle from enqueue to acknowledgement.

## What is a Redis Stream?

A Redis Stream is an append-only log data structure. Think of it like Kafka
topics or PostgreSQL's WAL -- every write appends a new entry with a
monotonically increasing ID (format: `<milliseconds>-<sequence>`).

```
XADD buraq_tasks * payload '{"id":"task-1","type":"email_notification",...}'
```

Returns: `"1719580800000-0"`

That ID is the message's unique address within the stream. Redis generates it
automatically based on the current timestamp and an internal counter.

## XADD -- Enqueuing Tasks

Buraq's `redisadapter.Store.Add()` wraps `XADD`:

```go
// redisadapter/adapter.go
func (s *Store) Add(ctx context.Context, stream string, t *task.Task) (string, error) {
    data, err := t.Marshal()
    if err != nil {
        return "", err
    }

    return s.client.XAdd(ctx, &redis.XAddArgs{
        Stream: stream,
        Values: map[string]interface{}{
            "payload": data,
        },
    }).Result()
}
```

Key points:
- The `*` lets Redis auto-generate the ID. You should never set IDs manually
  unless you need deterministic ordering for testing.
- Buraq stores the entire task as a single `payload` field. This keeps the
  schema simple -- adding new fields means changing the JSON, not the stream
  structure.
- `XADD` returns the message ID, which the producer logs for traceability.

## Consumer Groups

A consumer group is Redis's mechanism for distributing messages across multiple
consumers. Without a group, every consumer reads every message (broadcast).
With a group, each message is delivered to exactly one consumer (competing
consumers).

### Creating a Group

```go
// redisadapter/adapter.go
func (s *Store) CreateGroup(ctx context.Context, stream, group string) error {
    err := s.client.XGroupCreateMkStream(ctx, stream, group, "0").Err()
    if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
        return err
    }
    return nil
}
```

`XGROUP CREATE MKSTREAM` does two things:
1. Creates the stream if it does not exist (`MKSTREAM`).
2. Creates the group starting from ID `0`, meaning "deliver all existing
   messages on first read".

The `BUSYGROUP` error is expected when the group already exists. Buraq ignores
it so `Start()` is idempotent.

### XREADGROUP -- Fetching Messages

```go
// redisadapter/adapter.go
func (s *Store) ReadGroup(ctx context.Context, stream, group, consumer string,
    count int64, block interface{}) ([]task.StreamMessage, error) {

    blockDuration, ok := block.(time.Duration)
    if !ok {
        blockDuration = 2 * time.Second
    }

    streams, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
        Group:    group,
        Consumer: consumer,
        Streams:  []string{stream, ">"},
        Count:    count,
        Block:    blockDuration,
    }).Result()
    // ... convert to []task.StreamMessage
}
```

Arguments explained:

| Argument  | Meaning                                                     |
|-----------|-------------------------------------------------------------|
| `Group`   | The consumer group name (`buraq_workers`).                  |
| `Consumer`| This specific consumer's name (`worker_node_1`).            |
| `Streams` | Read from `buraq_tasks`, with `>` meaning "only new         |
|           | messages not yet delivered to this group."                  |
| `Count`   | Maximum messages per call (batch size).                     |
| `Block`   | Wait up to N seconds if no messages are available.          |

The `>` special ID is critical. It tells Redis "give me messages that have
never been delivered to any consumer in this group." If you pass `0` instead,
Redis returns messages from the PEL (see below).

### Message States in a Consumer Group

When `XREADGROUP` delivers a message, Redis tracks it internally:

1. **Delivered** -- the message is in the consumer's Pending Entry List (PEL).
2. **Acknowledged** -- `XACK` removes it from the PEL. It is considered done.

If a consumer crashes after `XREADGROUP` but before `XACK`, the message stays
in the PEL. This is the foundation of at-least-once delivery.

## XACK -- Acknowledging Messages

```go
func (s *Store) Ack(ctx context.Context, stream, group, msgID string) error {
    return s.client.XAck(ctx, stream, group, msgID).Err()
}
```

`XACK` tells Redis "this consumer has finished processing this message." Redis
removes it from the group's Pending Entry List. If you never call `XACK`, the
PEL grows unbounded and Redis thinks you have a backlog of unprocessed work.

Buraq acknowledges messages in two cases:
1. After successful processing.
2. After a task fails and is re-queued or moved to the DLQ. The original
   message must be acknowledged so it does not get re-delivered.

## PEL (Pending Entry List)

The PEL is Redis's internal ledger of "delivered but not yet acknowledged"
messages per consumer group. You can inspect it with:

```bash
# See all pending messages for the group
XPENDING buraq_tasks buraq_workers

# Detailed view with consumer names and idle times
XPENDING buraq_tasks buraq_workers - + 10
```

The PEL is what enables reliable delivery. If a consumer dies, its pending
messages can be claimed by another consumer using `XCLAIM`. Buraq does not
currently implement `XCLAIM` (it relies on restart + re-delivery), but the
building blocks are there.

## Stream Trimming

Streams grow forever unless you trim them. Redis offers two strategies:

1. **MAXLEN** -- cap the stream at N entries (approximate with `~`).
2. **MINID** -- discard entries older than a given ID.

```bash
# Keep roughly the last 10000 entries
XTRIM buraq_tasks MAXLEN ~ 10000

# Discard entries older than a specific timestamp
XTRIM buraq_tasks MINID 1719580800000-0
```

Buraq does not trim automatically because acknowledged messages are no longer
needed by the consumer group (they are removed from the PEL on `XACK`).
However, the raw stream data persists. For production deployments, set up a
cron job or Redis function to trim periodically:

```bash
# Example: trim to 100k entries every hour
0 * * * * redis-cli XTRIM buraq_tasks MAXLEN ~ 100000
```

## Putting It All Together

Here is the complete lifecycle of a single task in Buraq:

```
Producer                   Redis Stream               Consumer
   |                            |                          |
   |--- XADD (payload) ------->|                          |
   |                            |                          |
   |                            |<-- XREADGROUP (>) ------|
   |                            |   (message delivered)    |
   |                            |                          |
   |                            |    [process task]        |
   |                            |                          |
   |                            |<-- XACK (msg_id) -------|
   |                            |   (message removed       |
   |                            |    from PEL)             |
```

On failure, the flow diverges:

```
   |                            |                          |
   |                            |    [task failed]         |
   |                            |                          |
   |<-- XADD (re-queue) -------|  (if retries left)       |
   |--- XACK (original) ------>|                          |
   |                            |                          |
   |                            |  OR                      |
   |                            |                          |
   |<-- XADD to DLQ stream --- |  (if retries exhausted)  |
   |--- XACK (original) ------>|                          |
```

## Useful Redis Commands for Debugging

```bash
# List all consumer groups on a stream
XINFO GROUPS buraq_tasks

# See consumers in a group and their pending counts
XINFO CONSUMERS buraq_tasks buraq_workers

# Read the last 10 messages (bypasses consumer groups)
XRANGE buraq_tasks - + COUNT 10

# Check stream length
XLEN buraq_tasks

# Inspect a specific message
XRANGE buraq_tasks 1719580800000-0 1719580800000-0
```

## Trade-offs and Limitations

- **At-least-once, not exactly-once.** A message can be processed twice if the
  consumer crashes after processing but before `XACK`. Design your task
  handlers to be idempotent.
- **No priority ordering.** Streams are strictly FIFO. Buraq's `Priority`
  field on `Task` is a placeholder for future priority queue support (e.g.,
  separate streams per priority level).
- **Single Redis node.** Redis Streams replicate via Redis replication, not
  partitioning. For very high throughput (millions of msgs/sec), consider
  Kafka or sharding across multiple Redis instances.
