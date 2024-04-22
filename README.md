# Respite

**NOTE: Respite is in active development. It is not yet production-ready.**

Respite is a re-implementation of the Redis protocol on SQLite.

At its core, Respite is a single-threaded epoll TCP server that speaks the Redis protocol and uses SQLite for its backing store. Just like Redis, Respite can easily handle a huge number of clients sending commands at the same time.

## Compatibility

Respite is intended to be compatibile with existing Redis clients, libraries and tools including `redis-cli` and `redis-benchmark`. This means Respite can be thought of as a drop-in alternative to Redis for most projects.

While Respite may be a drop-in replacement, the use of SQLite does have implications that can either help or hurt depending on the specific use-case.

For use-cases where data-persistance is desired (not just an in-memory cache), Respite is an excellent option:
* Stores as much data as you have disk space for with no memory capacity limit.
* Excellent write durability provided by SQLite.
* Turns `MULTI`, `EXEC` into a real database transaction.

For use-cases that are essentially just an in-memory cache, Respite can still work well:
* Good: use SQLite's in-memory database, avoiding all disk writes and clearing on restart.
* Bad: missing support for key eviction policies and memory usage cap, etc.

## Networking

Both Respite and Redis use the same approach for networking and speak [RESP2](https://redis.io/docs/latest/develop/reference/protocol-spec/). This means you can carry over same expectations about connections and use the same clients as you would with Redis.

Client connections are easy to open, can either be long-lived or transient, and you can have a lot or a little of them at any time. Concerns around connections that may be highly relevant for something like Postgres are a non-issue for Respite (and Redis itself).

## Performance

`redis-benchmark -q -c 10 -n 1000000 -r 100000 -d 64 -t get,set`
