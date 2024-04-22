# Respite

**NOTE: Respite is in active development. It is not yet production-ready.**

Respite is an implementation of the Redis server API on SQLite.

Said another way, Respite is an epoll TCP server that speaks the Redis protocol and uses SQLite for its backing store.

Just like Redis, Respite can easily handle a huge number of clients sending commands at the same time.

## Supported commands

Below are the currently supported commands. Adding support for more commands is ongoing.

#### Transactions

`MULTI`
`EXEC`
`DISCARD`

#### Strings

`GET`
`SET`
`DEL`
`EXPIRE`
`EXPIREAT`
`TTL`
`EXPIRETIME`
`EXISTS`
`PERSIST`
`TYPE`
`DECR`
`DECRBY`
`INCR`
`INCRBY`
`GETDEL`

#### Hashes

`HSET`
`HSETNX`
`HGET`
`HINCRBY`
`HDEL`
`HEXISTS`
`HGETALL`
`HLEN`
`HKEYS`

#### Sets

`SADD`
`SCARD`
`SISMEMBER`
`SMEMBERS`
`SREM`
`SPOP`

#### Sorted sets

`ZADD`
`ZREM`
`ZCARD`
`ZSCORE`
`ZCOUNT`
`ZREMRANGEBYSCORE`

#### Connections

`AUTH`
`PING`
`ECHO`

## Compatibility

Respite is intended to be compatibile with existing Redis clients, libraries and tools including `redis-cli` and `redis-benchmark`. This means Respite can be thought of as a drop-in alternative to Redis for many projects.

While Respite may be a drop-in replacement, the use of SQLite does have implications that can either help or hurt depending on the specific use-case.

For use-cases where data persistence is desired (not just an in-memory cache), Respite is an excellent option:
* Stores as much data as you have disk space for with no memory capacity limit.
* Excellent write durability provided by SQLite.
* Turns `MULTI`, `EXEC` into a real database transaction.

For use-cases that are essentially just an in-memory cache, Respite can still work well:
* Good: use SQLite's in-memory database, avoiding all disk writes and clearing on restart.
* Bad: missing support for key eviction policies and memory usage cap, etc.

## Configuration

Command line configuration compatibility with Redis is a goal, within reason. So far, the following configuration options are supported:

* `-h hostname`
* `-p port`
* `--dir my/path`
* `--dbfilename respite.sqlite`
* `--save ""` (for in-memory only)
* `--requirepass password`

## Networking

Both Respite and Redis use the same approach for networking and speak [RESP2](https://redis.io/docs/latest/develop/reference/protocol-spec/). This means you can carry over same expectations about connections and use the same clients as you would with Redis.

Client connections are easy to open, can either be long-lived or transient, and you can have a lot or a little of them at any time. Concerns around connections that may be highly relevant for something like Postgres are a non-issue for Respite (and Redis itself).

## Performance

As one would expect, Redis generally is able to handle more requests per second than Respite. In exchange, Respite has no memory storage limit and a better data persistence story. While more requests per second sounds great, it is not the only concern to think about.

Command: `redis-benchmark -q -c 10 -n 1000000 -r 100000 -d 64 -t get,set`

### Google Cloud n2d-standard-2 VM, 2 vCPU + 8GB + balanced disk, $60/mo

Separate client and server in the same zone on same VPC.

Respite in-memory only `respite-server --save ""`

```
SET: 31436.65 requests per second, p50=0.279 msec
GET: 32840.72 requests per second, p50=0.263 msec
```

Redis in-memory only `redis-server --save ""`

```
SET: 43731.14 requests per second, p50=0.175 msec
GET: 46834.02 requests per second, p50=0.159 msec
```

Respite `respite-server`

```
SET: 22706.63 requests per second, p50=0.359 msec
GET: 30275.51 requests per second, p50=0.295 msec
```

Redis `redis-server --save "" --appendonly yes`

```
SET: 3342.47 requests per second, p50=2.919 msec      --appendfsync always
SET: 41684.04 requests per second, p50=0.183 msec     --appendfsync everysec
GET: 45167.12 requests per second, p50=0.167 msec
```

Very interesting results! It appears is that Redis with `--appendfsync everysec` is running face-first into the IOPS limit of GCP balanced disks, whereas SQLite appears not to. If you can sacrifice durability with `--appendfsync everysec` you avoid this sharp edge.

Performance can be hard to predict. It appears Respite using SQLite would perform far better for write-heavy workloads where risking data-loss is unacceptable.

### M1 MacBook Pro (local client and server, LOW VALUE METRIC!)

Respite in-memory only `respite-server --save ""`

```
SET: 72332.73 requests per second, p50=0.119 msec
GET: 77447.34 requests per second, p50=0.111 msec
```

Redis in-memory only `redis-server --save ""`

```
SET: 121212.12 requests per second, p50=0.063 msec
GET: 131943.53 requests per second, p50=0.055 msec
```

Respite `respite-server`

```
SET: 48206.71 requests per second, p50=0.175 msec
GET: 56773.02 requests per second, p50=0.167 msec
```

Redis `redis-server --save "" --appendonly yes`

```
SET: 110399.65 requests per second, p50=0.071 msec
GET: 125659.71 requests per second, p50=0.063 msec
```

## Building

To build Respite locally, clone the repo and run the following command from the repo base directory.

`nim c -r -d:release -d:useMalloc -o:respite-server src/respite.nim`

Note that this does require having [Nim](https://nim-lang.org/) installed and in your path.
