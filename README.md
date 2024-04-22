# Respite

**NOTE: Respite is in active development. It is not yet production-ready.**

Respite is a re-implementation of the Redis protocol on SQLite.

At its core, Respite is a single-threaded epoll TCP server that speaks the Redis protocol and uses SQLite for its backing store. Just like Redis, Respite can easily handle a huge number of clients sending commands at the same time.

## Supported commands

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

`PING`
`ECHO`

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

As one would expect, Redis is able to handle more requests per second than Respite. In exchange, Respite has no memory storage limit and a better data persistance story.

While more requests per second sounds great, it is not the only concern to think about! And please be mindful of whether or not you require even a fraction of the available requests per second.

Command: `redis-benchmark -q -c 10 -n 1000000 -r 100000 -d 64 -t get,set`

#### M1 MacBook Pro (local client and server)

`respite-server --save ""`

```
SET: 72332.73 requests per second, p50=0.119 msec
GET: 77447.34 requests per second, p50=0.111 msec
```

`redis-server --save ""`

```
SET: 121212.12 requests per second, p50=0.063 msec
GET: 131943.53 requests per second, p50=0.055 msec
```

`respite-server`

```
SET: 48206.71 requests per second, p50=0.175 msec
GET: 56773.02 requests per second, p50=0.167 msec
```

`redis-server --save "" --appendonly yes --appendfsync always`

```
SET: 110399.65 requests per second, p50=0.071 msec
GET: 125659.71 requests per second, p50=0.063 msec
```

#### Google Cloud n2d-standard-2 VM, 2 vCPU + 8GB, $60/mo

Separate client and server in the same zone on same VPC.

`respite-server --save ""`

```
```

`redis-server --save ""`

```
```

`respite-server`

```
```

`redis-server --save "" --appendonly yes --appendfsync always`

```
```

#### DigitalOcean Premium AMD droplet, 2 vCPU + 8GB, $48/mo

`respite-server --save ""`

```
```

`redis-server --save ""`

```
```

`respite-server`

```
```

`redis-server --save "" --appendonly yes --appendfsync always`

```
```

#### Hetzner CCX13 VM, 2 vCPU + 8GB, $13/mo

`respite-server --save ""`

```
```

`redis-server --save ""`

```
```

`respite-server`

```
```

`redis-server --save "" --appendonly yes --appendfsync always`

```
```
