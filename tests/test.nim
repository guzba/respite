import ready, respite, std/options, std/os

var clientThread: Thread[void]

proc clientProc() =
  var r: RedisConn

  for i in 0 ..< 10:
    try:
      r = newRedisConn("localhost", Port(9999))
      break
    except:
      sleep(500)

  doAssert r.command("PING").to(string) == "PONG"

  doAssertRaises RedisError:
    discard r.command("ECHO")
  doAssert r.command("ECHO", "abc").to(string) == "abc"

  doAssert not r.command("GET", "a").to(Option[string]).isSome
  doAssert r.command("SET", "a", "b").to(string) == "OK"
  doAssert r.command("GET", "a").to(string) == "b"
  doAssert r.command("DEL", "a", "b", "c").to(int) == 1
  discard r.command("SET", "a", "b", "EX", "10")
  doAssert r.command("TTL", "a").to(int) == 10
  discard r.command("EXPIRE", "a", "100", "GT")
  doAssert r.command("TTL", "a").to(int) == 100
  doAssert r.command("PERSIST", "a").to(int) == 1
  doAssert r.command("EXISTS", "a").to(int) == 1
  doAssert r.command("TYPE", "a").to(string) == "string"
  discard r.command("INCRBY", "n", "3")
  discard r.command("INCR", "n")
  discard r.command("DECRBY", "n", "3")
  discard r.command("DECR", "n")
  doAssert r.command("GETDEL", "n").to(int) == 0
  doAssert r.command("EXISTS", "n").to(int) == 0

  doAssert r.command("HSET", "h", "x", "y", "p", "q").to(int) == 2
  discard r.command("HSETNX", "h", "x", "0")
  discard r.command("HSETNX", "h", "a", "b")
  discard r.command("HINCRBY", "h", "z", "3")
  discard r.command("HINCRBY", "h", "z", "1")
  doAssert r.command("HGET", "h", "x").to(string) == "y"
  doAssert r.command("HGET", "h", "z").to(int) == 4
  doAssert r.command("HDEL", "h", "z", "g", "h", "i", "a").to(int) == 2
  doAssert r.command("HEXISTS", "h", "z").to(int) == 0
  doAssert r.command("HEXISTS", "h", "x").to(int) == 1
  doAssert r.command("HLEN", "h").to(int) == 2
  block:
    let tmp = r.command("HGETALL", "h").to(seq[string])
    doAssert tmp.len == 4
    if tmp[0] == "x":
      doAssert tmp == @["x", "y", "p", "q"]
    else:
      doAssert tmp == @["p", "q", "x", "y"]
  block:
    let tmp = r.command("HKEYS", "h").to(seq[string])
    doAssert tmp.len == 2
    doAssert ("x" in tmp) and ("p" in tmp)
  doAssert r.command("SADD", "s", "a", "b", "c", "a").to(int) == 3
  doAssert r.command("SCARD", "s").to(int) == 3
  doAssert r.command("SISMEMBER", "s", "z").to(int) == 0
  doAssert r.command("SISMEMBER", "s", "a").to(int) == 1
  block:
    let tmp = r.command("SMEMBERS", "s").to(seq[string])
    doAssert tmp.len == 3
    doAssert ("a" in tmp) and ("b" in tmp) and ("c" in tmp)
  doAssert r.command("SREM", "s", "z").to(int) == 0
  doAssert r.command("SREM", "s", "b", "c").to(int) == 2
  doAssert r.command("SPOP", "s").to(string) == "a"

  doAssert r.command("ZADD", "z", "-inf", "a").to(int) == 1
  doAssert r.command("ZADD", "z", "inf", "b").to(int) == 1
  doAssert r.command("ZADD", "z", "+inf", "c").to(int) == 1
  doAssert r.command("ZCARD", "z").to(int) == 3
  doAssert r.command("ZSCORE", "z", "a").to(string) == "-inf"
  doAssert r.command("ZSCORE", "z", "b").to(string) == "inf"
  doAssert r.command("ZSCORE", "z", "c").to(string) == "inf"
  doAssert r.command("ZREM", "z", "x").to(int) == 0
  doAssert r.command("ZREM", "z", "c").to(int) == 1
  doAssert not r.command("ZSCORE", "z", "c").to(Option[string]).isSome
  doAssert r.command("ZCARD", "z").to(int) == 2
  doAssert r.command("ZCOUNT", "z", "-inf", "inf").to(int) == 2
  doAssert r.command("ZREMRANGEBYSCORE", "z", "-inf", "inf").to(int) == 2
  doAssert r.command("ZCARD", "z").to(int) == 0

  block:
    discard r.command("DEL", "k")
    discard r.command("HSET", "k", "a", "b")
    doAssertRaises RedisError:
      discard r.command("GET", "k")
    discard r.command("SET", "k", "b")
    doAssert r.command("GET", "k").to(string) == "b"

  block:
    discard r.command("DEL", "k")
    discard r.command("SADD", "k", "a")
    doAssertRaises RedisError:
      discard r.command("GET", "k")
    discard r.command("SET", "k", "b")
    doAssert r.command("GET", "k").to(string) == "b"

  block:
    discard r.command("DEL", "k")
    discard r.command("ZADD", "k", "1", "a")
    doAssertRaises RedisError:
      discard r.command("GET", "k")
    discard r.command("SET", "k", "b")
    doAssert r.command("GET", "k").to(string) == "b"

  echo "Tests complete"
  stop()

createThread(clientThread, clientProc)

echo "Starting Respite server"
start("localhost", Port(9999))
