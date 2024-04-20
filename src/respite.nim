when not defined(nimdoc):
  when not defined(gcArc) and not defined(gcOrc):
    {.error: "Using --mm:arc or --mm:orc is required by Respite.".} # TODO

when not compileOption("threads"):
  {.error: "Using --threads:on is required by Respite.".} # TODO

import std/nativesockets, std/os, std/selectors, std/sets, std/options,
    std/strutils, std/parseutils, std/deques, respite/sqlite3, std/tables, std/times,
    std/decls

when defined(linux):
  import std/posix

  let SOCK_NONBLOCK
    {.importc: "SOCK_NONBLOCK", header: "<sys/socket.h>".}: cint

const
  listenBacklogLen = 128
  maxEventsPerSelectLoop = 64
  initialRecvBufLen = (4 * 1024) - 9 # 8 byte cap field + null terminator

type
  DataEntryKind = enum
    ServerSocketEntry, ClientSocketEntry

  DataEntry {.acyclic.} = ref object
    case kind: DataEntryKind:
    of ServerSocketEntry:
      discard
    of ClientSocketEntry:
      remoteAddress: string
      recvBuf: string
      bytesReceived: int
      outgoingBuffers: Deque[OutgoingBuffer]

  OutgoingBuffer {.acyclic.} = ref object
    buffer: string
    bytesSent: int

  RedisCommand = object
    raw, normalized: string
    args: seq[string]

  ArgumentValueKind = enum
    BlobValue, IntegerValue, RealValue, NullValue

  ArgumentValue = object
    case kind: ArgumentValueKind
    of BlobValue:
      b: string
    of IntegerValue:
      i: int64
    of RealValue:
      r: float64
    of NullValue:
      discard

  PreparedStatement {.acyclic.} = ref object
    stmt: SqliteStatement
    params: seq[string]
    argsHolder: Table[string, ArgumentValue]

  RedisKeyKind = enum
    StringKey, ListKey, SetKey, SortedSetKey, HashKey

  RedisKeyHash = distinct string

  RedisKey = object
    key: string
    id: int
    kind: RedisKeyKind
    expires: Option[int64]

const schema = """
  pragma journal_mode = wal;
  pragma synchronous = normal;
  pragma foreign_keys = on;
  pragma temp_store = memory;
  pragma mmap_size = 268435456;
  pragma cache_size = -64000;
  pragma busy_timeout = 60000;

  create table if not exists redis_keys (
    id integer primary key not null,
    redis_key blob not null,
    kind integer not null,
    expires integer
  );

  create unique index if not exists
  redis_keys_unique_idx on redis_keys (redis_key);

  create index if not exists
  redis_keys_expires_idx on redis_keys (expires)
  where expires is not null;

  create table if not exists redis_strings (
    redis_key_id integer primary key not null,
    value blob not null,
    foreign key (redis_key_id) references redis_keys (id)
      on update cascade
      on delete cascade
  ) without rowid;

  create table if not exists redis_hashes (
    redis_key_id integer not null,
    field blob not null,
    value blob not null,
    foreign key (redis_key_id) references redis_keys(id)
      on update cascade
      on delete cascade
  );

  create unique index if not exists
  redis_hashes_field_unique_idx on redis_hashes (redis_key_id, field);

  create table if not exists redis_sets (
    redis_key_id integer not null,
    member blob not null,
    foreign key (redis_key_id) references redis_keys (id)
      on update cascade
      on delete cascade
  );

  create unique index if not exists
  redis_sets_member_unique_idx on redis_sets (redis_key_id, member);

  create table if not exists redis_sorted_sets (
    redis_key_id integer not null,
    member blob not null,
    score real not null,
    foreign key (redis_key_id) references redis_keys (id)
      on update cascade
      on delete cascade
  );

  create unique index if not exists
  redis_sorted_sets_member_unique_idx on redis_sorted_sets (redis_key_id, member);
"""

let db = block:
  var handle: SqliteHandle
  if sqlite3_open(
    ":memory:",
    handle
  ) != SQLITE_OK:
    raise newException(CatchableError, "SQLite: " & $sqlite3_errmsg(handle))
  handle

if sqlite3_exec(
  db,
  schema.cstring,
  cast[SqliteCallback](nil),
  nil,
  nil
) != SQLITE_OK:
  raise newException(CatchableError, "SQLite: " & $sqlite3_errmsg(db))

proc newPreparedStatement(db: SqliteHandle, sql: string): PreparedStatement =
  result = PreparedStatement()

  if sqlite3_prepare_v2(
    db,
    sql.cstring,
    sql.len.int32,
    result.stmt,
    nil
  ) != SQLITE_OK:
    raise newException(CatchableError, "SQLite: " & $sqlite3_errmsg(db))

  let paramCount = sqlite3_bind_parameter_count(result.stmt)
  for i in 1 .. paramCount:
    let p = sqlite3_bind_parameter_name(result.stmt, i)
    if p == nil:
      raise newException(CatchableError, "Unexpected null bind parameter name")
    result.params.add($p)

proc reset(ps: PreparedStatement) =
  if sqlite3_reset(ps.stmt) != SQLITE_OK:
    raise newException(CatchableError, "SQLite: " & $sqlite3_errmsg(db))
  ps.argsHolder.clear()

proc bindArgs(ps: PreparedStatement, args: sink Table[string, ArgumentValue]) =
  for name in args.keys:
    if name notin ps.params:
      raise newException(CatchableError, "Unexpected parameter '" & name & "'")

  ps.argsHolder = ensureMove args

  for i, name in ps.params:
    if name in ps.argsHolder:
      let arg {.byaddr.} = ps.argsHolder[name]
      case arg.kind:
      of BlobValue:
        if sqlite3_bind_blob64(
          ps.stmt,
          (i + 1).int32,
          arg.b.cstring,
          arg.b.len,
          SQLITE_STATIC
        ) != SQLITE_OK:
          raise newException(CatchableError, "SQLite: " & $sqlite3_errmsg(db))
      of IntegerValue:
        if sqlite3_bind_int64(
          ps.stmt,
          (i + 1).int32,
          arg.i
        ) != SQLITE_OK:
          raise newException(CatchableError, "SQLite: " & $sqlite3_errmsg(db))
      of RealValue:
        if sqlite3_bind_double(
          ps.stmt,
          (i + 1).int32,
          arg.r
        ) != SQLITE_OK:
          raise newException(CatchableError, "SQLite: " & $sqlite3_errmsg(db))
      of NullValue:
        if sqlite3_bind_null(
          ps.stmt,
          (i + 1).int32
        ) != SQLITE_OK:
          raise newException(CatchableError, "SQLite: " & $sqlite3_errmsg(db))
    else:
      raise newException(
        CatchableError, 
        "No argument for param '" & name & '\''
      )

proc step(ps: PreparedStatement): bool =
  let code = sqlite3_step(ps.stmt)
  case code:
  of SQLITE_ROW:
    true
  of SQLITE_DONE:
    false
  of SQLITE_ERROR:
    raise newException(CatchableError, "SQLite: " & $sqlite3_errmsg(db))
  else:
    raise newException(CatchableError, "Unexpected SQLite result code: " & $code)

proc beginTransaction() =
  if sqlite3_exec(
    db,
    "BEGIN IMMEDIATE",
    cast[SqliteCallback](nil),
    nil,
    nil
  ) != SQLITE_OK:
    raise newException(CatchableError, "SQLite: " & $sqlite3_errmsg(db))

proc rollbackTransaction() =
  if sqlite3_exec(
    db,
    "ROLLBACK",
    cast[SqliteCallback](nil),
    nil,
    nil
  ) != SQLITE_OK:
    raise newException(CatchableError, "SQLite: " & $sqlite3_errmsg(db))

proc commitTransaction() =
  if sqlite3_exec(
    db,
    "COMMIT",
    cast[SqliteCallback](nil),
    nil,
    nil
  ) != SQLITE_OK:
    raise newException(CatchableError, "SQLite: " & $sqlite3_errmsg(db))

proc stepSqlIn(sql: string, args: seq[string]): SqliteStatement =
  var sql = sql
  sql.add " in ("
  for i in 0 ..< args.len:
    if i > 0:
      sql.add ','
    sql.add '?'
  sql.add ')'

  try:
    if sqlite3_prepare_v2(
      db,
      sql.cstring,
      sql.len.int32,
      result,
      nil
    ) != SQLITE_OK:
      raise newException(CatchableError, "SQLite: " & $sqlite3_errmsg(db))

    for i in 0 ..< args.len:
      if sqlite3_bind_blob64(
        result,
        (i + 1).int32,
        args[i].cstring,
        args[i].len,
        SQLITE_STATIC
      ) != SQLITE_OK:
        raise newException(CatchableError, "SQLite: " & $sqlite3_errmsg(db))

    discard sqlite3_step(result)
  except:
    discard sqlite3_finalize(result)
    raise

proc stepSqlIn(sql: string, id: int, args: seq[string]): SqliteStatement =
  var sql = sql
  sql.add " in ("
  for i in 0 ..< args.len:
    if i > 0:
      sql.add ','
    sql.add '?'
  sql.add ')'

  try:
    if sqlite3_prepare_v2(
      db,
      sql.cstring,
      sql.len.int32,
      result,
      nil
    ) != SQLITE_OK:
      raise newException(CatchableError, "SQLite: " & $sqlite3_errmsg(db))

    if sqlite3_bind_int64(
      result,
      1.int32,
      id
    ) != SQLITE_OK:
      raise newException(CatchableError, "SQLite: " & $sqlite3_errmsg(db))

    for i in 0 ..< args.len:
      if sqlite3_bind_blob64(
        result,
        (i + 2).int32,
        args[i].cstring,
        args[i].len,
        SQLITE_STATIC
      ) != SQLITE_OK:
        raise newException(CatchableError, "SQLite: " & $sqlite3_errmsg(db))

    discard sqlite3_step(result)
  except:
    discard sqlite3_finalize(result)
    raise

let
  deleteExpiredRedisKeys = newPreparedStatement(db, """
    delete from redis_keys
    where expires is not null and expires <= :now
  """)
  selectRedisKey = newPreparedStatement(db, """
    select id, kind, expires from redis_keys
    where redis_key = :redis_key
  """)
  insertRedisKey = newPreparedStatement(db, """
    insert into redis_keys (redis_key, kind, expires)
    values (:redis_key, :kind, :expires);
  """)
  updateRedisKeyExpires = newPreparedStatement(db, """
    update redis_keys set expires = :expires
    where redis_key = :redis_key
  """)
  deleteRedisKey = newPreparedStatement(db, """
    delete from redis_keys
    where redis_key = :redis_key
  """)
  selectRedisString = newPreparedStatement(db, """
    select value from redis_strings
    where redis_key_id = :redis_key_id
  """)
  upsertRedisString = newPreparedStatement(db, """
    insert into redis_strings (redis_key_id, value)
    values (:redis_key_id, :value)
    on conflict do update set value = excluded.value
  """)
  countRedisHashField = newPreparedStatement(db, """
    select count(*) from redis_hashes
    where redis_key_id = :redis_key_id and field = :field
  """)
  countRedisHashFields = newPreparedStatement(db, """
    select count(*) from redis_hashes
    where redis_key_id = :redis_key_id
  """)
  upsertRedisHashField = newPreparedStatement(db, """
    insert into redis_hashes (redis_key_id, field, value)
    values (:redis_key_id, :field, :value)
    on conflict do update set value = excluded.value
  """)
  selectRedisHashField = newPreparedStatement(db, """
    select value from redis_hashes
    where redis_key_id = :redis_key_id and field = :field
  """)
  deleteRedisHashField = newPreparedStatement(db, """
    delete from redis_hashes
    where redis_key_id = :redis_key_id and field = :field
  """)
  selectRedisHashFieldPairs = newPreparedStatement(db, """
    select field, value from redis_hashes
    where redis_key_id = :redis_key_id
  """)
  selectRedisHashFields = newPreparedStatement(db, """
    select field from redis_hashes
    where redis_key_id = :redis_key_id
  """)
  countRedisSetMember= newPreparedStatement(db, """
    select count(*) from redis_sets
    where redis_key_id = :redis_key_id and member = :member
  """)
  countRedisSetMembers = newPreparedStatement(db, """
    select count(*) from redis_sets
    where redis_key_id = :redis_key_id
  """)
  upsertRedisSetMember = newPreparedStatement(db, """
    insert into redis_sets (redis_key_id, member)
    values (:redis_key_id, :member)
    on conflict do nothing
  """)
  selectRedisSetMembers = newPreparedStatement(db, """
    select member from redis_sets
    where redis_key_id = :redis_key_id
  """)
  deleteRedisSetMember = newPreparedStatement(db, """
    delete from redis_sets
    where redis_key_id = :redis_key_id and member = :member
  """)
  sampleRedisSetMember = newPreparedStatement(db, """
    select member from redis_sets
    where redis_key_id = :redis_key_id limit 1
  """)
  countRedisSortedSetMember= newPreparedStatement(db, """
    select count(*) from redis_sorted_sets
    where redis_key_id = :redis_key_id and member = :member
  """)
  upsertRedisSortedSetMember = newPreparedStatement(db, """
    insert into redis_sorted_sets (redis_key_id, member, score)
    values (:redis_key_id, :member, :score)
    on conflict do update set score = excluded.score
  """)
  countRedisSortedSetMembers = newPreparedStatement(db, """
    select count(*) from redis_sorted_sets
    where redis_key_id = :redis_key_id
  """)
  deleteRedisSortedSetMember = newPreparedStatement(db, """
    delete from redis_sorted_sets
    where redis_key_id = :redis_key_id and member = :member
  """)
  selectRedisSortedSetMemberScore = newPreparedStatement(db, """
    select score from redis_sorted_sets
    where redis_key_id = :redis_key_id and member = :member
  """)
  deleteRedisSortedSetMemberInRange = newPreparedStatement(db, """
    delete from redis_sorted_sets
    where redis_key_id = :redis_key_id and score >= :min and score <= :max
  """)
  countRedisSortedSetMembersInRange = newPreparedStatement(db, """
    select count(*) from redis_sorted_sets
    where redis_key_id = :redis_key_id and score >= :min and score <= :max
  """)

proc getRedisKey(key: string): Option[RedisKey] =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key"] = ArgumentValue(kind: BlobValue, b: key)
    selectRedisKey.bindArgs(ensureMove args)
    if selectRedisKey.step():
      var tmp: RedisKey
      tmp.key = key
      tmp.id = sqlite3_column_int64(selectRedisKey.stmt, 0)
      tmp.kind = RedisKeyKind(sqlite3_column_int64(selectRedisKey.stmt, 1))
      if sqlite3_column_type(selectRedisKey.stmt, 2) == SQLITE_NULL:
        discard
      else:
        tmp.expires = some(sqlite3_column_int64(selectRedisKey.stmt, 2))
      result = some(ensureMove tmp)
  finally:
    selectRedisKey.reset()

proc setRedisKeyExpires(key: string, expires: int) =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key"] = ArgumentValue(kind: BlobValue, b: key)
    if expires > 0:
      args[":expires"] = ArgumentValue(kind: IntegerValue, i: expires)
    else:
      args[":expires"] = ArgumentValue(kind: NullValue)
    updateRedisKeyExpires.bindArgs(ensureMove args)
    discard updateRedisKeyExpires.step()
  finally:
    updateRedisKeyExpires.reset()

proc deleteRedisKey2(key: string) =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key"] = ArgumentValue(kind: BlobValue, b: key)
    deleteRedisKey.bindArgs(ensureMove args)
    discard deleteRedisKey.step()
  finally:
    deleteRedisKey.reset()

proc getRedisString(id: int): string =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key_id"] = ArgumentValue(kind: IntegerValue, i: id)
    selectRedisString.bindArgs(ensureMove args)
    if selectRedisString.step():
      let len = sqlite3_column_bytes(selectRedisString.stmt, 0)
      if len > 0:
        result.setLen(len)
        copyMem(
          result.cstring,
          sqlite3_column_blob(selectRedisString.stmt, 0),
          len
        )
    else:
      raise newException(CatchableError, "No value for Redis string")
  finally:
    selectRedisString.reset()

proc insertNewRedisKey(
  key: string, 
  kind: RedisKeyKind, 
  expires: int64 = 0
): RedisKey =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key"] = ArgumentValue(kind: BlobValue, b: key)
    args[":kind"] = ArgumentValue(kind: IntegerValue, i: ord(kind))
    if expires > 0:
      result.expires = some(expires)
      args[":expires"] = ArgumentValue(kind: IntegerValue, i: expires)
    else:
      args[":expires"] = ArgumentValue(kind: NullValue)
    insertRedisKey.bindArgs(ensureMove args)
    discard insertRedisKey.step()
  finally:
    insertRedisKey.reset()

  result.id = sqlite3_last_insert_rowid(db)
  result.key = key
  result.kind = kind

proc upsertRedisString2(id: int, value: string) =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key_id"] = ArgumentValue(kind: IntegerValue, i: id)
    args[":value"] = ArgumentValue(kind: BlobValue, b: value)
    upsertRedisString.bindArgs(ensureMove args)
    discard upsertRedisString.step()
  finally:
    upsertRedisString.reset()

proc insertNewRedisString(
  key: string, 
  value: string, 
  expires: int = 0
): RedisKey =
  let newRedisKey = insertNewRedisKey(key, StringKey, expires)
  upsertRedisString2(newRedisKey.id, value)
  return newRedisKey

proc redisHashFieldExists(id: int, field: string): bool =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key_id"] = ArgumentValue(kind: IntegerValue, i: id)
    args[":field"] = ArgumentValue(kind: BlobValue, b: field)
    countRedisHashField.bindArgs(ensureMove args)
    discard countRedisHashField.step()
    result = sqlite3_column_int64(countRedisHashField.stmt, 0) > 0
  finally:
    countRedisHashField.reset()

proc upsertRedisHashField2(id: int, field, value: string) =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key_id"] = ArgumentValue(kind: IntegerValue, i: id)
    args[":field"] = ArgumentValue(kind: BlobValue, b: field)
    args[":value"] = ArgumentValue(kind: BlobValue, b: value)
    upsertRedisHashField.bindArgs(ensureMove args)
    discard upsertRedisHashField.step()
  finally:
    upsertRedisHashField.reset()

proc getRedisHashField(id: int, field: string): Option[string] =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key_id"] = ArgumentValue(kind: IntegerValue, i: id)
    args[":field"] = ArgumentValue(kind: BlobValue, b: field)
    selectRedisHashField.bindArgs(ensureMove args)
    if selectRedisHashField.step():
      var tmp: string
      let len = sqlite3_column_bytes(selectRedisHashField.stmt, 0)
      if len > 0:
        tmp.setLen(len)
        copyMem(
          tmp.cstring,
          sqlite3_column_blob(selectRedisHashField.stmt, 0),
          len
        )
        result = some(ensureMove tmp)
  finally:
    selectRedisHashField.reset()

proc insertNewRedisHash(
  key: string, 
  field, value: string
): RedisKey =
  let newRedisKey = insertNewRedisKey(key, HashKey)
  upsertRedisHashField2(newRedisKey.id, field, value)
  return newRedisKey

proc deleteRedisHashField2(id: int, field: string) =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key_id"] = ArgumentValue(kind: IntegerValue, i: id)
    args[":field"] = ArgumentValue(kind: BlobValue, b: field)
    deleteRedisHashField.bindArgs(ensureMove args)
    discard deleteRedisHashField.step()
  finally:
    deleteRedisHashField.reset()

proc countRedisHashFields2(id: int): int =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key_id"] = ArgumentValue(kind: IntegerValue, i: id)
    countRedisHashFields.bindArgs(ensureMove args)
    discard countRedisHashFields.step()
    result = sqlite3_column_int64(countRedisHashFields.stmt, 0)
  finally:
    countRedisHashFields.reset()

proc getAllRedisHashFieldPairs(id: int): seq[(string, string)] =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key_id"] = ArgumentValue(kind: IntegerValue, i: id)
    selectRedisHashFieldPairs.bindArgs(ensureMove args)
    while selectRedisHashFieldPairs.step():
      var field, value: string
      block:
        let len = sqlite3_column_bytes(selectRedisHashFieldPairs.stmt, 0)
        if len > 0:
          field.setLen(len)
          copyMem(
            field.cstring,
            sqlite3_column_blob(selectRedisHashFieldPairs.stmt, 0),
            len
          )
      block:
        let len = sqlite3_column_bytes(selectRedisHashFieldPairs.stmt, 1)
        if len > 0:
          value.setLen(len)
          copyMem(
            value.cstring,
            sqlite3_column_blob(selectRedisHashFieldPairs.stmt, 1),
            len
          )
      result.add((ensureMove field, ensureMove value))
  finally:
    selectRedisHashFieldPairs.reset()

proc getAllRedisHashFields(id: int): seq[string] =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key_id"] = ArgumentValue(kind: IntegerValue, i: id)
    selectRedisHashFields.bindArgs(ensureMove args)
    while selectRedisHashFields.step():
      var field: string
      let len = sqlite3_column_bytes(selectRedisHashFields.stmt, 0)
      if len > 0:
        field.setLen(len)
        copyMem(
          field.cstring,
          sqlite3_column_blob(selectRedisHashFields.stmt, 0),
          len
        )
      result.add(ensureMove field)
  finally:
    selectRedisHashFields.reset()

proc redisSetMemberExists(id: int, member: string): bool =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key_id"] = ArgumentValue(kind: IntegerValue, i: id)
    args[":member"] = ArgumentValue(kind: BlobValue, b: member)
    countRedisSetMember.bindArgs(ensureMove args)
    discard countRedisSetMember.step()
    result = sqlite3_column_int64(countRedisSetMember.stmt, 0) > 0
  finally:
    countRedisSetMember.reset()

proc upsertRedisSetMember2(id: int, member: string) =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key_id"] = ArgumentValue(kind: IntegerValue, i: id)
    args[":member"] = ArgumentValue(kind: BlobValue, b: member)
    upsertRedisSetMember.bindArgs(ensureMove args)
    discard upsertRedisSetMember.step()
  finally:
    upsertRedisSetMember.reset()

proc countRedisSetMembers2(id: int): int =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key_id"] = ArgumentValue(kind: IntegerValue, i: id)
    countRedisSetMembers.bindArgs(ensureMove args)
    discard countRedisSetMembers.step()
    result = sqlite3_column_int64(countRedisSetMembers.stmt, 0)
  finally:
    countRedisSetMembers.reset()

proc getAllRedisSetMembers(id: int): seq[string] =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key_id"] = ArgumentValue(kind: IntegerValue, i: id)
    selectRedisSetMembers.bindArgs(ensureMove args)
    while selectRedisSetMembers.step():
      var member: string
      let len = sqlite3_column_bytes(selectRedisSetMembers.stmt, 0)
      if len > 0:
        member.setLen(len)
        copyMem(
          member.cstring,
          sqlite3_column_blob(selectRedisSetMembers.stmt, 0),
          len
        )
      result.add(ensureMove member)
  finally:
    selectRedisSetMembers.reset()

proc deleteRedisSetMember2(id: int, member: string) =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key_id"] = ArgumentValue(kind: IntegerValue, i: id)
    args[":member"] = ArgumentValue(kind: BlobValue, b: member)
    deleteRedisSetMember.bindArgs(ensureMove args)
    discard deleteRedisSetMember.step()
  finally:
    deleteRedisSetMember.reset()

proc sampleRedisSetMember2(id: int): string =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key_id"] = ArgumentValue(kind: IntegerValue, i: id)
    sampleRedisSetMember.bindArgs(ensureMove args)
    if sampleRedisSetMember.step():
      let len = sqlite3_column_bytes(sampleRedisSetMember.stmt, 0)
      if len > 0:
        result.setLen(len)
        copyMem(
          result.cstring,
          sqlite3_column_blob(sampleRedisSetMember.stmt, 0),
          len
        )
    else:
      raise newException(CatchableError, "No members in Redis set")
  finally:
    sampleRedisSetMember.reset()

proc redisSortedSetMemberExists(id: int, member: string): bool =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key_id"] = ArgumentValue(kind: IntegerValue, i: id)
    args[":member"] = ArgumentValue(kind: BlobValue, b: member)
    countRedisSortedSetMember.bindArgs(ensureMove args)
    discard countRedisSortedSetMember.step()
    result = sqlite3_column_int64(countRedisSortedSetMember.stmt, 0) > 0
  finally:
    countRedisSortedSetMember.reset()

proc upsertRedisSortedSetMember2(id: int, member: string, score: float64) =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key_id"] = ArgumentValue(kind: IntegerValue, i: id)
    args[":member"] = ArgumentValue(kind: BlobValue, b: member)
    args[":score"] = ArgumentValue(kind: RealValue, r: score)
    upsertRedisSortedSetMember.bindArgs(ensureMove args)
    discard upsertRedisSortedSetMember.step()
  finally:
    upsertRedisSortedSetMember.reset()

proc countRedisSortedSetMembers2(id: int): int =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key_id"] = ArgumentValue(kind: IntegerValue, i: id)
    countRedisSortedSetMembers.bindArgs(ensureMove args)
    discard countRedisSortedSetMembers.step()
    result = sqlite3_column_int64(countRedisSortedSetMembers.stmt, 0)
  finally:
    countRedisSortedSetMembers.reset()

proc deleteRedisSortedSetMember2(id: int, member: string) =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key_id"] = ArgumentValue(kind: IntegerValue, i: id)
    args[":member"] = ArgumentValue(kind: BlobValue, b: member)
    deleteRedisSortedSetMember.bindArgs(ensureMove args)
    discard deleteRedisSortedSetMember.step()
  finally:
    deleteRedisSortedSetMember.reset()

proc getRedisSortedSetMemberScore(id: int, member: string): Option[float64] =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key_id"] = ArgumentValue(kind: IntegerValue, i: id)
    args[":member"] = ArgumentValue(kind: BlobValue, b: member)
    selectRedisSortedSetMemberScore.bindArgs(ensureMove args)
    if selectRedisSortedSetMemberScore.step():
      result = some(sqlite3_column_double(selectRedisSortedSetMemberScore.stmt, 0))
  finally:
    selectRedisSortedSetMemberScore.reset()

proc deleteRedisSortedSetMembersInRange(id: int, min, max: float64) =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key_id"] = ArgumentValue(kind: IntegerValue, i: id)
    args[":min"] = ArgumentValue(kind: RealValue, r: min)
    args[":max"] = ArgumentValue(kind: RealValue, r: max)
    deleteRedisSortedSetMemberInRange.bindArgs(ensureMove args)
    discard deleteRedisSortedSetMemberInRange.step()
  finally:
    deleteRedisSortedSetMemberInRange.reset()

proc countRedisSortedSetMembersInRange2(id: int, min, max: float64): int =
  try:
    var args: Table[string, ArgumentValue]
    args[":redis_key_id"] = ArgumentValue(kind: IntegerValue, i: id)
    args[":min"] = ArgumentValue(kind: RealValue, r: min)
    args[":max"] = ArgumentValue(kind: RealValue, r: max)
    countRedisSortedSetMembersInRange.bindArgs(ensureMove args)
    discard countRedisSortedSetMembersInRange.step()
    result = sqlite3_column_int64(countRedisSortedSetMembersInRange.stmt, 0)
  finally:
    countRedisSortedSetMembersInRange.reset()

proc simpleStringReply(msg: string): string =
  '+' & msg & "\r\n"

proc simpleErrorReply(msg: string): string =
  '-' & msg & "\r\n"

proc integerReply(n: int): string =
  ':' & $n & "\r\n"

proc bulkStringReply(msg: string): string =
  '$' & $msg.len & "\r\n" & msg & "\r\n"

proc bulkStringArrayReply(msgs: seq[string]): string =
  result = '*' & $msgs.len & "\r\n"
  for msg in msgs:
    result.add bulkStringReply(msg)

proc wrongNumberOfArgsReply(cmd: string): string =
  simpleErrorReply("ERR wrong number of arguments for '" & cmd & "' command")

proc invalidExpireTimeReply(cmd: string): string =
  simpleErrorReply("ERR invalid expire time in '" & cmd & "' command")

const
  pongReply = simpleStringReply("PONG")
  okReply = simpleStringReply("OK")
  nilReply = "$-1\r\n"
  emptyBulkStringArrayReply = bulkStringArrayReply(@[])
  syntaxErrorReply = simpleErrorReply("ERR syntax error")
  integerErrorReply = simpleErrorReply("ERR value is not an integer or out of range")
  floatErrorReply = simpleErrorReply("ERR value is not a valid float")
  wrongTypeErrorReply = simpleErrorReply("WRONGTYPE Operation against a key holding the wrong kind of value")

proc echoCommand(cmd: RedisCommand): string =
  if cmd.args.len == 1:
    bulkStringReply(cmd.args[0])
  else:
    wrongNumberOfArgsReply(cmd.raw)

proc pingCommand(cmd: RedisCommand): string =
  if cmd.args.len == 0:
    pongReply
  elif cmd.args.len == 1:
    bulkStringReply(cmd.args[0])
  else:
    wrongNumberOfArgsReply(cmd.raw)

proc getCommand(cmd: RedisCommand): string =
  if cmd.args.len != 1:
    return wrongNumberOfArgsReply(cmd.raw)

  let redisKey = getRedisKey(cmd.args[0])
  if redisKey.isSome and redisKey.unsafeGet.kind != StringKey:
    return wrongTypeErrorReply

  if redisKey.isSome:
    bulkStringReply(getRedisString(redisKey.unsafeGet.id))
  else:
    nilReply

proc setCommand(cmd: RedisCommand): string =
  if cmd.args.len < 2:
    return wrongNumberOfArgsReply(cmd.raw)

  var
    i = 2
    nx, xx, get, keepTtl: bool
    ex, exat: Option[int]
  while i < cmd.args.len:
    var normalizedArg = cmd.args[i]
    for c in normalizedArg.mitems:
      c = toUpperAscii(c)
    case normalizedArg:
    of "NX":
      if xx:
        return syntaxErrorReply
      nx = true
      inc i
    of "XX":
      if nx:
        return syntaxErrorReply
      xx = true
      inc i
    of "GET":
      get = true
      inc i
    of "EX":
      if keepTtl or exat.isSome:
        return syntaxErrorReply
      if i + 1 < cmd.args.len:
        try:
          ex = some(parseInt(cmd.args[i + 1]))
          i += 2
        except:
          return integerErrorReply
      else:
        inc i
    of "EXAT":
      if keepTtl or ex.isSome:
        return syntaxErrorReply
      if i + 1 < cmd.args.len:
        try:
          exat = some(parseInt(cmd.args[i + 1]))
          i += 2
        except:
          return integerErrorReply
      else:
        inc i
    of "KEEPTTL":
      if ex.isSome or exat.isSome:
        return syntaxErrorReply
      keepTtl = true
      inc i
    else:
      return syntaxErrorReply

  if (ex.isSome and ex.unsafeGet <= 0) or (exat.isSome and exat.unsafeGet <= 0):
    return invalidExpireTimeReply(cmd.raw)

  let existingKey = getRedisKey(cmd.args[0])
  if (nx and existingKey.isSome) or (xx and not existingKey.isSome):
    # Aborted
    if get and existingKey.isSome:
      if existingKey.unsafeGet.kind == StringKey:
        return bulkStringReply(getRedisString(existingKey.get.id))
      else:
        return wrongTypeErrorReply
    else:
      return nilReply

  var expires: int
  if ex.isSome:
    expires = epochTime().int + ex.unsafeGet
  elif exat.isSome:
    expires = exat.unsafeGet

  if existingKey.isSome:
    if existingKey.unsafeGet.kind == StringKey:
      var existingRedisStringForGet: string
      if get:
        existingRedisStringForGet = getRedisString(existingKey.get.id)

      if keepTtl:
        discard
      elif existingKey.unsafeGet.expires.isSome or expires > 0:
        setRedisKeyExpires(cmd.args[0], expires)

      upsertRedisString2(existingKey.get.id, cmd.args[1])

      if get:
        return bulkStringReply(existingRedisStringForGet)
      else:
        return okReply
    else:
      if get:
        return wrongTypeErrorReply
      else:
        if keepTtl and existingKey.unsafeGet.expires.isSome:
          expires = existingKey.unsafeGet.expires.unsafeGet

        deleteRedisKey2(cmd.args[0])
        discard insertNewRedisString(cmd.args[0], cmd.args[1], expires)

        return okReply
  else:
    discard insertNewRedisString(cmd.args[0], cmd.args[1], expires)
    if get:
      return nilReply
    else:
      return okReply

proc delCommand(cmd: RedisCommand): string =
  if cmd.args.len > 1:
    let stmt = stepSqlIn("delete from redis_keys where redis_key", cmd.args)
    discard sqlite3_finalize(stmt)
    integerReply(sqlite3_changes64(db))
  elif cmd.args.len == 1:
    deleteRedisKey2(cmd.args[0])
    integerReply(sqlite3_changes64(db))
  else:
    wrongNumberOfArgsReply(cmd.raw)

proc expireCommand(cmd: RedisCommand): string =
  if cmd.args.len < 2:
    return wrongNumberOfArgsReply(cmd.raw)

  var nx, xx, lt, gt: bool
  for i in 2 ..< cmd.args.len:
    var normalizedArg = cmd.args[i]
    for c in normalizedArg.mitems:
      c = toUpperAscii(c)
    case normalizedArg:
    of "NX":
      if xx or lt or gt:
        return syntaxErrorReply
      nx = true
    of "XX":
      if nx:
        return syntaxErrorReply
      xx = true
    of "LT":
      if gt or nx:
        return syntaxErrorReply
      lt = true
    of "GT":
      if lt or nx:
        return syntaxErrorReply
      gt = true
    else:
      return syntaxErrorReply

  let now = epochTime().int
  var
    expires: int
    integerError: bool
  try:
    if cmd.normalized == "EXPIREAT":
      expires = parseInt(cmd.args[1])
    else: # EXPIRE
      expires = now + parseInt(cmd.args[1])
  except:
    return integerErrorReply

  let existingKey = getRedisKey(cmd.args[0])
  if existingKey.isSome:
    if existingKey.unsafeGet.expires.isSome:
      if nx or
        (lt and expires >= existingKey.unsafeGet.expires.unsafeGet) or
        (gt and expires <= existingKey.unsafeGet.expires.unsafeGet):
        integerReply(0)
      else:
        setRedisKeyExpires(cmd.args[0], expires)
        integerReply(1)
    else:
      if xx or gt:
        integerReply(0)
      else:
        setRedisKeyExpires(cmd.args[0], expires)
        integerReply(1)
  else:
    integerReply(0)

proc ttlCommand(cmd: RedisCommand): string =
  if cmd.args.len != 1:
    return wrongNumberOfArgsReply(cmd.raw)

  let redisKey = getRedisKey(cmd.args[0])
  if redisKey.isSome:
    if redisKey.unsafeGet.expires.isSome:
      var expires = redisKey.unsafeGet.expires.unsafeGet
      if cmd.normalized == "EXPIRETIME":
        discard
      else: # TTL
        expires -= epochTime().int
      integerReply(max(expires, 1))
    else:
      integerReply(-1)
  else:
    integerReply(-2)

proc existsCommand(cmd: RedisCommand): string =
  if cmd.args.len > 1:
    let stmt = stepSqlIn("select count(*) from redis_keys where redis_key", cmd.args)
    try:
      integerReply(sqlite3_column_int64(stmt, 0))
    finally:
      discard sqlite3_finalize(stmt)
  elif cmd.args.len == 1:
    if getRedisKey(cmd.args[0]).isSome:
      integerReply(1)
    else:
      integerReply(0)
  else:
    wrongNumberOfArgsReply(cmd.raw)

proc persistCommand(cmd: RedisCommand): string =
  if cmd.args.len != 1:
    return wrongNumberOfArgsReply(cmd.raw)

  let redisKey = getRedisKey(cmd.args[0])
  if redisKey.isSome and redisKey.unsafeGet.expires.isSome:
      setRedisKeyExpires(cmd.args[0], -1)
      integerReply(1)
  else:
    integerReply(0)

proc typeCommand(cmd: RedisCommand): string =
  if cmd.args.len != 1:
    return wrongNumberOfArgsReply(cmd.raw)

  let redisKey = getRedisKey(cmd.args[0])
  if redisKey.isSome:
    case redisKey.unsafeGet.kind:
    of StringKey:
      simpleStringReply("string")
    of ListKey:
      simpleStringReply("list")
    of SetKey:
      simpleStringReply("set")
    of SortedSetKey:
      simpleStringReply("zset")
    of HashKey:
      simpleStringReply("hash")
  else:
    simpleStringReply("none")

proc incrbyCommand(key: string, increment: int): string =
  let redisKey = getRedisKey(key)
  if redisKey.isSome:
    if redisKey.unsafeGet.kind == StringKey:
      let value = getRedisString(redisKey.unsafeGet.id)
      var n: int
      try:
        n = parseInt(value)
      except:
        return integerErrorReply
      n += increment
      upsertRedisString2(redisKey.unsafeGet.id, $n)
      integerReply(n)
    else:
      wrongTypeErrorReply
  else:
    discard insertNewRedisString(key, $increment)
    integerReply(increment)

proc getdelCommand(cmd: RedisCommand): string =
  if cmd.args.len != 1:
    return wrongNumberOfArgsReply(cmd.raw)

  let redisKey = getRedisKey(cmd.args[0])
  if redisKey.isSome and redisKey.unsafeGet.kind == StringKey:
    let value = getRedisString(redisKey.unsafeGet.id)
    deleteRedisKey2(cmd.args[0])
    bulkStringReply(value)
  else:
    nilReply

proc hsetCommand(cmd: RedisCommand): string =
  if cmd.args.len < 3 or cmd.args.len mod 2 == 0:
    return wrongNumberOfArgsReply(cmd.raw)

  var redisKey = getRedisKey(cmd.args[0])
  if redisKey.isSome and redisKey.unsafeGet.kind != HashKey:
    return wrongTypeErrorReply

  var inserted: int

  if not redisKey.isSome:
    redisKey = some(insertNewRedisKey(cmd.args[0], HashKey))

  var i = 1
  while i < cmd.args.len:
    if not redisHashFieldExists(redisKey.unsafeGet.id, cmd.args[i]):
      inc inserted
    upsertRedisHashField2(redisKey.unsafeGet.id, cmd.args[i], cmd.args[i + 1])
    i += 2

  integerReply(inserted)

proc hsetnxCommand(cmd: RedisCommand): string =
  if cmd.args.len != 3:
    return wrongNumberOfArgsReply(cmd.raw)

  var redisKey = getRedisKey(cmd.args[0])
  if redisKey.isSome and redisKey.unsafeGet.kind != HashKey:
    return wrongTypeErrorReply

  var inserted: int
  if not redisKey.isSome:
    redisKey = some(insertNewRedisKey(cmd.args[0], HashKey))
    upsertRedisHashField2(redisKey.unsafeGet.id, cmd.args[1], cmd.args[2])
    inserted = 1
  elif not redisHashFieldExists(redisKey.unsafeGet.id, cmd.args[1]):
    upsertRedisHashField2(redisKey.unsafeGet.id, cmd.args[1], cmd.args[2])
    inserted = 1

  integerReply(inserted)

proc hgetCommand(cmd: RedisCommand): string =
  if cmd.args.len != 2:
    return wrongNumberOfArgsReply(cmd.raw)

  let redisKey = getRedisKey(cmd.args[0])
  if redisKey.isSome:
    if redisKey.unsafeGet.kind == HashKey:
      let value = getRedisHashField(redisKey.unsafeGet.id, cmd.args[1])
      if value.isSome:
        bulkStringReply(value.unsafeGet)
      else:
        nilReply
    else:
      wrongTypeErrorReply
  else:
    nilReply

proc hincrbyCommand(cmd: RedisCommand): string =
  if cmd.args.len != 3:
    return wrongNumberOfArgsReply(cmd.raw)

  var increment: int
  try:
    increment = parseInt(cmd.args[2])
  except:
    return integerErrorReply

  let redisKey = getRedisKey(cmd.args[0])
  if redisKey.isSome:
    if redisKey.unsafeGet.kind == HashKey:
      let value = getRedisHashField(redisKey.unsafeGet.id, cmd.args[1])
      var n: int
      if value.isSome:
        try:
          n = parseInt(value.unsafeGet)
        except:
          return integerErrorReply
      n += increment
      upsertRedisHashField2(redisKey.unsafeGet.id, cmd.args[1], $n)
      integerReply(n)
    else:
      wrongTypeErrorReply
  else:
    discard insertNewRedisHash(cmd.args[0], cmd.args[1], $increment)
    integerReply(increment)

proc hdelCommand(cmd: RedisCommand): string =
  if cmd.args.len < 2:
    return wrongNumberOfArgsReply(cmd.raw)

  let redisKey = getRedisKey(cmd.args[0])
  if not redisKey.isSome:
    return integerReply(0)

  if redisKey.unsafeGet.kind != HashKey:
    return wrongTypeErrorReply

  var deleted: int
  if cmd.args.len == 2:
    deleteRedisHashField2(redisKey.unsafeGet.id, cmd.args[1])
  else:
    var fields: seq[string]
    for i in 1 ..< cmd.args.len:
      fields.add(cmd.args[i])
    let stmt = stepSqlIn(
      "delete from redis_hashes where redis_key_id = ? and field",
      redisKey.unsafeGet.id, 
      fields
    )
    discard sqlite3_finalize(stmt)

  deleted = sqlite3_changes64(db)

  if countRedisHashFields2(redisKey.unsafeGet.id) == 0:
    deleteRedisKey2(cmd.args[0])

  integerReply(deleted)

proc hexistsCommand(cmd: RedisCommand): string =
  if cmd.args.len != 2:
    return wrongNumberOfArgsReply(cmd.raw)

  let redisKey = getRedisKey(cmd.args[0])
  if not redisKey.isSome:
    return integerReply(0)

  if redisKey.unsafeGet.kind != HashKey:
    return wrongTypeErrorReply

  if redisHashFieldExists(redisKey.unsafeGet.id, cmd.args[1]):
    integerReply(1)
  else:
    integerReply(0)

proc hgetallCommand(cmd: RedisCommand): string =
  if cmd.args.len != 1:
    return wrongNumberOfArgsReply(cmd.raw)

  let redisKey = getRedisKey(cmd.args[0])
  if not redisKey.isSome:
    return emptyBulkStringArrayReply

  if redisKey.unsafeGet.kind != HashKey:
    return wrongTypeErrorReply

  var msgs: seq[string]
  block:
    var pairs = getAllRedisHashFieldPairs(redisKey.unsafeGet.id)
    msgs.setLen(pairs.len * 2)
    var i = msgs.len - 2
    while i >= 0:
      var tmp = pairs.pop()
      msgs[i] = ensureMove tmp[0]
      msgs[i + 1] = ensureMove tmp[1]
      i -= 2
  bulkStringArrayReply(msgs)

proc hlenCommand(cmd: RedisCommand): string =
  if cmd.args.len != 1:
    return wrongNumberOfArgsReply(cmd.raw)

  let redisKey = getRedisKey(cmd.args[0])
  if not redisKey.isSome:
    return integerReply(0)

  if redisKey.unsafeGet.kind == HashKey:
    integerReply(countRedisHashFields2(redisKey.unsafeGet.id))
  else:
    wrongTypeErrorReply

proc hkeysCommand(cmd: RedisCommand): string =
  if cmd.args.len != 1:
    return wrongNumberOfArgsReply(cmd.raw)

  let redisKey = getRedisKey(cmd.args[0])
  if not redisKey.isSome:
    return emptyBulkStringArrayReply

  if redisKey.unsafeGet.kind == HashKey:
    bulkStringArrayReply(getAllRedisHashFields(redisKey.unsafeGet.id))
  else:
    wrongTypeErrorReply

proc saddCommand(cmd: RedisCommand): string =
  if cmd.args.len < 2:
    return wrongNumberOfArgsReply(cmd.raw)

  var redisKey = getRedisKey(cmd.args[0])
  if redisKey.isSome and redisKey.unsafeGet.kind != SetKey:
    return wrongTypeErrorReply
  
  if not redisKey.isSome:
    redisKey = some(insertNewRedisKey(cmd.args[0], SetKey))

  var inserted: int
  for i in 1 ..< cmd.args.len:
    if not redisSetMemberExists(redisKey.unsafeGet.id, cmd.args[i]):
      inc inserted
    upsertRedisSetMember2(redisKey.unsafeGet.id, cmd.args[i])

  integerReply(inserted)

proc scardCommand(cmd: RedisCommand): string =
  if cmd.args.len != 1:
    return wrongNumberOfArgsReply(cmd.raw)

  let redisKey = getRedisKey(cmd.args[0])
  if not redisKey.isSome:
    return integerReply(0)

  if redisKey.unsafeGet.kind == SetKey:
    integerReply(countRedisSetMembers2(redisKey.unsafeGet.id))
  else:
    wrongTypeErrorReply

proc sismemberCommand(cmd: RedisCommand): string =
  if cmd.args.len != 2:
    return wrongNumberOfArgsReply(cmd.raw)

  let redisKey = getRedisKey(cmd.args[0])
  if not redisKey.isSome:
    return integerReply(0)

  if redisKey.unsafeGet.kind != SetKey:
    return wrongTypeErrorReply

  if redisSetMemberExists(redisKey.unsafeGet.id, cmd.args[1]):
    integerReply(1)
  else:
    integerReply(0)

proc smembersCommand(cmd: RedisCommand): string =
  if cmd.args.len != 1:
    return wrongNumberOfArgsReply(cmd.raw)

  let redisKey = getRedisKey(cmd.args[0])
  if not redisKey.isSome:
    return emptyBulkStringArrayReply

  if redisKey.unsafeGet.kind == SetKey:
    bulkStringArrayReply(getAllRedisSetMembers(redisKey.unsafeGet.id))
  else:
    wrongTypeErrorReply

proc sremCommand(cmd: RedisCommand): string =
  if cmd.args.len < 2:
    return wrongNumberOfArgsReply(cmd.raw)

  let redisKey = getRedisKey(cmd.args[0])
  if not redisKey.isSome:
    return integerReply(0)

  if redisKey.unsafeGet.kind != SetKey:
    return wrongTypeErrorReply

  if cmd.args.len == 2:
    deleteRedisSetMember2(redisKey.unsafeGet.id, cmd.args[1])
  else:
    var members: seq[string]
    for i in 1 ..< cmd.args.len:
      members.add(cmd.args[i])
    let stmt = stepSqlIn(
      "delete from redis_sets where redis_key_id = ? and member",
      redisKey.unsafeGet.id, 
      members
    )
    discard sqlite3_finalize(stmt)

  let deleted = sqlite3_changes64(db)

  if countRedisSetMembers2(redisKey.unsafeGet.id) == 0:
    deleteRedisKey2(cmd.args[0])

  integerReply(deleted)

proc spopCommand(cmd: RedisCommand): string =
  if cmd.args.len != 1:
    return wrongNumberOfArgsReply(cmd.raw)

  let redisKey = getRedisKey(cmd.args[0])
  if not redisKey.isSome:
    return nilReply

  if redisKey.unsafeGet.kind != SetKey:
    return wrongTypeErrorReply

  let member = sampleRedisSetMember2(redisKey.unsafeGet.id)

  deleteRedisSetMember2(redisKey.unsafeGet.id, member)

  if countRedisSetMembers2(redisKey.unsafeGet.id) == 0:
    deleteRedisKey2(cmd.args[0])

  bulkStringReply(member)

proc zaddCommand(cmd: RedisCommand): string =
  if cmd.args.len != 3:
    return wrongNumberOfArgsReply(cmd.raw)

  var redisKey = getRedisKey(cmd.args[0])
  if redisKey.isSome and redisKey.unsafeGet.kind != SortedSetKey:
    return wrongTypeErrorReply

  var score: float64
  try:
    if cmpIgnoreCase(cmd.args[1], "inf") == 0 or 
      cmpIgnoreCase(cmd.args[1], "+inf") == 0:
      score = Inf
    elif cmpIgnoreCase(cmd.args[1], "-inf") == 0:
      score = NegInf
    else:
      score = parseFloat(cmd.args[1])
  except:
    return floatErrorReply

  if not redisKey.isSome:
    redisKey = some(insertNewRedisKey(cmd.args[0], SortedSetKey))

  var inserted: int
  if not redisSortedSetMemberExists(redisKey.unsafeGet.id, cmd.args[2]):
    inc inserted

  upsertRedisSortedSetMember2(redisKey.unsafeGet.id, cmd.args[2], score)
  
  integerReply(inserted)

proc zremCommand(cmd: RedisCommand): string =
  if cmd.args.len < 2:
    return wrongNumberOfArgsReply(cmd.raw)

  let redisKey = getRedisKey(cmd.args[0])
  if not redisKey.isSome:
    return integerReply(0)

  if redisKey.unsafeGet.kind != SortedSetKey:
    return wrongTypeErrorReply

  if cmd.args.len == 2:
    deleteRedisSortedSetMember2(redisKey.unsafeGet.id, cmd.args[1])
  else:
    var members: seq[string]
    for i in 1 ..< cmd.args.len:
      members.add(cmd.args[i])
    let stmt = stepSqlIn(
      "delete from redis_sorted_sets where redis_key_id = ? and member",
      redisKey.unsafeGet.id, 
      members
    )
    discard sqlite3_finalize(stmt)

  let deleted = sqlite3_changes64(db)

  if countRedisSortedSetMembers2(redisKey.unsafeGet.id) == 0:
    deleteRedisKey2(cmd.args[0])

  integerReply(deleted)

proc zcardCommand(cmd: RedisCommand): string =
  if cmd.args.len != 1:
    return wrongNumberOfArgsReply(cmd.raw)

  let redisKey = getRedisKey(cmd.args[0])
  if not redisKey.isSome:
    return integerReply(0)

  if redisKey.unsafeGet.kind == SortedSetKey:
    integerReply(countRedisSortedSetMembers2(redisKey.unsafeGet.id))
  else:
    wrongTypeErrorReply

proc zscoreCommand(cmd: RedisCommand): string =
  if cmd.args.len != 2:
    return wrongNumberOfArgsReply(cmd.raw)

  let redisKey = getRedisKey(cmd.args[0])
  if not redisKey.isSome:
    return nilReply

  if redisKey.unsafeGet.kind != SortedSetKey:
    return wrongTypeErrorReply

  let score = getRedisSortedSetMemberScore(redisKey.unsafeGet.id, cmd.args[1])
  if score.isSome:
    bulkStringReply($score.unsafeGet)
  else:
    nilReply

proc zcountCommand(cmd: RedisCommand): string =
  if cmd.args.len != 3:
    return wrongNumberOfArgsReply(cmd.raw)

  let redisKey = getRedisKey(cmd.args[0])
  if not redisKey.isSome:
    return integerReply(0)

  if redisKey.unsafeGet.kind != SortedSetKey:
    return wrongTypeErrorReply

  var scores: array[2, float64]
  for i in 0 .. 1:
    try:
      if cmpIgnoreCase(cmd.args[1 + i], "inf") == 0 or 
        cmpIgnoreCase(cmd.args[1 + i], "+inf") == 0:
        scores[i] = Inf
      elif cmpIgnoreCase(cmd.args[1 + i], "-inf") == 0:
        scores[i] = NegInf
      else:
        scores[i] = parseFloat(cmd.args[1 + i])
    except:
      return floatErrorReply

  integerReply(countRedisSortedSetMembersInRange2(redisKey.unsafeGet.id, scores[0], scores[1]))

proc zremrangebyscoreCommand(cmd: RedisCommand): string =
  if cmd.args.len != 3:
    return wrongNumberOfArgsReply(cmd.raw)

  let redisKey = getRedisKey(cmd.args[0])
  if not redisKey.isSome:
    return integerReply(0)

  if redisKey.unsafeGet.kind != SortedSetKey:
    return wrongTypeErrorReply

  var scores: array[2, float64]
  for i in 0 .. 1:
    try:
      if cmpIgnoreCase(cmd.args[1 + i], "inf") == 0 or 
        cmpIgnoreCase(cmd.args[1 + i], "+inf") == 0:
        scores[i] = Inf
      elif cmpIgnoreCase(cmd.args[1 + i], "-inf") == 0:
        scores[i] = NegInf
      else:
        scores[i] = parseFloat(cmd.args[1 + i])
    except:
      return floatErrorReply


  deleteRedisSortedSetMembersInRange(redisKey.unsafeGet.id, scores[0], scores[1])

  let deleted = sqlite3_changes64(db)

  if countRedisSortedSetMembers2(redisKey.unsafeGet.id) == 0:
    deleteRedisKey2(cmd.args[0])

  integerReply(deleted)

proc execute(cmd: RedisCommand): string =
  case cmd.normalized:
  of "ECHO":
    echoCommand(cmd)
  of "PING":
    pingCommand(cmd)
  of "GET":
    getCommand(cmd)
  of "SET":
    setCommand(cmd)
  of "DEL":
    delCommand(cmd)
  of "EXPIRE", "EXPIREAT":
    expireCommand(cmd)
  of "TTL", "EXPIRETIME":
    ttlCommand(cmd)
  of "EXISTS":
    existsCommand(cmd)
  of "PERSIST":
    persistCommand(cmd)
  of "TYPE":
    typeCommand(cmd)
  of "DECR", "DECRBY", "INCR", "INCRBY":
    if (cmd.normalized in ["INCR", "DECR"] and cmd.args.len == 1) or
      (cmd.normalized in ["INCRBY", "DECRBY"] and cmd.args.len == 2):
      var increment = 1
      if cmd.normalized in ["INCRBY", "DECRBY"]:
        try:
          increment = parseInt(cmd.args[1])
        except:
          return integerErrorReply
      if cmd.normalized in ["DECR", "DECRBY"]:
        increment = -increment
      incrbyCommand(cmd.args[0], increment)
    else:
      wrongNumberOfArgsReply(cmd.raw)
  of "GETDEL":
    getdelCommand(cmd)
  of "HSET":
    hsetCommand(cmd)
  of "HSETNX":
    hsetnxCommand(cmd)
  of "HGET":
    hgetCommand(cmd)
  of "HINCRBY":
    hincrbyCommand(cmd)
  of "HDEL":
    hdelCommand(cmd)
  of "HEXISTS":
    hexistsCommand(cmd)
  of "HGETALL":
    hgetallCommand(cmd)
  of "HLEN":
    hlenCommand(cmd)
  of "HKEYS":
    hkeysCommand(cmd)
  of "SADD":
    saddCommand(cmd)
  of "SCARD":
    scardCommand(cmd)
  of "SISMEMBER":
    sismemberCommand(cmd)
  of "SMEMBERS":
    smembersCommand(cmd)
  of "SREM":
    sremCommand(cmd)
  of "SPOP":
    spopCommand(cmd)
  of "ZADD":
    zaddCommand(cmd)
  of "ZREM":
    zremCommand(cmd)
  of "ZCARD":
    zcardCommand(cmd)
  of "ZSCORE":
    zscoreCommand(cmd)
  of "ZCOUNT":
    zcountCommand(cmd)
  of "ZREMRANGEBYSCORE":
    zremrangebyscoreCommand(cmd)
  else:
    simpleErrorReply("ERR unknown command '" & cmd.raw & '\'')

proc popRedisCommand(dataEntry: DataEntry, pos: var int): Option[RedisCommand] =

  proc redisParseInt(buf: string, start, expectedLen: int): int =
    try:
      let byteLen = parseInt(buf, result, start)
      if byteLen != expectedLen:
        raise newException(CatchableError, "Number byte len mismatch")
    except ValueError:
      raise newException(CatchableError, "Error parsing number")

  if dataEntry.recvBuf[pos] == '*':
    inc pos
    var arrayLen: int
    block:
      let numEnd = dataEntry.recvBuf.find("\r\n", pos, dataEntry.bytesReceived - 1)
      if numEnd < 0:
        return # Need more bytes
      arrayLen = redisParseInt(dataEntry.recvBuf, pos, numEnd - pos)
      pos = numEnd + 2
    if arrayLen <= 0:
      raise newException(CatchableError, "Unexpected array len")
    var bulkStrings: seq[(int, int)]
    for _ in 0 ..< arrayLen:
      if pos >= dataEntry.bytesReceived:
        return # Need more bytes
      if dataEntry.recvBuf[pos] == '$':
        inc pos
        let numEnd = dataEntry.recvBuf.find("\r\n", pos, dataEntry.bytesReceived - 1)
        if numEnd < 0:
          return # Need more bytes
        let strLen = redisParseInt(dataEntry.recvBuf, pos, numEnd - pos)
        pos = numEnd + 2
        if strLen >= 0:
          if pos + strLen + 2 > dataEntry.bytesReceived:
            return # Need more bytes
          bulkStrings.add((pos, strLen))
          pos += strLen
          if dataEntry.recvBuf[pos] != '\r' or dataEntry.recvBuf[pos + 1] != '\n':
            raise newException(
              CatchableError,
              "Unexpected bytes after bulk string"
            )
          pos += 2
        else:
          raise newException(CatchableError, "Unexpected bulk string len")
      else:
        raise newException(
          CatchableError,
          "Unexpected RESP data type " &
          dataEntry.recvBuf[pos] & " (" & $dataEntry.recvBuf[pos].uint8 & ")"
        )
    var tmp: RedisCommand
    for i, (start, len) in bulkStrings:
      var s = newString(len)
      if len > 0:
        copyMem(s.cstring, dataEntry.recvBuf[start].addr, len)
      if i == 0:
        tmp.raw = ensureMove s
      else:
        tmp.args.add(ensureMove s)
    tmp.normalized = tmp.raw
    for c in tmp.normalized.mitems:
      c = toUpperAscii(c)
    return some(ensureMove tmp)
  else:
    raise newException(
      CatchableError,
      "Unexpected RESP data type " &
      dataEntry.recvBuf[pos] & " (" & $dataEntry.recvBuf[pos].uint8 & ")"
    )

let selector = newSelector[DataEntry]()

proc afterRecv(clientSocket: SocketHandle): bool =
  let dataEntry = selector.getData(clientSocket)

  var
    startOfNextCmd: Option[int]
    needsWriteUpdate = false
  try:
    var pos: int
    while pos < dataEntry.bytesReceived:
      let cmd = dataEntry.popRedisCommand(pos)
      if cmd.isSome:
        startOfNextCmd = some(pos)
        if dataEntry.outgoingBuffers.len == 0:
          needsWriteUpdate = true
        beginTransaction()
        try:
          dataEntry.outgoingBuffers.addLast(
            OutgoingBuffer(buffer: execute(cmd.unsafeGet))
          )
          commitTransaction()
        except:
          rollbackTransaction()
          raise
      else:
        break
  except:
    # TODO: error logging?
    echo getCurrentExceptionMsg()
    return true # Close the connection

  if needsWriteUpdate:
    selector.updateHandle(clientSocket, {Read, Write})

  if startOfNextCmd.isSome:
    # Remove finished commands from the receive buffer
    if startOfNextCmd.unsafeGet == dataEntry.bytesReceived:
      dataEntry.bytesReceived = 0
    else:
      copyMem(
        dataEntry.recvBuf[0].addr,
        dataEntry.recvBuf[startOfNextCmd.unsafeGet].addr,
        dataEntry.bytesReceived - startOfNextCmd.unsafeGet
      )
      dataEntry.bytesReceived -= startOfNextCmd.unsafeGet

let
  expireTimerFd = selector.registerTimer(1 * 1000, false, nil)
  redisSocket = createNativeSocket(
    Domain.AF_INET,
    SockType.SOCK_STREAM,
    Protocol.IPPROTO_TCP,
    false
  )
if redisSocket == osInvalidSocket:
  raiseOSError(osLastError())

redisSocket.setBlocking(false)
redisSocket.setSockOptInt(SOL_SOCKET, SO_REUSEADDR, 1)

block:
  let ai = getAddrInfo(
    "0.0.0.0", # TODO
    Port(6379),
    Domain.AF_INET,
    SockType.SOCK_STREAM,
    Protocol.IPPROTO_TCP,
  )
  try:
    if bindAddr(redisSocket, ai.ai_addr, ai.ai_addrlen.SockLen) < 0:
      raiseOSError(osLastError())
  finally:
    freeAddrInfo(ai)

if nativesockets.listen(redisSocket, listenBacklogLen) < 0:
  raiseOSError(osLastError())

selector.registerHandle(redisSocket, {Read}, DataEntry(kind: ServerSocketEntry))

var
  readyKeys: array[maxEventsPerSelectLoop, ReadyKey]
  receivedFrom: seq[SocketHandle]
  needClosing: HashSet[SocketHandle]
while true:
  receivedFrom.setLen(0)
  needClosing.clear()

  let readyCount = selector.selectInto(-1, readyKeys)
  for i in 0 ..< readyCount:
    let readyKey = readyKeys[i]

    # echo "Socket ready: ", readyKey.fd, " ", readyKey.events

    if readyKey.fd == expireTimerFd:
      let now = epochTime().int
      if sqlite3_bind_int64(
        deleteExpiredRedisKeys.stmt,
        1,
        now
      ) != SQLITE_OK:
        raise newException(CatchableError, "SQLite: " & $sqlite3_errmsg(db))
      discard deleteExpiredRedisKeys.step()
      deleteExpiredRedisKeys.reset()
    elif readyKey.fd == redisSocket.int:
      # We should have a new client socket to accept
      if Read in readyKey.events:
        let (clientSocket, remoteAddress) =
          when defined(linux) and not defined(nimdoc):
            var
              sockAddr: SockAddr
              addrLen = sizeof(sockAddr).SockLen
            let
              socket =
                accept4(
                  redisSocket,
                  sockAddr.addr,
                  addrLen.addr,
                  SOCK_CLOEXEC or SOCK_NONBLOCK
                )
              sockAddrStr =
                try:
                  getAddrString(sockAddr.addr)
                except:
                  ""
            (socket, sockAddrStr)
          else:
            redisSocket.accept()

        if clientSocket == osInvalidSocket:
          continue

        when not defined(linux):
          # Not needed on linux where we can use SOCK_NONBLOCK
          clientSocket.setBlocking(false)

        let dataEntry = DataEntry(kind: ClientSocketEntry)
        dataEntry.remoteAddress = remoteAddress
        dataEntry.recvBuf.setLen(initialRecvBufLen)
        selector.registerHandle(clientSocket, {Read}, dataEntry)

    else: # Client socket
      if Error in readyKey.events:
        needClosing.incl(readyKey.fd.SocketHandle)
        continue

      let dataEntry = selector.getData(readyKey.fd)

      if Read in readyKey.events:
        # Expand the buffer if it is full
        if dataEntry.bytesReceived == dataEntry.recvBuf.len:
          dataEntry.recvBuf.setLen(dataEntry.recvBuf.len * 2)

        let bytesReceived = readyKey.fd.SocketHandle.recv(
          dataEntry.recvBuf[dataEntry.bytesReceived].addr,
          (dataEntry.recvBuf.len - dataEntry.bytesReceived).cint,
          0
        )
        if bytesReceived > 0:
          dataEntry.bytesReceived += bytesReceived
          receivedFrom.add(readyKey.fd.SocketHandle)
        else:
          needClosing.incl(readyKey.fd.SocketHandle)
          continue

      if Write in readyKey.events:
        let
          outgoingBuffer = dataEntry.outgoingBuffers.peekFirst()
          bytesSent = readyKey.fd.SocketHandle.send(
            outgoingBuffer.buffer[outgoingBuffer.bytesSent].addr,
            (outgoingBuffer.buffer.len - outgoingBuffer.bytesSent).cint,
            when defined(MSG_NOSIGNAL): MSG_NOSIGNAL else: 0
          )
        if bytesSent > 0:
          outgoingBuffer.bytesSent += bytesSent
          if outgoingBuffer.bytesSent == outgoingBuffer.buffer.len:
            # The current outgoing buffer for this socket has been fully sent
            # Remove it from the outgoing buffer queue
            dataEntry.outgoingBuffers.shrink(fromFirst = 1)
          # If we don't have any more outgoing buffers, update the selector
          if dataEntry.outgoingBuffers.len == 0:
            selector.updateHandle(readyKey.fd.SocketHandle, {Read})
        else:
          needClosing.incl(readyKey.fd.SocketHandle)
          continue

  for clientSocket in receivedFrom:
    if clientSocket in needClosing:
      continue
    let needsClosing = afterRecv(clientSocket)
    if needsClosing:
      needClosing.incl(clientSocket)

  for clientSocket in needClosing:
    try:
      selector.unregister(clientSocket)
    except:
      # Should never happen
      # Leaks DataEntry for this socket
      discard
    finally:
      clientSocket.close()
