when defined(windows):
  const libname = "sqlite3_64.dll"
elif defined(macosx):
  const libname = "libsqlite3(|.0).dylib"
else:
  const libname = "libsqlite3.so(|.0)"

type
  SqliteHandle* = distinct pointer
  SqliteStatement* = distinct pointer
  SqliteCallback* = proc(
    userValue: pointer,
    count: int32,
    values: cstringArray,
    columns: cstringArray
  ): int32 {.cdecl.}
  SqliteBindDestructor* = proc(p: pointer) {.cdecl, gcsafe.}

const
  SQLITE_OK* = 0
  SQLITE_ERROR* = 1
  SQLITE_NULL* = 5
  SQLITE_ROW* = 100
  SQLITE_DONE* = 101
  SQLITE_STATIC* = cast[SqliteBindDestructor](0)
  SQLITE_TRANSIENT* = cast[SqliteBindDestructor](-1)

{.push importc, cdecl, dynlib: libname.}

proc sqlite3_open*(
  filename: cstring,
  db: var SqliteHandle
): int32

proc sqlite3_errmsg*(
  db: SqliteHandle
): cstring

proc sqlite3_changes64*(
  db: SqliteHandle
): int64

proc sqlite3_last_insert_rowid*(
  db: SqliteHandle
): int64

proc sqlite3_exec*(
  db: SqliteHandle,
  sql: cstring,
  callback: SqliteCallback,
  callbackUserValue: pointer,
  errmsg: ptr cstring
): int32

proc sqlite3_prepare_v2*(
  db: SqliteHandle,
  zSql: cstring,
  nByte: int32,
  pStatement: var SqliteStatement,
  pzTail: ptr cstring
): int32

proc sqlite3_finalize*(
  statement: SqliteStatement
): int32

proc sqlite3_bind_parameter_count*(
  statement: SqliteStatement
): int32

proc sqlite3_bind_parameter_name*(
  statement: SqliteStatement,
  index: int32
): cstring

proc sqlite3_bind_null*(
  statement: SqliteStatement,
  index: int32
): int32

proc sqlite3_bind_int64*(
  statement: SqliteStatement,
  index: int32,
  value: int64
): int32

proc sqlite3_bind_double*(
  statement: SqliteStatement,
  index: int32,
  value: float64
): int32

proc sqlite3_bind_blob64*(
  statement: SqliteStatement,
  index: int32,
  value: pointer,
  len: int64,
  destructor: SqliteBindDestructor
): int32

proc sqlite3_reset*(
  statement: SqliteStatement
): int32

proc sqlite3_step*(
  statement: SqliteStatement
): int32

proc sqlite3_column_count*(
  statement: SqliteStatement
): int32

proc sqlite3_column_type*(
  statement: SqliteStatement,
  iCol: int32
): int32

proc sqlite3_column_int64*(
  statement: SqliteStatement,
  iCol: int32
): int64

proc sqlite3_column_double*(
  statement: SqliteStatement,
  iCol: int32
): float64

proc sqlite3_column_bytes*(
  statement: SqliteStatement,
  iCol: int32
): int32

proc sqlite3_column_blob*(
  statement: SqliteStatement,
  iCol: int32
): pointer

{.pop.}
