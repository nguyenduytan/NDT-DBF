<p align="center"><img alt="NDT DBF" src="./assets/brand/logo.png" width="360"></p>

# NDT DBF 0.3.0

A PHP SQL framework whose entire runtime is contained in **one file: `DBF.php`**.
PDO is the only runtime dependency. Use the query builder or parameterized SQL.

[![Release](https://img.shields.io/github/v/release/nguyenduytan/NDT-DBF?label=release)](https://github.com/nguyenduytan/NDT-DBF/releases/latest)
[![CI](https://github.com/nguyenduytan/NDT-DBF/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/nguyenduytan/NDT-DBF/actions/workflows/ci.yml)
[![PHP](https://img.shields.io/badge/php-%3E%3D%208.1-777bb4)](https://www.php.net/)
[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE.md)

[Download DBF.php](https://github.com/nguyenduytan/NDT-DBF/releases/download/v0.3.0/DBF.php) |
[Release notes](CHANGELOG.md) |
[Website source](website/) |
[Author](https://ndtan.net)

## Requirements

PHP 8.1 or later, PDO, and the database's PDO extension.
SQLite tests require SQLite JSON functions and SQLite 3.35+ for `RETURNING`.

| Feature | MySQL / MariaDB | PostgreSQL | SQLite | SQL Server / Oracle |
| --- | --- | --- | --- | --- |
| Connection, quoted CRUD, pagination | Yes | Yes | Yes | Basic SQL compilation; not integration tested |
| Transactions and nested savepoints | Yes | Yes | Yes | Throws unsupported |
| Upsert and unique metadata | Yes | Yes | Yes | Throws unsupported |
| JSON read and update | Yes | Yes, JSONB for updates | Yes | Throws unsupported |
| Row locking | Yes, version dependent | Yes | Unsupported | Unsupported |
| Query timeout | MySQL SELECT / MariaDB statement | Statement | Lock wait only | Unsupported |

Unsupported features fail explicitly. SQL Server pagination requires an order and positive limit.
Oracle pagination requires Oracle 12c+. Use lowercase PostgreSQL identifiers or quote exact names.
This package is a SQL library; it does not provide an ORM or migrations.

## Install

Single file:

```php
require __DIR__ . '/DBF.php';
$db = new ndtan\DBF('sqlite::memory:');
```

Composer:

```bash
composer require ndtan/dbf:^0.3
```

```php
require __DIR__ . '/vendor/autoload.php';
$db = new ndtan\DBF('mysql://user:password@127.0.0.1/app?charset=utf8mb4');
```

## Connection

```php
$db = new ndtan\DBF([
    'type' => 'mysql',
    'host' => '127.0.0.1',
    'port' => 3306,
    'database' => 'app',
    'username' => 'app_user',
    'password' => 'secret',
    'charset' => 'utf8mb4',
    'prefix' => 'app_',
    'options' => [PDO::ATTR_TIMEOUT => 5],
    'features' => [
        'max_in_params' => 1000,
        'soft_delete' => ['enabled' => true, 'column' => 'deleted_at', 'mode' => 'timestamp'],
    ],
]);
```

Accepted forms: URI, native PDO DSN, PDO instance, or config array.
Use `['dsn' => 'pgsql:host=localhost;dbname=app', 'username' => 'user', 'password' => 'secret']`
for a DSN with credentials. Existing PDO: `new ndtan\DBF($pdo)` or `new ndtan\DBF(['pdo' => $pdo])`.
SQLite memory: `sqlite::memory:` or `sqlite:///:memory:`; absolute paths are preserved.
URI credentials must be percent encoded if they contain reserved characters.

`new ndtan\DBF()` reads `NDTAN_DBF_URL`.
`options` (legacy alias `option`) accepts PDO attributes. Exception mode is enforced.
Configure the injected PDO's remaining attributes yourself.
`logging`, `command`, and `collation` are not config options.
Use `setLogger()` and explicit SQL for connection commands.

## Read / Write Routing

```php
$db = new ndtan\DBF([
    'write' => 'mysql://writer:secret@primary/app',
    'read' => 'mysql://reader:secret@replica/app',
    'routing' => 'auto',
]);
```

Each connection accepts URI, array, or PDO. `auto` routes builder reads and `selectRaw()`
to the reader. All writes and `raw()` use the writer. During any transaction,
reads and writes use the writer, including queries created by a scoped clone.

For manual routing set `routing => 'manual'`, then use `using('read')` or `using('write')`.
The route persists until changed. Writes still use the writer. `single` uses only the writer.
Replica lag is expected: read after a write inside `tx()` or explicitly choose the writer.
A read-only connection configuration defaults to readonly mode.

## Query Builder

```php
$rows = $db->table('users')
    ->select(['id', 'email AS address'])
    ->where('status', '=', 'active')
    ->whereIn('id', [1, 2, 3])
    ->orderBy('id', 'desc')
    ->limit(20)
    ->get();

$row = $db->table('users')->where('id', '=', 1)->first();
$exists = $db->table('users')->where('id', '=', 1)->exists();
```

`where(column, operator, value)` accepts scalar values and null, not an array condition DSL.
Operators: `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `LIKE`, `NOT LIKE`;
`ILIKE` and `NOT ILIKE` require PostgreSQL. Null equality becomes `IS NULL`;
null inequality becomes `IS NOT NULL`.

`orWhere()`, `whereBetween(column, [min, max])`, `whereNull()`, and `whereIn()`
are supported. An empty IN is false; an empty NOT IN is true. IN lists are guarded.
User conditions are parenthesized together underneath scope/soft-delete guards.
Within that group, SQL's normal AND/OR precedence applies.

Identifiers support qualified names, `table.*`, column aliases, and
`COUNT/SUM/AVG/MIN/MAX(column) AS alias`. Arbitrary expressions belong in parameterized `raw()`.
Builder methods mutate their builder. Create a new builder for independent queries;
`first()`, `pluck()`, and `getKeyset()` preserve its selection and limit.

## Joins and Aggregates

```php
$rows = $db->table('orders')
    ->select(['orders.user_id', 'SUM(orders.total) AS total'])
    ->join('users', 'orders.user_id', '=', 'users.id')
    ->groupBy(['orders.user_id'])
    ->having('SUM(orders.total)', '>', 100)
    ->get();

$count = $db->table('orders')->count();
$sum = $db->table('orders')->sum('total');
$avg = $db->table('orders')->avg('total');
$emails = $db->table('users')->pluck('email');
$map = $db->table('users')->pluck('email', 'id');
```

Aggregates retain joins and filters. Grouped `count()` counts groups.
Grouped numeric aggregates returning several values throw; use an aggregate alias with `get()`.
Database scalar types are preserved (DECIMAL may be a string); empty sum is 0, empty average/min/max is null.
Joins apply the configured prefix to joined table names. Qualified references must use physical prefixed names.

## Writes

```php
$id = $db->table('users')->insert(['email' => 'a@example.com']);
$ids = $db->table('users')->insertMany([
    ['email' => 'b@example.com'],
    ['email' => 'c@example.com'],
]);
$row = $db->table('users')->insertGet(['email' => 'd@example.com'], ['id', 'email']);
$affected = $db->table('users')->where('id', '=', $id)->update(['status' => 'vip']);
$affected = $db->table('users')->where('id', '=', $id)->delete();
```

`insertMany()` normalizes column order and runs individual inserts inside one transaction
to return actual numeric IDs without assuming contiguous sequences. A failed row rolls back the batch.
Use native parameterized bulk SQL when throughput matters more than obtaining each ID.
Numeric `insert()` IDs require database-generated numeric identity; for UUID keys use `insertGet()`
on PostgreSQL/SQLite. Its fallback on other drivers requires a column named `id` and reads the writer.

Empty insert/update data is rejected. Unfiltered update/delete affect the entire table:
supply a WHERE clause when that is not intended.

```php
$db->table('users')->upsert(
    ['email' => 'a@example.com', 'status' => 'vip'],
    ['email'],
    ['status']
);
```

The database must have the correct unique/primary constraint. PostgreSQL/SQLite use the conflict columns.
MySQL handles conflicts on any unique key; its affected-row count has driver-specific semantics.
Empty update columns produce DO NOTHING (MySQL uses a no-op assignment).

## Soft Delete and Scope

Enable a nullable timestamp `deleted_at` column using the connection config above.
Default reads exclude deleted rows; use `withTrashed()` or `onlyTrashed()`.
`delete()` sets the timestamp; `restore()` clears it; `forceDelete()` physically deletes.
Flag mode uses `mode => 'flag'`, active value 0 and configured `deleted_value`.

```php
$tenantDb = $db->withScope(['tenant_id' => 7]);
$users = $tenantDb->table('users')->get();
$tenantDb->table('users')->where('id', '=', 1)->restore();
```

Scope filters builder queries; it does not automatically populate inserted data or protect raw SQL.
Supply tenant columns on insert and use database permissions for isolation.
`withScope()`, `policy()`, and `use()` return a clone; assign the return value.

## Transactions and Row Locks

```php
$db->tx(function (ndtan\DBF $tx) {
    $row = $tx->table('accounts')->where('id', '=', 1)->forUpdate()->first();
    $tx->table('accounts')->where('id', '=', 1)->update(['balance' => 100]);
}, attempts: 3);
```

MySQL, PostgreSQL and SQLite support nested `tx()` via savepoints.
Only the outer transaction retries known deadlock/serialization/busy errors.
The callback may run more than once: keep email, payment, and other external side effects outside it.
Do not begin/commit a transaction manually inside the callback.
Externally owned PDO transactions cannot be nested through `tx()`.

`forUpdate()` requires an active MySQL/PostgreSQL transaction and always uses the writer.
`skipLocked()` requires `forUpdate()` and database version support.

## Pagination and Streaming

```php
$query = $db->table('users')->orderBy('id', 'desc')->limit(50);
$page = $query->getKeyset(null, 'id');
$next = $page['next'] === null ? null : $query->getKeyset($page['next'], 'id');

foreach ($db->table('users')->orderBy('id')->stream() as $row) {
    processRow($row);
}
$db->table('users')->chunkById(100, function (array $rows) {
    processBatch($rows);
});
```

Keyset uses one unique, non-null, unqualified key that must be selected. Set a positive limit;
the only ordering must be that key. Cursor direction/key are validated and one extra row determines
whether a next page exists. Cursors from 0.2.x must be regenerated.
`chunkById()` supports deleting processed rows; `chunk()` uses offsets and needs stable data.
Returning false from either callback stops iteration.
Streaming closes cursors even when stopped early; PDO driver buffering may still retain rows.

## JSON

```php
$row = $db->table('users')->whereJson('data->profile->name', '=', 'Tony')->first();
$db->table('users')->where('id', '=', 1)->jsonSet('data', [
    'profile.name' => 'Tony',
    'active' => true,
    'roles' => ['editor'],
]);
```

Paths accept alphanumeric/underscore segments. Multiple updates are nested in one expression.
`jsonSet()` preserves JSON scalar/array/object types and executes immediately, returning the builder.
PostgreSQL updates require JSONB; missing intermediate parents must already exist.
JSON null queries have database-specific semantics; use native SQL for complex paths/array indexes.

## Raw SQL

```php
$rows = $db->selectRaw('SELECT email FROM users WHERE id = :id', ['id' => 1]);
$affected = $db->execute('UPDATE users SET status = ? WHERE id = ?', ['vip', 1]);
$result = $db->raw('INSERT INTO users(email) VALUES (?) RETURNING id', ['a@example.com']);
```

`raw()` always runs on the writer, returns rows when the statement has result columns,
otherwise affected rows. It accepts CTEs, DDL and RETURNING without guessing the SQL verb.
`execute()` is for DML/DDL without a returned result set. `selectRaw()` routes to the reader
and accepts a leading SELECT without a semicolon.

Readonly mode blocks writes and all `raw()` calls; use builder reads or `selectRaw()`.
A leading SELECT can still invoke side-effecting database functions: use an actual read-only
database account to enforce permissions. Raw SQL is trusted code; bind every untrusted value.

## Observability and Test Mode

```php
$db->setLogger(function (string $sql, array $params, float $ms) {
    error_log(sprintf('[%.2fms] %s', $ms, $sql));
});
$db->setMetrics(function (array $metrics) {
    recordMetrics($metrics);
});
$db = $db->policy(function (array $ctx) {
    if (($ctx['type'] ?? '') === 'delete') throw new RuntimeException('Deletion denied');
});
$db = $db->use(fn(array $ctx, callable $next) => $next($ctx));

$db->setTestMode(true);
$db->table('users')->where('id', '=', 1)->get();
echo $db->queryString();
$params = $db->queryParams();
```

Query context includes type and table (raw context includes SQL). Logger/metrics callbacks
must not throw after a successful write; use nonthrowing callbacks and redact sensitive data.
`queryString()`/`queryParams()` track executed or previewed SQL.
Test mode still establishes connections and may inspect schema; it is not an offline database mock.
Transactions are blocked in test mode.

`timeout(ms)` restores connection settings after execution. MySQL execution limits apply to SELECT,
MariaDB and PostgreSQL have statement timeouts; SQLite busy_timeout limits lock waits, not CPU time.

## Tests and Website

```bash
composer install
composer test -- --exclude-group integration
php -S 127.0.0.1:8080 -t website
```

CI runs SQLite regression tests on PHP 8.1-8.5 and integration tests against MySQL 8.4/PostgreSQL 16.
Integration tests use disposable tables in a dedicated test database.
The `website/` directory is synchronized with the project's `web3/` docs site.
Publish it on a PHP host; it needs no SQL connection.

## License

MIT, Tony Nguyen. [Donate](https://www.paypal.com/paypalme/copbeo).
