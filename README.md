<p align="center">
  <a href="https://ndtan.net" target="_blank" rel="noopener">
    <picture>
      <img alt="NDT DBF logo" src="./assets/brand/logo.png" width="520">
    </picture>
  </a>
</p>
<p align="center"><strong>NDT DBF</strong> — A single‑file PHP Database Framework (PRO · Enterprise+).<br>
Secure by default, compact API, works as <em>one file</em> or via <strong>Composer/PSR‑4</strong>.</p>

<p align="center">
  <a href="https://ndtan.net">Website & Docs</a> ·
  <a href="https://github.com/nguyenduytan/NDT-DBF/blob/main/src/DBF.php">Download single file</a> ·
  <a href="https://www.paypal.com/paypalme/copbeo">Donate</a>
</p>

<p align="center">
  <a href="https://github.com/nguyenduytan/NDT-DBF/actions">
    <img alt="build" src="https://img.shields.io/github/actions/workflow/status/nguyenduytan/NDT-DBF/ci.yml?label=build&logo=github">
  </a>
  <a href="https://packagist.org/packages/ndtan/dbf">
    <img alt="stable" src="https://img.shields.io/packagist/v/ndtan/dbf.svg?label=stable">
  </a>
  <img alt="downloads" src="https://img.shields.io/packagist/dm/ndtan/dbf.svg?label=downloads">
  <img alt="php" src="https://img.shields.io/badge/php-%3E%3D%208.1-777bb3">
  <img alt="license" src="https://img.shields.io/badge/license-MIT-brightgreen">
</p>
<p align="center" dir="auto">
    <a href="https://github.com/nguyenduytan/NDT-DBF/actions"><img alt="Build Status" src="https://github.com/nguyenduytan/NDT-DBF/actions/workflows/ci.yml/badge.svg" style="max-width: 100%;"></a>
    <a href="https://packagist.org/packages/ndtan/dbf" rel="nofollow"><img alt="Total Downloads" src="https://camo.githubusercontent.com/524236374e9eb92f2857e742171e1b8136777ef08041555739317a3a553f25ad/68747470733a2f2f706f7365722e707567782e6f72672f63617466616e2f6d65646f6f2f646f776e6c6f616473" data-canonical-src="hhttps://poser.pugx.org/ndtan/dbf/downloads" style="max-width: 100%;"></a>
    <a href="https://packagist.org/packages/ndtan/dbf" rel="nofollow"><img alt="Latest Stable Version" src="https://camo.githubusercontent.com/2d2894222800260b3835e9210b29e9291d59f5776a1a17ed4e35f8e40ec6800f/68747470733a2f2f706f7365722e707567782e6f72672f63617466616e2f6d65646f6f2f762f737461626c65" data-canonical-src="https://poser.pugx.org/ndtan/dbf/v/stable" style="max-width: 100%;"></a>
    <a href="https://packagist.org/packages/ndtan/dbf" rel="nofollow"><img alt="License" src="https://camo.githubusercontent.com/e3670c6b08206f0dd7c17934f220365e71d972e98feddeb0becebc1f21ad5bda/68747470733a2f2f706f7365722e707567782e6f72672f63617466616e2f6d65646f6f2f6c6963656e7365" data-canonical-src="hhttps://poser.pugx.org/ndtan/dbf/license" style="max-width: 100%;"></a>
</p>

---

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
  - [Composer](#composer)
  - [Single file](#single-file)
- [Connection Setup](#connection-setup)
  - [URI/DSN (one line)](#uridsn-one-line)
  - [Array config](#array-config)
  - [Existing PDO](#existing-pdo)
  - [Master/Replica](#masterreplica)
- [Quick Start](#quick-start)
- [Query Builder](#query-builder)
  - [Basic select](#basic-select)
  - [Where conditions](#where-conditions)
  - [Join / Group / Having / Order / Limit](#join--group--having--order--limit)
  - [CRUD](#crud)
  - [Upsert](#upsert)
  - [Aggregates & Pluck](#aggregates--pluck)
  - [Keyset pagination](#keyset-pagination)
- [Enterprise+ Features](#enterprise-features)
  - [Transactions + Deadlock retry](#transactions--deadlock-retry)
  - [Readonly/Maintenance mode](#readonlymaintenance-mode)
  - [Soft Delete guard](#soft-delete-guard)
  - [Middleware / Policy / Metrics](#middleware--policy--metrics)
  - [Per-query timeout](#per-query-timeout)
  - [Test Mode](#test-mode)
- [Raw SQL](#raw-sql)
- [Security](#security)
- [Debugging & Logging](#debugging--logging)
- [Tests & CI](#tests--ci)
- [Contributing](#contributing)
- [License](#license)
- [Donate](#donate)

---

## Features

- ⚡️ **Lightweight & single file**: no third-party deps (PDO only)
- 🔐 **Secure by default**: prepared statements, per-driver identifier quoting, IN-list guard
- 🧱 **Query Builder**: `select / where / orWhere / whereIn / between / null / join / group / having / order / limit / offset`
- 🧾 **Full CRUD**: `insert`, `insertMany`, `insertGet` (PG/SQLite `RETURNING`)
- 🔁 **Cross-dialect Upsert**: MySQL (`ON DUP KEY`), PG/SQLite (`ON CONFLICT`), safe fallback
- 💳 **Transactions**: exponential backoff + jitter on deadlock
- 🚦 **Readonly/Maintenance mode**: block DML when system is frozen
- 🪶 **Soft Delete**: `withTrashed() / onlyTrashed() / restore() / forceDelete()`
- 🧭 **Master/Replica routing**: auto/manual read-write split
- ⏱️ **Per-query timeout** (best-effort for MySQL/PG)
- 🧩 **Middleware & Policy hooks** + **Logger** & **Metrics hook**
- 📊 **Aggregates & Sugar**: `sum()`, `avg()`, `min()`, `max()`, `pluck()`
- 🧪 **Test Mode**: build SQL **without executing** (great for unit tests/previews)
- 📚 **Keyset pagination** helper

Supports **MySQL/MariaDB**, **PostgreSQL**, **SQLite**, **SQL Server**.

---

## Requirements

- PHP **>= 8.1**
- Extension: **PDO** (+ respective drivers: `pdo_mysql`, `pdo_pgsql`, `pdo_sqlite`, `pdo_sqlsrv`)

---

## Installation

### Composer

```bash
composer require ndtan/dbf
```

```php
<?php
require 'vendor/autoload.php';

$db = new \ndtan\DBF([
  'type'     => 'mysql',
  'host'     => '127.0.0.1',
  'database' => 'app',
  'username' => 'root',
  'password' => 'secret',
  'charset'  => 'utf8mb4',
  'logging'  => false,
]);
```

### Single file

Copy `src/DBF.php` into your project:

```php
<?php
require __DIR__ . '/DBF.php';

$db = new \ndtan\DBF('mysql://root:secret@127.0.0.1/app?charset=utf8mb4');
```

> You may also use **ENV**: set `NDTAN_DBF_URL` and call `new \ndtan\DBF()`.

---

## Connection Setup

### URI/DSN (one line)

```php
$db = new \ndtan\DBF('pgsql://user:pass@localhost:5432/app');
$db = new \ndtan\DBF('sqlite:///:memory:');        // memory
$db = new \ndtan\DBF('sqlite:///path/to/app.db');  // file
$db = new \ndtan\DBF('sqlsrv://sa:pass@localhost/app');
```

### Array config

```php
$db = new \ndtan\DBF([
  'type'     => 'mysql',           // mysql|pgsql|sqlite|sqlsrv
  'host'     => 'localhost',
  'database' => 'app',
  'username' => 'root',
  'password' => 'secret',
  'charset'  => 'utf8mb4',
  'collation'=> 'utf8mb4_general_ci',
  'prefix'   => 'app_',
  'logging'  => true,
  'error'    => PDO::ERRMODE_EXCEPTION,
  'option'   => [PDO::ATTR_CASE => PDO::CASE_NATURAL],

  // Enterprise options
  'readonly' => false,
  'features' => [
    'soft_delete'   => ['enabled'=>true,'column'=>'deleted_at','mode'=>'timestamp'],
    'max_in_params' => 1000
  ],
  'command'  => ['SET SQL_MODE=ANSI_QUOTES'],
]);
```

### Existing PDO

```php
$pdo = new PDO('mysql:host=127.0.0.1;dbname=app;charset=utf8mb4','root','secret',[
  PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
  PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
  PDO::ATTR_EMULATE_PREPARES => false,
]);

$db = new \ndtan\DBF(['pdo'=>$pdo, 'type'=>'mysql']);
```

### Master/Replica

```php
$db = new \ndtan\DBF([
  'write'   => 'mysql://root:secret@10.0.0.10/app',
  'read'    => 'mysql://reader:secret@10.0.0.11/app',
  'routing' => 'auto', // auto|manual|single
  'prefix'  => 'app_',
]);
// Manual route when needed:
$one = $db->using('read')->table('users')->first();
```

---

## Quick Start

```php
$users = $db->table('users')
  ->select(['id','email'])
  ->where('status','=','active')
  ->orderBy('id','desc')
  ->limit(20)
  ->get();
```

---

## Query Builder

### Basic select

```php
$db->table('users')->get();              // SELECT * FROM users
$db->table('users')->first();            // fetch one row
$db->table('users')->exists();           // boolean
$db->table('users')->count();            // COUNT(*)
```

### Where conditions

```php
$db->table('users')->where('status','=','active')->get();
$db->table('users')->where('id','>',100)->orWhere('email','=','a@ndtan.net')->get();
$db->table('users')->whereBetween('created_at',['2025-01-01','2025-12-31'])->get();
$db->table('users')->whereIn('id',[1,2,3])->get();   // guarded IN list
$db->table('users')->whereNull('deleted_at')->get();
```

### Join / Group / Having / Order / Limit

```php
$db->table('orders')
  ->select(['orders.id','users.email','SUM(total) AS sum_total'])
  ->join('users','orders.user_id','=','users.id')
  ->groupBy(['orders.user_id'])
  ->having('SUM(total)','>',100)
  ->orderBy('orders.id','desc')
  ->limit(50)->offset(0)
  ->get();
```

### CRUD

```php
// Insert
$id = $db->table('users')->insert(['email'=>'a@ndtan.net','status'=>'active']);

// Insert many
$db->table('order_items')->insertMany([
  ['order_id'=>1,'sku'=>'A','qty'=>1],
  ['order_id'=>1,'sku'=>'B','qty'=>2],
]);

// Insert + RETURNING (PG/SQLite) or re-fetch by id (MySQL/SQLSrv)
$row = $db->table('users')->insertGet(['email'=>'b@ndtan.net','status'=>'vip'], ['id','email']);

// Update
$aff = $db->table('users')->where('id','=', $id)->update(['status'=>'vip']);

// Delete (soft delete if enabled)
$aff = $db->table('users')->where('id','=', $id)->delete();
```

### Upsert

```php
// MySQL: ON DUPLICATE KEY; PG/SQLite: ON CONFLICT; others: safe fallback (tx)
$db->table('users')->upsert(
  ['email'=>'a@ndtan.net','status'=>'vip'],
  conflict: ['email'],
  updateColumns: ['status']
);
```

### Aggregates & Pluck

```php
$sum = $db->table('orders')->sum('total');
$avg = $db->table('orders')->avg('total');
$min = $db->table('orders')->min('total');
$max = $db->table('orders')->max('total');

$emails = $db->table('users')->pluck('email');          // ['a@x','b@y',...]
$map    = $db->table('users')->pluck('email','id');     // [1=>'a@x', 2=>'b@y', ...]
```

### Keyset pagination

```php
$page1 = $db->table('orders')->orderBy('id','desc')->limit(50)->getKeyset(null,'id');
$page2 = $db->table('orders')->orderBy('id','desc')->limit(50)->getKeyset($page1['next'],'id');
```

---

## Enterprise+ Features

### Transactions + Deadlock retry

```php
$db->tx(function(\ndtan\DBF $tx) {
  $orderId = $tx->table('orders')->insert(['user_id'=>10,'total'=>100,'created_at'=>date('c')]);
  $tx->table('order_items')->insert(['order_id'=>$orderId,'sku'=>'A','qty'=>1]);
}, attempts: 3); // auto-retry on deadlock
```

### Readonly/Maintenance mode

```php
$db->setReadonly(true);
// any DML (INSERT/UPDATE/DELETE) will be blocked
```

### Soft Delete guard

```php
// Enable via config: features.soft_delete.enabled=true
$db->table('users')->where('id','=',123)->delete();     // set deleted_at
$db->table('users')->withTrashed()->first();            // include soft-deleted
$db->table('users')->onlyTrashed()->get();              // only soft-deleted
$db->table('users')->where('id','=',123)->restore();    // restore
$db->table('users')->where('id','=',123)->forceDelete();// hard delete
```

### Middleware / Policy / Metrics

```php
// Middleware: audit
$db->use(function(array $ctx, callable $next) {
  $start = microtime(true);
  $res = $next($ctx);
  error_log('SQL '.$ctx['type'].' took '.round((microtime(true)-$start)*1000,2).'ms');
  return $res;
});

// Policy: block UPDATE on users
$db->policy(function(array $ctx){
  if (($ctx['table'] ?? null)==='users' && ($ctx['action'] ?? '')==='update') {
    throw new RuntimeException('Policy blocked');
  }
});

// Metrics hook
$db->setMetrics(function(array $m){
  // $m = ['type'=>'select','table'=>'users','ms'=>12.3,'count'=>10, ...]
});
```

### Per-query timeout

```php
$db->table('big_table')->timeout(1500)->get(); // 1.5s best-effort (MySQL/PG)
```

### Test Mode

```php
$db->setTestMode(true);
// Queries are not executed; SQL & params are still built
$rows = $db->table('users')->where('status','=','active')->limit(10)->get(); // []
echo $db->queryString();
print_r($db->queryParams());
```

---

## Raw SQL

```php
// Positional
$rows = $db->raw('SELECT * FROM users WHERE email LIKE ?', ['%ndtan.net']);

// Named
$rows = $db->raw('SELECT * FROM users WHERE email = :e', ['e'=>'a@ndtan.net']);

// DML
$aff  = $db->raw('UPDATE users SET status=? WHERE id=?', ['vip', 10]);
```

---

## Security

- **Prepared statements** for all user inputs
- **Identifier quoting** per driver to prevent injection via table/column names
- **IN-list guard**: limit `whereIn` items (default 1000)
- **Readonly mode** to block unintended writes
- **Scope/Policy** hooks for tenant/RBAC-like checks

> DBF is “secure by default”, but you should still harden your DB (users/roles, TLS, firewall, backups, monitoring).

---

## Debugging & Logging

```php
$db->setLogger(function(string $sql, array $params, float $ms) {
  error_log(sprintf('[SQL %.2fms] %s | %s', $ms, $sql, json_encode($params)));
});

$info = $db->info(); // driver, routing, readonly, soft_delete, test_mode...
```

---


## Tests & CI

Run tests (SQLite `:memory:`):

```bash
composer install
vendor/bin/phpunit
```

The template repo ships with GitHub Actions (`.github/workflows/ci.yml`) running PHPUnit on PHP 8.1–8.4.

---

## Contributing

PRs/issues are welcome! Please:

1. Fork → create a feature branch
2. Add tests for your change
3. Ensure `vendor/bin/phpunit` passes
4. Describe your change clearly in the PR

---

## License

MIT © Tony Nguyen

---

## Donate

If DBF helps your work, consider buying the author a coffee ☕️  
👉 **PayPal:** https://www.paypal.com/paypalme/copbeo
