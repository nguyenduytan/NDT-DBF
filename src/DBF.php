<?php
declare(strict_types=1);

namespace ndtan;

use PDO;
use PDOException;
use RuntimeException;

/**
 * ============================================================================
 *  NDT DBF — Enterprise+ Single-file Database Framework
 * ============================================================================
 *  Goals
 *  -----
 *  • Single-file, dependency-free (PDO only)
 *  • Secure-by-default (prepared statements, identifier quoting, IN guard)
 *  • Familiar Query Builder with CRUD, Upsert, Aggregates, Pagination (keyset)
 *  • Enterprise+:
 *      - Transactions with deadlock retry (exponential backoff + jitter)
 *      - Readonly/Maintenance mode
 *      - Soft delete with helpers (withTrashed/onlyTrashed/restore/forceDelete)
 *      - Middleware pipeline (audit, trace, multi-tenant scope, etc.)
 *      - Policy hooks (block by table/action/tenant, etc.)
 *      - Metrics & Logger hooks
 *      - Per-query timeout (best-effort per driver)
 *      - Master/Replica routing placeholder (using("read"/"write"))
 *  • Test Mode: build SQL without executing (preview queryString/queryParams)
 *
 *  Public surface area is intentionally compact; implementation is explicit
 *  with docblocks to keep it readable for beginners.
 *
 *  NOTE: This file intentionally includes detailed comments to serve as
 *  documentation lines — many teams prefer “literate source” that is
 *  self-explanatory. If you want a smaller build, you can run a minifier that
 *  removes comments/whitespace to produce a ~700-line core.
 */

// ----------------------------------------------------------------------------
//  Core: DBF (connection, execution, hooks)
// ----------------------------------------------------------------------------

class DBF
{
    /** @var PDO Primary connection (also used for read by default) */
    protected PDO $pdo;

    /** @var PDO|null Optional replica conn if you later extend master/read split */
    protected ?PDO $pdoRead = null;

    /** @var string Active driver type: mysql|pgsql|sqlite|sqlsrv */
    public string $type = 'sqlite';

    /** @var string|null Optional table prefix */
    protected ?string $prefix = null;

    /** @var bool Whether to bypass execution and only build SQL */
    protected bool $testMode = false;

    /** @var bool Whether DML should be blocked (maintenance) */
    protected bool $readonly = false;

    /** @var array<string,mixed> Features flags & options */
    protected array $features = [
        'soft_delete'   => ['enabled' => false, 'column' => 'deleted_at', 'mode' => 'timestamp'],
        'max_in_params' => 1000,
    ];

    /** @var callable|null Logger: function(string $sql, array $params, float $ms): void */
    protected $logger = null;

    /** @var callable|null Metrics: function(array $metrics): void */
    protected $metrics = null;

    /** @var array<int, callable> Middleware pipeline (ctx, next) => result */
    protected array $middlewares = [];

    /** @var array<int, callable> Policy hooks: function(array $ctx): void (throw to block) */
    protected array $policies = [];

    /** @var array<string,mixed> Immutable global scopes added via withScope() */
    private array $scopes = [];

    /** @var array<string,array<string,bool>> Table schema cache by connection */
    private array $schemaCache = [];

    /** @var string Last executed/built SQL (for debugging/test mode) */
    private string $lastQuery = '';

    /** @var array<int,mixed> Last parameters (for debugging/test mode) */
    private array $lastParams = [];

    /** @var string|null Route hint: "read" | "write" | null */
    private ?string $route = null;

    // ---------------------------------------------------------------------
    //  Construction & connections
    // ---------------------------------------------------------------------

    /**
     * @param string|array<string,mixed>|null $config DSN string, array config, or null
     */
    public function __construct($config = null)
    {
        if ($config === null) {
            $env = getenv('NDTAN_DBF_URL');
            if ($env) {
                $config = $env;
            }
        }

        if (is_string($config)) {
            $this->connectFromUri($config);
            return;
        }

        if (is_array($config)) {
            if (isset($config['pdo']) && $config['pdo'] instanceof PDO) {
                $this->pdo  = $config['pdo'];
                $this->type = $config['type'] ?? $this->pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
            } else {
                $this->connectFromArray($config);
            }
            $this->prefix  = $config['prefix'] ?? null;
            $this->readonly = (bool)($config['readonly'] ?? false);
            if (isset($config['features'])) {
                $this->features = array_replace_recursive($this->features, $config['features']);
            }
            return;
        }

        // Defaults to SQLite :memory: for the simplest experience
        $this->pdo = new PDO('sqlite::memory:');
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);
        $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, false);
        $this->type = 'sqlite';
    }

    /** Connect from URI/DSN like mysql://user:pass@host:3306/db?charset=utf8mb4 */
    protected function connectFromUri(string $uri): void
    {
        $parts = parse_url($uri);
        if (!$parts || empty($parts['scheme'])) {
            throw new RuntimeException('Invalid DB URI');
        }
        $this->type = strtolower($parts['scheme']);
        if ($this->type === 'sqlite') {
            $path = $parts['path'] ?? ':memory:';
            if ($path === '/' || $path === '') $path = ':memory:';
            $dsn = 'sqlite:' . (($path === ':memory:') ? ':memory:' : ltrim($path, '/'));
            $this->pdo = new PDO($dsn);
        } else {
            $host = $parts['host'] ?? 'localhost';
            $port = isset($parts['port']) ? (int)$parts['port'] : null;
            $db   = isset($parts['path']) ? ltrim($parts['path'], '/') : '';
            $user = $parts['user'] ?? '';
            $pass = $parts['pass'] ?? '';
            $query= [];
            if (!empty($parts['query'])) parse_str($parts['query'], $query);

            switch ($this->type) {
                case 'mysql':
                case 'mariadb':
                    $charset = $query['charset'] ?? 'utf8mb4';
                    $dsn = "mysql:host={$host}" . ($port?";port={$port}":"") . ";dbname={$db};charset={$charset}";
                    break;
                case 'pgsql':
                    $dsn = "pgsql:host={$host}" . ($port?";port={$port}":"") . ";dbname={$db}";
                    break;
                case 'sqlsrv':
                    $dsn = "sqlsrv:Server={$host}" . ($port?",{$port}":"") . ";Database={$db}";
                    break;
                default:
                    throw new RuntimeException('Unsupported driver: ' . $this->type);
            }
            $this->pdo = new PDO($dsn, $user, $pass);
        }

        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);
        $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, false);
    }

    /** Connect from config array (Medoo-like) */
    protected function connectFromArray(array $cfg): void
    {
        $this->type = strtolower($cfg['type'] ?? 'sqlite');
        if ($this->type === 'sqlite') {
            $db = $cfg['database'] ?? ':memory:';
            if ($db === '' || $db === null) $db = ':memory:';
            $dsn = ($db === ':memory:') ? 'sqlite::memory:' : 'sqlite:' . $db;
            $this->pdo = new PDO($dsn);
        } else {
            $host = $cfg['host'] ?? 'localhost';
            $db   = $cfg['database'] ?? '';
            $user = $cfg['username'] ?? '';
            $pass = $cfg['password'] ?? '';
            $charset = $cfg['charset'] ?? 'utf8mb4';
            switch ($this->type) {
                case 'mysql':
                case 'mariadb':
                    $port = $cfg['port'] ?? 3306;
                    $dsn = "mysql:host={$host};port={$port};dbname={$db};charset={$charset}";
                    break;
                case 'pgsql':
                    $port = $cfg['port'] ?? 5432;
                    $dsn = "pgsql:host={$host};port={$port};dbname={$db}";
                    break;
                case 'sqlsrv':
                    $port = $cfg['port'] ?? null;
                    $dsn = "sqlsrv:Server={$host}" . ($port?",{$port}":"") . ";Database={$db}";
                    break;
                default:
                    throw new RuntimeException('Unsupported driver: ' . $this->type);
            }
            $this->pdo = new PDO($dsn, $user, $pass);
        }
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);
        $this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, false);
    }

    // ---------------------------------------------------------------------
    //  Public config APIs & info
    // ---------------------------------------------------------------------

    public function setTestMode(bool $on): void { $this->testMode = $on; }
    public function queryString(): string { return $this->lastQuery; }
    public function queryParams(): array  { return $this->lastParams; }
    public function setLogger(callable $cb): void { $this->logger = $cb; }
    public function setMetrics(callable $cb): void { $this->metrics = $cb; }
    public function setReadonly(bool $on): void { $this->readonly = $on; }
    public function using(?string $route): self { $clone = clone $this; $clone->route = $route; return $clone; }
    public function policy(callable $cb): void { $this->policies[] = $cb; }
    public function use(callable $mw): void { $this->middlewares[] = $mw; }

    /** Return clone with added global scope (immutable) */
    public function withScope(array $scope): self
    {
        $clone = clone $this;
        $clone->scopes = array_merge($this->scopes, $scope);
        return $clone;
    }

    public function info(): array
    {
        return [
            'driver'     => $this->pdo->getAttribute(PDO::ATTR_DRIVER_NAME),
            'type'       => $this->type,
            'readonly'   => $this->readonly,
            'features'   => $this->features,
            'scopes'     => $this->scopes,
            'test_mode'  => $this->testMode,
            'route'      => $this->route,
        ];
    }

    // ---------------------------------------------------------------------
    //  Query entry & raw
    // ---------------------------------------------------------------------

    public function table(string $name): Builder
    {
        return new Builder($this, $name);
    }

    public function raw(string $sql, array $params = [])
    {
        return $this->execute($sql, $params, 'raw', []);
    }

    // ---------------------------------------------------------------------
    //  Transactions with retry
    // ---------------------------------------------------------------------

    public function tx(callable $fn, int $attempts = 3)
    {
        $last = null;
        for ($i = 1; $i <= $attempts; $i++) {
            try {
                $this->pdo->beginTransaction();
                $res = $fn($this);
                $this->pdo->commit();
                return $res;
            } catch (PDOException $e) {
                $last = $e;
                $code = (int)$e->getCode(); // placeholder for future driver-specific checks
                $this->pdo->rollBack();
                if ($i === $attempts) break;
                usleep((int)(100000 * $i + mt_rand(0, 200000))); // backoff + jitter
            }
        }
        if ($last) throw $last;
        return null;
    }

    // ---------------------------------------------------------------------
    //  Low-level execution with middleware/policy/metrics/logger/timeout
    // ---------------------------------------------------------------------

    /**
     * Execute a prepared statement (or simulate in test mode).
     * @param string               $sql
     * @param array<int,mixed>     $params
     * @param string               $type   select|insert|update|delete|raw|upsert
     * @param array<string,mixed>  $meta   e.g. ['table'=>'users','action'=>'select','timeout_ms'=>1500]
     */
    public function execute(string $sql, array $params, string $type, array $meta)
    {
        $this->lastQuery  = $sql;
        $this->lastParams = $params;

        $ctx = array_merge($meta, [
            'sql'    => $sql,
            'params' => $params,
            'type'   => $type,
            'route'  => $this->route,
        ]);

        // Apply policies (can throw to block)
        foreach ($this->policies as $p) { $p($ctx); }

        // Test mode: do not execute
        if ($this->testMode) {
            return (stripos($sql, 'select') === 0) ? [] : 0;
        }

        // Build executor closure for middleware pipeline
        $executor = function(array $c) {
            $pdo = $this->pdo; // For now route doesn't swap connections
            // Per-query timeout (best-effort)
            $timeout = $c['timeout_ms'] ?? null;
            if ($timeout) {
                $this->applyTimeout((int)$timeout, $pdo);
            }

            $t0 = microtime(true);
            $stmt = $pdo->prepare($c['sql']);
            $ok = $stmt->execute($c['params']);
            $ms = (microtime(true) - $t0) * 1000.0;

            if ($this->metrics) {
                ($this->metrics)([
                    'type' => $c['type'],
                    'table'=> $c['table'] ?? null,
                    'ms'   => $ms,
                    'count'=> $stmt->rowCount(),
                ]);
            }
            if ($this->logger) {
                ($this->logger)($c['sql'], $c['params'], $ms);
            }

            if (!$ok) return 0;
            if (stripos($c['sql'], 'select') === 0) {
                return $stmt->fetchAll(PDO::FETCH_ASSOC);
            }
            return $stmt->rowCount();
        };

        // Wrap with middlewares
        $runner = array_reduce(
            array_reverse($this->middlewares),
            fn($next, $mw) => function(array $c) use ($mw, $next) { return $mw($c, $next); },
            $executor
        );

        return $runner($ctx);
    }

    /** Apply per-query timeout (best-effort by driver) */
    private function applyTimeout(int $ms, PDO $pdo): void
    {
        try {
            switch ($this->type) {
                case 'mysql':
                case 'mariadb':
                    // Applies to SELECTs; UPDATE/INSERT may not obey.
                    $pdo->exec('SET SESSION MAX_EXECUTION_TIME=' . max(1, $ms));
                    break;
                case 'pgsql':
                    // statement_timeout expects milliseconds
                    $pdo->exec('SET LOCAL statement_timeout TO ' . max(1, $ms));
                    break;
                case 'sqlite':
                    // busy_timeout is in milliseconds
                    $pdo->exec('PRAGMA busy_timeout=' . max(1, $ms));
                    break;
                case 'sqlsrv':
                    // Not standardized; rely on PDO::ATTR_TIMEOUT if available
                    @$pdo->setAttribute(PDO::ATTR_TIMEOUT, (int)ceil($ms/1000));
                    break;
            }
        } catch (\Throwable $e) {
            // Ignore timeout tuning errors; query will still run
        }
    }

    // ---------------------------------------------------------------------
    //  Helpers
    // ---------------------------------------------------------------------

    public function getPdo(): PDO { return $this->pdo; }
    public function getType(): string { return $this->type; }
    public function getPrefix(): ?string { return $this->prefix; }
    public function isReadonly(): bool { return $this->readonly; }
    public function getFeatures(): array { return $this->features; }
    public function getScopes(): array { return $this->scopes; }

    /** Quote identifier per driver */
    public function quoteIdent(string $name): string
    {
        switch ($this->type) {
            case 'mysql':
            case 'mariadb':
                return '`' . str_replace('`', '``', $name) . '`';
            case 'sqlsrv':
                return '[' . str_replace(']', ']]', $name) . ']';
            default: // sqlite, pgsql
                return '"' . str_replace('"', '""', $name) . '"';
        }
    }

    /** Cache-aware column presence check */
    public function hasColumn(string $table, string $column): bool
    {
        $tableName = ($this->prefix ?? '') . $table;
        $key = strtolower($this->type . '|' . $tableName);
        if (!isset($this->schemaCache[$key])) {
            $cols = [];
            switch ($this->type) {
                case 'sqlite':
                    $stmt = $this->pdo->query('PRAGMA table_info(' . $tableName . ')');
                    foreach ($stmt->fetchAll(PDO::FETCH_ASSOC) as $r) {
                        $cols[strtolower($r['name'])] = true;
                    }
                    break;
                case 'mysql':
                case 'mariadb':
                    $stmt = $this->pdo->prepare('SHOW COLUMNS FROM ' . $this->quoteIdent($tableName));
                    $stmt->execute();
                    foreach ($stmt->fetchAll(PDO::FETCH_ASSOC) as $r) {
                        $cols[strtolower($r['Field'])] = true;
                    }
                    break;
                case 'pgsql':
                    $stmt = $this->pdo->prepare("SELECT column_name FROM information_schema.columns WHERE table_name = :t");
                    $stmt->execute([':t' => $tableName]);
                    foreach ($stmt->fetchAll(PDO::FETCH_ASSOC) as $r) {
                        $cols[strtolower($r['column_name'])] = true;
                    }
                    break;
                case 'sqlsrv':
                    $stmt = $this->pdo->prepare("SELECT c.name FROM sys.columns c JOIN sys.objects o ON o.object_id=c.object_id WHERE o.name=:t");
                    $stmt->execute([':t' => $tableName]);
                    foreach ($stmt->fetchAll(PDO::FETCH_NUM) as $r) {
                        $cols[strtolower($r[0])] = true;
                    }
                    break;
            }
            $this->schemaCache[$key] = $cols;
        }
        return isset($this->schemaCache[$key][strtolower($column)]);
    }
}

// ----------------------------------------------------------------------------
//  Builder
// ----------------------------------------------------------------------------

class Builder
{
    private DBF $db;
    private string $table;

    /** @var array<int,string> */
    private array $columns = ['*'];

    /**
     *  Each where is [boolean, sql, params]
     *  Example:
     *      ['AND', '"status" = ?', ['active']]
     *      ['OR', '"email" = ?',   ['a@x.com']]
     */
    private array $wheres = [];

    /** @var array<int,array{type:string,table:string,on:string}> */
    private array $joins = [];

    /** @var array<int,string> */
    private array $group = [];

    /** @var array<int,string> */
    private array $having = [];

    /** @var array{0:string,1:string}|null */
    private ?array $order = null;

    private ?int $limit = null;
    private ?int $offset = null;

    private bool $withTrashed = false;
    private bool $onlyTrashed = false;

    private ?string $orderDirTracking = null;
    private array $params = [];

    /** @var int|null Per-query timeout in ms */
    private ?int $timeoutMs = null;

    public function __construct(DBF $db, string $table)
    {
        $this->db = $db;
        $this->table = $table;
    }

    // --------------------- Chainable modifiers ----------------------------

    public function select(array $cols): self { $this->columns = $cols; return $this; }

    public function where(string $col, string $op, $val): self
    {
        $this->wheres[] = ['AND', $this->db->quoteIdent($col) . " {$op} ?", [$val]];
        return $this;
    }
    public function orWhere(string $col, string $op, $val): self
    {
        $this->wheres[] = ['OR', $this->db->quoteIdent($col) . " {$op} ?", [$val]];
        return $this;
    }
    public function whereIn(string $col, array $vals): self
    {
        $max = $this->db->getFeatures()['max_in_params'] ?? 1000;
        if (count($vals) > $max) $vals = array_slice($vals, 0, $max);
        $place = implode(',', array_fill(0, count($vals), '?'));
        $this->wheres[] = ['AND', $this->db->quoteIdent($col) . " IN ({$place})", array_values($vals)];
        return $this;
    }
    public function whereBetween(string $col, array $range): self
    {
        $this->wheres[] = ['AND', $this->db->quoteIdent($col) . " BETWEEN ? AND ?", [$range[0], $range[1]]];
        return $this;
    }
    public function whereNull(string $col): self
    {
        $this->wheres[] = ['AND', $this->db->quoteIdent($col) . " IS NULL", []];
        return $this;
    }

    // Joins
    public function join(string $table, string $left, string $op, string $right, string $type = 'INNER'): self
    {
        $on = $this->db->quoteIdent($left) . " {$op} " . $this->db->quoteIdent($right);
        $this->joins[] = ['type' => strtoupper($type), 'table' => $table, 'on' => $on];
        return $this;
    }
    public function leftJoin(string $table, string $left, string $op, string $right): self
    { return $this->join($table, $left, $op, $right, 'LEFT'); }
    public function rightJoin(string $table, string $left, string $op, string $right): self
    { return $this->join($table, $left, $op, $right, 'RIGHT'); }

    // Group & Having
    public function groupBy(array $cols): self
    {
        foreach ($cols as $c) $this->group[] = $this->db->quoteIdent($c);
        return $this;
    }
    public function having(string $expr, string $op, $val): self
    {
        // Note: $expr may be an aggregate expression string
        $this->having[] = $expr . " {$op} " . (is_numeric($val) ? $val : '?');
        if (!is_numeric($val)) $this->params[] = $val;
        return $this;
    }

    public function orderBy(string $col, string $dir='asc'): self
    {
        $dir = strtolower($dir)==='desc' ? 'desc' : 'asc';
        $this->order = [$this->db->quoteIdent($col), $dir];
        $this->orderDirTracking = $dir;
        return $this;
    }
    public function limit(int $n): self { $this->limit = $n; return $this; }
    public function offset(int $n): self { $this->offset = $n; return $this; }
    public function timeout(int $ms): self { $this->timeoutMs = max(1, $ms); return $this; }

    public function withTrashed(): self { $this->withTrashed = true; return $this; }
    public function onlyTrashed(): self { $this->onlyTrashed = true; return $this; }

    // --------------------- Read API --------------------------------------

    public function get(): array
    {
        [$sql, $params, $meta] = $this->compileSelect();
        return $this->db->execute($sql, $params, 'select', $meta);
    }
    public function first()
    {
        $this->limit(1);
        $rows = $this->get();
        return $rows[0] ?? null;
    }
    public function exists(): bool
    {
        $row = $this->select(['COUNT(1) AS c'])->first();
        return (int)($row['c'] ?? 0) > 0;
    }
    public function count(): int
    {
        $row = $this->select(['COUNT(1) AS c'])->first();
        return (int)($row['c'] ?? 0);
    }

    public function sum(string $col)
    {
        $row = $this->select(["SUM(".$this->db->quoteIdent($col).") AS s"])->first();
        return $row['s'] ?? 0;
    }
    public function avg(string $col)
    {
        $row = $this->select(["AVG(".$this->db->quoteIdent($col).") AS a"])->first();
        return $row['a'] ?? 0;
    }
    public function min(string $col)
    {
        $row = $this->select(["MIN(".$this->db->quoteIdent($col).") AS m"])->first();
        return $row['m'] ?? 0;
    }
    public function max(string $col)
    {
        $row = $this->select(["MAX(".$this->db->quoteIdent($col).") AS m"])->first();
        return $row['m'] ?? 0;
    }

    public function pluck(string $col, ?string $key = null): array
    {
        $rows = $this->select([$this->db->quoteIdent($col) . ($key?','.$this->db->quoteIdent($key):'')])->get();
        if ($key === null) return array_map(fn($r)=>$r[$col] ?? null, $rows);
        $out = [];
        foreach ($rows as $r) {
            $out[$r[$key]] = $r[$col];
        }
        return $out;
    }

    // --------------------- Write API -------------------------------------

    public function insert(array $data): int|string
    {
        $this->assertWritable();
        $cols = array_keys($data);
        $idents = implode(',', array_map([$this->db,'quoteIdent'], $cols));
        $place = implode(',', array_fill(0, count($cols), '?'));
        $sql = "INSERT INTO ".$this->tableName()." ({$idents}) VALUES ({$place})";
        $this->db->execute($sql, array_values($data), 'insert', ['table'=>$this->table,'action'=>'insert','timeout_ms'=>$this->timeoutMs]);
        return $this->db->getPdo()->lastInsertId();
    }

    public function insertMany(array $rows): int
    {
        $this->assertWritable();
        if (empty($rows)) return 0;
        $cols = array_keys($rows[0]);
        $idents = implode(',', array_map([$this->db,'quoteIdent'], $cols));
        $values = [];
        $params = [];
        foreach ($rows as $r) {
            $values[] = '(' . implode(',', array_fill(0, count($cols), '?')) . ')';
            $params = array_merge($params, array_values($r));
        }
        $sql = "INSERT INTO ".$this->tableName()." ({$idents}) VALUES " . implode(',', $values);
        return (int)$this->db->execute($sql, $params, 'insert', ['table'=>$this->table,'action'=>'insert','timeout_ms'=>$this->timeoutMs]);
    }

    public function insertGet(array $data, array $returning = ['*']): array
    {
        $id = $this->insert($data);
        return $this->where('id','=', $id)->select($returning)->first() ?? [];
    }

    public function update(array $data): int
    {
        $this->assertWritable();
        $set = [];
        $params = [];
        foreach ($data as $k=>$v) { $set[] = $this->db->quoteIdent($k) . ' = ?'; $params[] = $v; }
        [$whereSql, $whereParams] = $this->compileWhere();
        $params = array_merge($params, $whereParams);
        $sql = "UPDATE ".$this->tableName()." SET " . implode(', ', $set) . $whereSql;
        return (int)$this->db->execute($sql, $params, 'update', ['table'=>$this->table,'action'=>'update','timeout_ms'=>$this->timeoutMs]);
    }

    public function delete(): int
    {
        $this->assertWritable();
        $feat = $this->db->getFeatures()['soft_delete'] ?? ['enabled'=>false];
        if (!empty($feat['enabled'])) {
            $col = $feat['column'] ?? 'deleted_at';
            if ($this->db->hasColumn($this->table, $col)) {
                $val = ($feat['mode'] ?? 'timestamp') === 'timestamp' ? date('c') : 1;
                return $this->update([$col => $val]);
            }
        }
        [$whereSql, $whereParams] = $this->compileWhere();
        $sql = "DELETE FROM ".$this->tableName().$whereSql;
        return (int)$this->db->execute($sql, $whereParams, 'delete', ['table'=>$this->table,'action'=>'delete','timeout_ms'=>$this->timeoutMs]);
    }

    public function restore(): int
    {
        $feat = $this->db->getFeatures()['soft_delete'] ?? ['enabled'=>false];
        if (empty($feat['enabled'])) return 0;
        $col = $feat['column'] ?? 'deleted_at';
        if (!$this->db->hasColumn($this->table, $col)) return 0;
        return $this->update([$col => null]);
    }
    public function forceDelete(): int
    {
        [$whereSql, $whereParams] = $this->compileWhere();
        $sql = "DELETE FROM ".$this->tableName().$whereSql;
        return (int)$this->db->execute($sql, $whereParams, 'delete', ['table'=>$this->table,'action'=>'delete','timeout_ms'=>$this->timeoutMs]);
    }

    public function upsert(array $data, array $conflict, array $updateColumns): int
    {
        $this->assertWritable();
        $cols = array_keys($data);
        $idents = implode(',', array_map([$this->db,'quoteIdent'], $cols));
        $place  = implode(',', array_fill(0, count($cols), '?'));
        $params = array_values($data);
        $table  = $this->tableName();

        switch ($this->db->getType()) {
            case 'sqlite':
            case 'pgsql':
                $conf = implode(',', array_map([$this->db,'quoteIdent'], $conflict));
                $set  = implode(', ', array_map(fn($c)=>$this->db->quoteIdent($c)."=excluded.".$this->db->quoteIdent($c), $updateColumns));
                $sql  = "INSERT INTO {$table} ({$idents}) VALUES ({$place}) ON CONFLICT ({$conf}) DO UPDATE SET {$set}";
                return (int)$this->db->execute($sql, $params, 'upsert', ['table'=>$this->table,'action'=>'upsert','timeout_ms'=>$this->timeoutMs]);
            case 'mysql':
            case 'mariadb':
                $set  = implode(', ', array_map(fn($c)=>$this->db->quoteIdent($c)."=VALUES(".$this->db->quoteIdent($c).")", $updateColumns));
                $sql  = "INSERT INTO {$table} ({$idents}) VALUES ({$place}) ON DUPLICATE KEY UPDATE {$set}";
                return (int)$this->db->execute($sql, $params, 'upsert', ['table'=>$this->table,'action'=>'upsert','timeout_ms'=>$this->timeoutMs]);
            case 'sqlsrv':
            default:
                // Fallback two-phase upsert
                return (int)$this->db->tx(function() use ($data, $conflict, $updateColumns) {
                    try { $this->insert($data); return 1; }
                    catch (\Throwable $e) {
                        $w = []; $p = []; foreach ($conflict as $c) { $w[] = $this->db->quoteIdent($c).' = ?'; $p[] = $data[$c]; }
                        $upd = [];          foreach ($updateColumns as $c) { $upd[] = $this->db->quoteIdent($c).' = ?'; $p[] = $data[$c]; }
                        $sql = "UPDATE ".$this->tableName()." SET ".implode(', ',$upd)." WHERE ".implode(' AND ', $w);
                        return (int)$this->db->execute($sql, $p, 'update', ['table'=>$this->table,'action'=>'upsert','timeout_ms'=>$this->timeoutMs]);
                    }
                });
        }
    }

    // --------------------- Keyset Pagination ------------------------------

    public function getKeyset($cursor, string $key): array
    {
        $dir = strtolower($this->orderDirTracking ?? 'asc');
        $op  = ($dir === 'asc') ? '>' : '<';
        if ($cursor !== null) {
            $this->where($key, $op, $cursor);
        }
        $rows = $this->get();
        $next = null;
        if (!empty($rows)) {
            $last = end($rows);
            if (isset($last[$key])) $next = $last[$key];
        }
        return ['data' => $rows, 'next' => $next];
    }

    // --------------------- Compilation -----------------------------------

    private function compileSelect(): array
    {
        $table = $this->tableName();

        $cols  = implode(', ', array_map(function($c){
            return (strpos($c, '(')!==false || $c==='*') ? $c : $this->db->quoteIdent($c);
        }, $this->columns));

        $sql = "SELECT {$cols} FROM {$table}";

        // Joins
        foreach ($this->joins as $j) {
            $sql .= ' ' . $j['type'] . ' JOIN ' . $this->db->quoteIdent($j['table']) . ' ON ' . $j['on'];
        }

        // WHERE
        [$whereSql, $whereParams] = $this->compileWhere();
        $sql .= $whereSql;

        // Group/Having
        if (!empty($this->group)) { $sql .= ' GROUP BY ' . implode(', ', $this->group); }
        if (!empty($this->having)) { $sql .= ' HAVING ' . implode(' AND ', $this->having); }

        // Order/Limit/Offset
        if (!empty($this->order)) {
            $sql .= ' ORDER BY ' . $this->order[0] . ' ' . strtoupper($this->order[1]);
        }
        if ($this->limit !== null)  $sql .= ' LIMIT ' . (int)$this->limit;
        if ($this->offset !== null) $sql .= ' OFFSET ' . (int)$this->offset;

        $meta = ['table'=>$this->table,'action'=>'select','timeout_ms'=>$this->timeoutMs];
        return [$sql, $whereParams, $meta];
    }

    private function compileWhere(): array
    {
        $parts = [];
        $params= [];

        // Global immutable scopes
        foreach ($this->db->getScopes() as $k=>$v) {
            $parts[] = $this->db->quoteIdent($k) . ' = ?';
            $params[] = $v;
        }

        // Builder wheres
        $first = true;
        foreach ($this->wheres as [$bool, $sql, $p]) {
            if ($first) { $parts[] = $sql; $first=false; }
            else        { $parts[] = $bool . ' ' . $sql; }
            $params = array_merge($params, $p);
        }

        // Soft delete guard (only if column exists)
        $feat = $this->db->getFeatures()['soft_delete'] ?? ['enabled'=>false];
        if (!empty($feat['enabled'])) {
            $col = $feat['column'] ?? 'deleted_at';
            if ($this->db->hasColumn($this->table, $col)) {
                if ($this->onlyTrashed) {
                    $parts[] = $this->db->quoteIdent($col) . ' IS NOT NULL';
                } elseif (!$this->withTrashed) {
                    $parts[] = $this->db->quoteIdent($col) . ' IS NULL';
                }
            }
        }

        if (empty($parts)) return ['', []];
        $sql = ' WHERE ' . implode(' AND ', $parts);
        return [$sql, $params];
    }

    // --------------------- Utilities -------------------------------------

    private function tableName(): string
    {
        $p = $this->db->getPrefix();
        return $this->db->quoteIdent(($p ? $p : '') . $this->table);
    }

    private function assertWritable(): void
    {
        if ($this->db->isReadonly()) {
            throw new RuntimeException('Database is in readonly mode');
        }
    }
}
