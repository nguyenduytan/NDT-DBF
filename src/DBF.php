<?php
/**
 * NDT DBF - Simple, Lightweight PHP Database Framework (Enterprise+)
 *
 * @version   0.3.0
 * @package   NDT DBF
 * @description Single-file, secure PHP Database Framework with PRO & Advanced++ features.
 * @author    Tony Nguyen
 * @link      https://ndtan.net
 * @license   MIT
 *
 * Highlights:
 * - Single file, zero external deps (PDO required)
 * - Secure by default: prepared statements, identifier quoting per driver, IN-guard, policy/scope
 * - PRO: Query Builder (select/where/join/group/having/order/limit/offset), raw SQL (positional & named)
 * - PRO: Transactions with exponential backoff on deadlocks
 * - PRO: Upsert (MySQL/PG/SQLite) + fallback, insertMany, insertGet (RETURNING on PG/SQLite)
 * - PRO: Keyset pagination helper
 * - PRO: Logger & middleware pipeline; Metrics hook
 * - PRO: Master/Replica routing (auto/manual)
 * - Advanced++: Readonly/Maintenance mode; Soft Delete guard (withTrashed/onlyTrashed/restore/forceDelete)
 * - Advanced++: Per-query timeout (MySQL/PG best-effort)
 * - NEW (PRO parity): Aggregates helpers sum/avg/min/max + pluck
 * - NEW (DX): Test Mode ($db->setTestMode(true)) + last query string/params getters
 *
 * Quickstart:
 *   require 'DBF.php';
 *   $db = new \ndtan\DBF('mysql://user:pass@localhost/app?charset=utf8mb4');
 *   $rows = $db->table('users')->select(['id','email'])->where('status','=','active')->get();
 */

namespace ndtan;

use PDO;
use PDOStatement;
use Throwable;

final class DBF
{
    private PDO $pdoWrite;
    private ?PDO $pdoRead = null;
    private string $driverWrite;
    private ?string $driverRead = null;
    private string $prefix = '';
    private bool $readonly = false;

    /** @var callable|null function(string $sql, array $params, float $ms): void */
    private $logger = null;
    /** @var callable|null function(array $metrics): void */
    private $metrics = null;

    /** @var array<int, callable> Middlewares: function(array $ctx, callable $next): mixed */
    private array $middlewares = [];

    /** @var array<string,mixed> Default scope applied to WHERE */
    private array $scope = [];

    /** @var callable|null Policy hook: function(array $ctx): void; throw to block */
    private $policy = null;

    /** @var string Routing mode: 'single'|'auto'|'manual' */
    private string $routing = 'single';

    /** @var string Current manual route when routing='manual': 'write'|'read' */
    private string $currentRoute = 'write';

    /** @var int Maximum items allowed in WHERE IN (guard) */
    private int $maxInParams = 1000;

    /** @var array Statement cache */
    private array $stmtCache = [];

    /** @var array Soft delete configuration */
    private array $softDelete = [
        'enabled' => false,
        'column'  => 'deleted_at', // or 'is_deleted'
        'mode'    => 'timestamp',  // 'timestamp' | 'boolean'
        'deleted_value' => 1,      // for boolean mode
    ];

    /** --- Test Mode & last query tracking --- */
    private bool $testMode = false;
    private string $lastQueryString = '';
    private array  $lastQueryParams = [];

    /**
     * Create a DBF instance.
     *
     * @param string|array|null $configOrUri
     * - string: URI like "mysql://user:pass@host:port/db?charset=utf8mb4"
     * - array:  Medoo-like config or advanced options (see README)
     */
    public function __construct(string|array|null $configOrUri = null)
    {
        if ($configOrUri === null) {
            $env = getenv('NDTAN_DBF_URL');
            if (!$env) throw new \InvalidArgumentException('No configuration provided. Pass URI/array/PDO or set NDTAN_DBF_URL.');
            $configOrUri = $env;
        }

        if (is_string($configOrUri)) {
            [$pdo, $driver] = $this->connectFromUri($configOrUri);
            $this->pdoWrite = $pdo;
            $this->driverWrite = $driver;
            $this->routing = 'single';
        } elseif (is_array($configOrUri)) {
            if (isset($configOrUri['write']) || isset($configOrUri['read'])) {
                $this->initMasterReplica($configOrUri);
            } else {
                [$pdo, $driver] = $this->connectFromArray($configOrUri);
                $this->pdoWrite = $pdo;
                $this->driverWrite = $driver;
                $this->routing = 'single';
            }
            // Global options
            if (isset($configOrUri['prefix'])) $this->prefix = (string)$configOrUri['prefix'];
            if (!empty($configOrUri['readonly'])) $this->readonly = (bool)$configOrUri['readonly'];
            if (isset($configOrUri['logging']) && $configOrUri['logging']) {
                $this->logger ??= function(string $sql, array $params, float $ms) {
                    error_log(sprintf('[SQL %.2fms] %s | %s', $ms, $sql, json_encode($params)));
                };
            }
            if (isset($configOrUri['logger']) && is_callable($configOrUri['logger'])) $this->logger = $configOrUri['logger'];
            if (isset($configOrUri['metrics']) && is_callable($configOrUri['metrics'])) $this->metrics = $configOrUri['metrics'];
            if (isset($configOrUri['features']['max_in_params'])) $this->maxInParams = max(1, (int)$configOrUri['features']['max_in_params']);

            // Soft delete feature
            if (!empty($configOrUri['features']['soft_delete'])) {
                $sd = $configOrUri['features']['soft_delete'];
                $this->softDelete['enabled'] = (bool)($sd['enabled'] ?? true);
                $this->softDelete['column']  = (string)($sd['column']  ?? 'deleted_at');
                $this->softDelete['mode']    = (string)($sd['mode']    ?? 'timestamp');
                $this->softDelete['deleted_value'] = $sd['deleted_value'] ?? 1;
            }

            // Test Mode
            if (!empty($configOrUri['testMode'])) {
                $this->testMode = (bool)$configOrUri['testMode'];
            }

            // Post-connect commands
            if (isset($configOrUri['command']) && is_array($configOrUri['command'])) {
                foreach ($configOrUri['command'] as $cmd) {
                    $this->pdoWrite->exec($cmd);
                    if ($this->pdoRead) { $this->pdoRead->exec($cmd); }
                }
            }
        } else {
            throw new \InvalidArgumentException('Unsupported configuration type.');
        }
    }

    /** Manual route (when routing='manual'). */
    public function using(string $route): self
    {
        $route = strtolower($route);
        if ($route !== 'read' && $route !== 'write') $route = 'write';
        $clone = clone $this;
        $clone->currentRoute = $route;
        return $clone;
    }

    /** Set or toggle readonly (maintenance) mode. */
    public function setReadonly(bool $state): self { $this->readonly = $state; return $this; }

    /** Enable/disable Test Mode. When ON, DBF will NOT execute queries. */
    public function setTestMode(bool $on = true): self { $this->testMode = $on; return $this; }
    public function isTestMode(): bool { return $this->testMode; }
    /** Last built SQL & params (for debugging / test mode) */
    public function queryString(): string { return $this->lastQueryString; }
    public function queryParams(): array  { return $this->lastQueryParams; }

    /** Assign a logger (overwrites). */
    public function setLogger(callable $logger): self { $this->logger = $logger; return $this; }

    /** Assign a metrics hook: function(array $metrics) */
    public function setMetrics(callable $metrics): self { $this->metrics = $metrics; return $this; }

    /** Add a middleware: function(array $ctx, callable $next): mixed */
    public function use(callable $middleware): self { $this->middlewares[] = $middleware; return $this; }

    /** Set default scope (tenant) for new queries. */
    public function withScope(array $scope): self { $clone = clone $this; $clone->scope = $scope; return $clone; }

    /** Set policy hook: function(array $ctx): void */
    public function policy(callable $policy): self { $this->policy = $policy; return $this; }

    /** Info for debugging */
    public function info(): array
    {
        return [
            'routing' => $this->routing,
            'driver_write' => $this->driverWrite,
            'driver_read' => $this->driverRead,
            'prefix' => $this->prefix,
            'readonly' => $this->readonly,
            'soft_delete' => $this->softDelete,
            'test_mode' => $this->testMode,
        ];
    }

    /** Query builder bound to a table. */
    public function table(string $table): Query
    {
        return new Query($this, $table, $this->scope, $this->softDelete);
    }

    /** Raw SQL with params (positional or named). $options: ['timeout_ms'=>int] */
    public function raw(string $sql, array $params = [], array $options = []): array|int
    {
        $type = $this->detectSqlType($sql);
        if ($this->readonly && $this->isWriteType($type)) {
            throw new \RuntimeException('Readonly mode: write operation blocked.');
        }
        $pdo = $this->choosePdo($type);
        $timeoutMs = (int)($options['timeout_ms'] ?? 0);

        // Test mode short-circuit
        if ($this->testMode) {
            $this->lastQueryString = $sql;
            $this->lastQueryParams = $params;
            if ($this->logger) { ($this->logger)($sql, $params, 0.0); }
            // Simulate result shapes
            return $this->isSelectType($type) ? [] : 0;
        }

        $ctx = ['sql'=>$sql,'params'=>$params,'type'=>$type,'table'=>null,'action'=>'raw','route'=>$pdo === $this->pdoRead ? 'read':'write','timeout_ms'=>$timeoutMs];
        $runner = $this->buildRunner(function($ctx) use ($pdo) {
            [$stmt, $ms] = $this->execPreparedOn($pdo, $ctx['sql'], $ctx['params'], $ctx['timeout_ms']);
            $isSelect = $this->isSelectType($ctx['type']);
            $res = $isSelect ? $stmt->fetchAll() : $stmt->rowCount();
            $this->emitMetrics($ctx, $ms, $isSelect ? count($res) : $res);
            return $res;
        });
        return $runner($ctx);
    }

    /** Transaction helper with exponential backoff retry for deadlocks. */
    public function tx(callable $fn, int $attempts = 1): mixed
    {
        $attempts = max(1, $attempts);
        $base = 50000; // 50ms
        for ($try = 1; $try <= $attempts; $try++) {
            $this->pdoWrite->beginTransaction();
            try {
                $result = $fn($this);
                $this->pdoWrite->commit();
                return $result;
            } catch (Throwable $e) {
                $this->pdoWrite->rollBack();
                if ($this->isDeadlock($e) && $try < $attempts) {
                    $sleep = $base * (1 << ($try-1)); // exponential
                    $sleep += random_int(0, 20000);   // jitter up to 20ms
                    usleep($sleep);
                    continue;
                }
                throw $e;
            }
        }
        return null;
    }

    // ===== Internals =====================================================

    private function initMasterReplica(array $cfg): void
    {
        $write = $cfg['write'] ?? null;
        if (!$write) throw new \InvalidArgumentException('Master/Replica config requires "write".');
        if (is_string($write)) { [$this->pdoWrite, $this->driverWrite] = $this->connectFromUri($write); }
        elseif (is_array($write)) { [$this->pdoWrite, $this->driverWrite] = $this->connectFromArray($write); }
        else throw new \InvalidArgumentException('"write" config must be string URI or array.');

        $read = $cfg['read'] ?? null;
        if ($read) {
            if (is_string($read)) { [$this->pdoRead, $this->driverRead] = $this->connectFromUri($read); }
            elseif (is_array($read)) { [$this->pdoRead, $this->driverRead] = $this->connectFromArray($read); }
            else throw new \InvalidArgumentException('"read" config must be string URI or array.');
        }

        $this->routing = strtolower($cfg['routing'] ?? ($this->pdoRead ? 'auto' : 'single'));
        if (!in_array($this->routing, ['auto','manual','single'], true)) $this->routing = $this->pdoRead ? 'auto' : 'single';

        if (isset($cfg['prefix'])) $this->prefix = (string)$cfg['prefix'];
        if (!empty($cfg['readonly'])) $this->readonly = (bool)$cfg['readonly'];
        if (isset($cfg['logger']) && is_callable($cfg['logger'])) $this->logger = $cfg['logger'];
        if (isset($cfg['logging']) && $cfg['logging']) {
            $this->logger ??= function(string $sql, array $params, float $ms) {
                error_log(sprintf('[SQL %.2fms] %s | %s', $ms, $sql, json_encode($params)));
            };
        }
        if (isset($cfg['metrics']) && is_callable($cfg['metrics'])) $this->metrics = $cfg['metrics'];
        if (!empty($cfg['testMode'])) $this->testMode = (bool)$cfg['testMode'];
    }

    private function connectFromUri(string $uri): array
    {
        $parts = parse_url($uri);
        if ($parts === false || !isset($parts['scheme'])) throw new \InvalidArgumentException("Invalid DB URI: $uri");

        $scheme = strtolower($parts['scheme']);
        $user = $parts['user'] ?? null;
        $pass = $parts['pass'] ?? null;
        $host = $parts['host'] ?? null;
        $port = $parts['port'] ?? null;
        $path = $parts['path'] ?? null;
        $db   = $path ? ltrim($path, '/') : null;
        $query= [];
        if (isset($parts['query'])) parse_str($parts['query'], $query);

        $options = [
            PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::ATTR_EMULATE_PREPARES   => false,
        ];

        switch ($scheme) {
            case 'mysql':
                $charset = $query['charset'] ?? 'utf8mb4';
                $collation = $query['collation'] ?? null;
                $dsn = "mysql:host={$host}".($port?";port={$port}":"").";dbname={$db};charset={$charset}";
                $pdo = new PDO($dsn, $user, $pass, $options);
                if ($collation) $pdo->exec("SET NAMES '{$charset}' COLLATE '{$collation}'");
                return [$pdo, 'mysql'];
            case 'pgsql':
                $dsn = "pgsql:host={$host}".($port?";port={$port}":"").";dbname={$db}";
                $pdo = new PDO($dsn, $user, $pass, $options);
                if (isset($query['charset'])) $pdo->exec("SET client_encoding TO '{$query['charset']}'");
                return [$pdo, 'pgsql'];
            case 'sqlite':
                $dbPath = $db ?: ($path ?? ':memory:');
                if ($dbPath === '' || $dbPath === '/') $dbPath = ':memory:';
                if (str_starts_with($uri, 'sqlite:///')) { $dbPath = substr($uri, strlen('sqlite:///')); }
                elseif ($dbPath === ':memory:' || $dbPath === 'memory') { $dbPath=':memory:'; }
                $dsn = "sqlite:{$dbPath}";
                $pdo = new PDO($dsn, null, null, $options);
                return [$pdo, 'sqlite'];
            case 'sqlsrv':
                $dsn = "sqlsrv:Server={$host}".($port?",{$port}":"").($db?";Database={$db}":"");
                $pdo = new PDO($dsn, $user, $pass, $options);
                return [$pdo, 'sqlsrv'];
            default:
                throw new \InvalidArgumentException("Unsupported scheme: {$scheme}");
        }
    }

    private function connectFromArray(array $cfg): array
    {
        if (isset($cfg['pdo']) && $cfg['pdo'] instanceof PDO) {
            $pdo = $cfg['pdo'];
            $driver = (string)$pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
            return [$pdo, $driver];
        }

        $options = $cfg['option'] ?? [
            PDO::ATTR_ERRMODE            => $cfg['error'] ?? PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::ATTR_EMULATE_PREPARES   => false,
        ];

        if (isset($cfg['dsn'])) {
            $pdo = new PDO($cfg['dsn'], $cfg['username'] ?? null, $cfg['password'] ?? null, $options);
            $driver = $cfg['type'] ?? (string)$pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
            return [$pdo, $driver];
        }

        $type = strtolower((string)($cfg['type'] ?? 'mysql'));
        $user = $cfg['username'] ?? null;
        $pass = $cfg['password'] ?? null;
        $host = $cfg['host'] ?? 'localhost';
        $port = $cfg['port'] ?? null;
        $db   = $cfg['database'] ?? null;
        $charset = $cfg['charset'] ?? 'utf8mb4';
        if ($type === 'mariadb') $type = 'mysql';

        switch ($type) {
            case 'mysql':
                if (!empty($cfg['socket'])) {
                    $dsn = "mysql:unix_socket={$cfg['socket']};dbname={$db};charset={$charset}";
                } else {
                    $dsn = "mysql:host={$host}".($port?";port={$port}":"").";dbname={$db};charset={$charset}";
                }
                $pdo = new PDO($dsn, $user, $pass, $options);
                if (!empty($cfg['collation'])) $pdo->exec("SET NAMES '{$charset}' COLLATE '{$cfg['collation']}'");
                return [$pdo, 'mysql'];
            case 'pgsql':
                $dsn = "pgsql:host={$host}".($port?";port={$port}":"").";dbname={$db}";
                $pdo = new PDO($dsn, $user, $pass, $options);
                return [$pdo, 'pgsql'];
            case 'sqlite':
                $dbPath = $db ?: ($cfg['database'] ?? ':memory:');
                if ($dbPath === '' || $dbPath === '/') $dbPath = ':memory:';
                $dsn = "sqlite:{$dbPath}";
                $pdo = new PDO($dsn, null, null, $options);
                return [$pdo, 'sqlite'];
            case 'sqlsrv':
                $dsn = "sqlsrv:Server={$host}".($port?",{$port}":"").($db?";Database={$db}":"");
                $pdo = new PDO($dsn, $user, $pass, $options);
                return [$pdo, 'sqlsrv'];
            default:
                throw new \InvalidArgumentException("Unsupported type: {$type}");
        }
    }

    public function choosePdo(string $sqlType): PDO
    {
        if ($this->routing === 'single' || !$this->pdoRead) return $this->pdoWrite;
        if ($this->routing === 'manual') {
            return $this->currentRoute === 'read' ? $this->pdoRead : $this->pdoWrite;
        }
        return $this->isSelectType($sqlType) ? ($this->pdoRead ?? $this->pdoWrite) : $this->pdoWrite;
    }

    /** Execute prepared with per-query timeout and logging */
    private function execPreparedOn(PDO $pdo, string $sql, array $params = [], int $timeoutMs = 0): array
    {
        $driver = (string)$pdo->getAttribute(PDO::ATTR_DRIVER_NAME);

        // Track last query
        $this->lastQueryString = $sql;
        $this->lastQueryParams = $params;

        // Per-query timeout (best-effort)
        $resetSql = null;
        if ($timeoutMs > 0) {
            if ($driver === 'mysql') {
                $pdo->exec("SET SESSION MAX_EXECUTION_TIME={$timeoutMs}");
                $resetSql = "SET SESSION MAX_EXECUTION_TIME=0";
            } elseif ($driver === 'pgsql') {
                $inTx = $pdo->inTransaction();
                if ($inTx) { @$pdo->exec("SET LOCAL statement_timeout = '{$timeoutMs}ms'"); }
                else { @$pdo->exec("SET statement_timeout TO '{$timeoutMs}ms'"); $resetSql = "SET statement_timeout TO DEFAULT"; }
            } elseif ($driver === 'sqlsrv') {
                @$pdo->exec("SET LOCK_TIMEOUT {$timeoutMs}");
            }
        }

        $t0 = microtime(true);
        $stmt = $this->prepareCached($pdo, $sql, $params);
        $this->bindAll($stmt, $params);
        $stmt->execute();
        $ms = (microtime(true) - $t0) * 1000.0;

        if ($resetSql) { @$pdo->exec($resetSql); }

        if ($this->logger) { ($this->logger)($sql, $params, $ms); }
        return [$stmt, $ms];
    }

    private function prepareCached(PDO $pdo, string $sql, array $params): PDOStatement
    {
        $driver = (string)$pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
        $isNamed = $this->isAssoc($params);
        $key = $driver.'|'.($isNamed?'N|':'P|').$sql;
        if (!isset($this->stmtCache[$key])) {
            $this->stmtCache[$key] = ['stmt'=>$pdo->prepare($sql)];
        }
        return $this->stmtCache[$key]['stmt'];
    }

    private function bindAll(PDOStatement $stmt, array $params): void
    {
        if ($this->isAssoc($params)) {
            foreach ($params as $name => $value) {
                $param = is_string($name) && $name !== '' && $name[0] !== ':' ? ':'.$name : $name;
                $stmt->bindValue($param, $value);
            }
        } else {
            foreach (array_values($params) as $i => $v) { $stmt->bindValue($i+1, $v); }
        }
    }

    private function isAssoc(array $arr): bool { return $arr !== [] && array_keys($arr) !== range(0, count($arr)-1); }

    public function qi(string $ident, ?PDO $pdo = null): string
    {
        $pdo ??= $this->pdoWrite;
        $driver = (string)$pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
        $parts = explode('.', $ident);
        $quoted = array_map(function($p) use ($driver) {
            return match ($driver) {
                'mysql'  => '`'.str_replace('`','``',$p).'`',
                'pgsql', 'sqlite' => '"'.str_replace('"','""',$p).'"',
                'sqlsrv' => '['.str_replace(']',']]',$p).']',
                default  => '"'.str_replace('"','""',$p).'"',
            };
        }, $parts);
        return implode('.', $quoted);
    }

    public function compileLimitOffset(?int $limit, ?int $offset, ?PDO $pdo = null): string
    {
        $pdo ??= $this->pdoWrite;
        $driver = (string)$pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
        if ($limit === null && $offset === null) return '';
        return match ($driver) {
            'mysql'  => ($limit !== null ? " LIMIT {$limit}" : " LIMIT 18446744073709551615").($offset !== null ? " OFFSET {$offset}" : ''),
            'pgsql', 'sqlite' => ($limit !== null ? " LIMIT {$limit}" : '').($offset !== null ? " OFFSET {$offset}" : ''),
            'sqlsrv' => ($limit !== null ? " OFFSET ".(int)($offset ?? 0)." ROWS FETCH NEXT {$limit} ROWS ONLY" : ''),
            default  => ($limit !== null ? " LIMIT {$limit}" : '').($offset !== null ? " OFFSET {$offset}" : ''),
        };
    }

    private function detectSqlType(string $sql): string
    {
        $trim = ltrim($sql);
        if (preg_match('/^(WITH|SELECT|SHOW|PRAGMA)\b/i', $trim)) return 'select';
        if (preg_match('/^(INSERT)\b/i', $trim)) return 'insert';
        if (preg_match('/^(UPDATE)\b/i', $trim)) return 'update';
        if (preg_match('/^(DELETE)\b/i', $trim)) return 'delete';
        return 'other';
    }
    private function isSelectType(string $type): bool { return $type === 'select'; }
    private function isWriteType(string $type): bool { return in_array($type, ['insert','update','delete'], true); }

    private function isDeadlock(Throwable $e): bool
    {
        $msg = strtolower($e->getMessage());
        $code = (int)($e->getCode() ?: 0);
        if (str_contains($msg, 'deadlock')) return true;
        if ($code === 1213 || $code === 40001) return true; // MySQL / PG serialization
        return false;
    }

    private function buildRunner(callable $core): callable
    {
        $runner = array_reduce(
            array_reverse($this->middlewares),
            function($next, $mw) { return function($ctx) use ($mw, $next) { return $mw($ctx, $next); }; },
            $core
        );
        return function($ctx) use ($runner) {
            if ($this->policy) { ($this->policy)($ctx); }
            return $runner($ctx);
        };
    }

    private function emitMetrics(array $ctx, float $ms, int $count): void
    {
        if ($this->metrics) {
            ($this->metrics)([
                'type'  => $ctx['type'],
                'table' => $ctx['table'],
                'route' => $ctx['route'] ?? 'write',
                'ms'    => $ms,
                'count' => $count,
                'time'  => microtime(true),
            ]);
        }
    }

    public function guardMaxIn(): int { return $this->maxInParams; }
    public function getPrefix(): string { return $this->prefix; }
    public function isReadonly(): bool { return $this->readonly; }
    public function getSoftDelete(): array { return $this->softDelete; }
}

// ===== Query Builder =======================================================

final class Query
{
    private DBF $db;
    private string $table;
    private array $scope;
    private array $softDelete;

    private ?array $columns = ['*'];
    private array $wheres = [];
    private array $joins = [];
    private array $groupBys = [];
    private array $havings = [];
    private array $orderBys = [];
    private ?int $limit = null;
    private ?int $offset = null;
    private int $timeoutMs = 0;

    private bool $withTrashed = false;
    private bool $onlyTrashed = false;

    public function __construct(DBF $db, string $table, array $scope = [], array $softDelete = [])
    {
        $this->db = $db; $this->table = $table; $this->scope = $scope; $this->softDelete = $softDelete;
    }

    public function select(array|string $cols = '*'): self { $this->columns = is_array($cols) ? $cols : ['*']; return $this; }
    public function where(string|callable $col, ?string $op = null, mixed $val = null, string $bool = 'AND'): self
    {
        if (is_callable($col)) { $nested = new self($this->db, $this->table, $this->scope, $this->softDelete); $col($nested); $this->wheres[]=['type'=>'group','bool'=>$bool,'query'=>$nested]; return $this; }
        $this->wheres[]=['type'=>'basic','bool'=>$bool,'col'=>$col,'op'=>$op ?? '=','val'=>$val]; return $this;
    }
    public function orWhere(string|callable $col, ?string $op=null, mixed $val=null): self { return $this->where($col,$op,$val,'OR'); }
    public function whereIn(string $col, array $values, string $bool='AND', bool $not=false): self { $this->wheres[]=['type'=>'in','bool'=>$bool,'col'=>$col,'vals'=>$values,'not'=>$not]; return $this; }
    public function whereNotIn(string $col, array $values, string $bool='AND'): self { return $this->whereIn($col,$values,$bool,true); }
    public function whereNull(string $col, string $bool='AND', bool $not=false): self { $this->wheres[]=['type'=>'null','bool'=>$bool,'col'=>$col,'not'=>$not]; return $this; }
    public function whereNotNull(string $col, string $bool='AND'): self { return $this->whereNull($col,$bool,true); }
    public function whereBetween(string $col, array $pair, string $bool='AND', bool $not=false): self { $this->wheres[]=['type'=>'between','bool'=>$bool,'col'=>$col,'pair'=>$pair,'not'=>$not]; return $this; }

    public function join(string $table, string $left, string $op, string $right, string $type='INNER', ?string $alias=null): self
    { $this->joins[]=['table'=>$table,'alias'=>$alias,'on'=>['l'=>$left,'op'=>$op,'r'=>$right],'type'=>strtoupper($type)]; return $this; }
    public function leftJoin(string $table, string $l, string $op, string $r, ?string $alias=null): self { return $this->join($table,$l,$op,$r,'LEFT',$alias); }
    public function rightJoin(string $table, string $l, string $op, string $r, ?string $alias=null): self { return $this->join($table,$l,$op,$r,'RIGHT',$alias); }

    public function groupBy(array $cols): self { $this->groupBys = $cols; return $this; }
    public function having(string $expr, string $op, mixed $val, string $bool='AND'): self { $this->havings[]=['type'=>'basic','bool'=>$bool,'expr'=>$expr,'op'=>$op,'val'=>$val]; return $this; }
    public function orderBy(string $col, string $dir='asc'): self { $this->orderBys[] = [$col, strtoupper($dir)==='DESC'?'DESC':'ASC']; return $this; }
    public function limit(int $n): self { $this->limit = $n; return $this; }
    public function offset(int $n): self { $this->offset = $n; return $this; }
    public function timeout(int $ms): self { $this->timeoutMs = max(0,$ms); return $this; }

    public function withTrashed(): self { $this->withTrashed = true; $this->onlyTrashed = false; return $this; }
    public function onlyTrashed(): self { $this->onlyTrashed = true; $this->withTrashed = false; return $this; }

    // === Reads ===
    public function get(): array
    {
        [$sql, $bind, $route] = $this->compileSelect();
        // Test mode short-circuit
        if ($this->isTestMode()) { $this->storeLast($sql, $bind); return []; }

        $pdo = $this->db->choosePdo('select');
        $ctx = ['sql'=>$sql,'params'=>$bind,'type'=>'select','table'=>$this->table,'action'=>'select','route'=>$route,'timeout_ms'=>$this->timeoutMs];
        $runner = $this->dbRunner(function($ctx) use ($pdo) {
            [$stmt, $ms] = $this->dbExec($pdo, $ctx['sql'], $ctx['params'], $ctx['timeout_ms']);
            $rows = $stmt->fetchAll();
            $this->dbEmit($ctx, $ms, count($rows));
            return $rows;
        });
        return $runner($ctx);
    }

    public function first(): ?array { $this->limit ??= 1; $rows = $this->get(); return $rows[0] ?? null; }
    public function exists(): bool { $orig = $this->columns; $this->columns=['1']; $this->limit=1; $rows=$this->get(); $this->columns=$orig; return !empty($rows); }
    public function count(): int { $orig=$this->columns; $this->columns=['COUNT(*)']; $rows=$this->get(); $this->columns=$orig; $val=$rows[0]??null; $first=$val?array_values($val)[0]:0; return (int)$first; }

    /** Aggregates (sugar) */
    public function sum(string $col): float|int
    {
        $alias = '_sum';
        $res = $this->aggregate("SUM", $col, $alias);
        return $this->num($res[$alias] ?? 0);
    }
    public function avg(string $col): float
    {
        $alias = '_avg';
        $res = $this->aggregate("AVG", $col, $alias);
        return (float)($res[$alias] ?? 0.0);
    }
    public function min(string $col): float|int|string
    {
        $alias = '_min';
        $res = $this->aggregate("MIN", $col, $alias);
        return $res[$alias] ?? 0;
    }
    public function max(string $col): float|int|string
    {
        $alias = '_max';
        $res = $this->aggregate("MAX", $col, $alias);
        return $res[$alias] ?? 0;
    }
    private function aggregate(string $fn, string $col, string $alias): array
    {
        $clone = clone $this;
        $clone->columns = ["{$fn}({$clone->quoteCol($col)}) AS {$alias}"];
        $clone->limit = 1;
        return $clone->first() ?? [$alias => 0];
    }
    private function quoteCol(string $c): string
    {
        $pdo = $this->dbPdoWrite();
        return $this->db->qi($c, $pdo);
    }
    private function num($v){ if (is_numeric($v)) return $v+0; return 0; }

    /** Pluck: return flat array of a column, optionally keyed by $keyCol */
    public function pluck(string $col, ?string $keyCol = null): array
    {
        $rows = (clone $this)->select([$col, ...($keyCol?[$keyCol]:[])])->get();
        if ($keyCol) {
            $out = [];
            foreach ($rows as $r) { $out[$r[$keyCol]] = $r[$col]; }
            return $out;
        }
        return array_column($rows, $col);
    }

    public function getKeyset(?int $afterId, string $idCol='id'): array
    {
        if (!$this->limit) $this->limit = 50;
        if ($afterId !== null) { $this->where($idCol,'<',$afterId); }
        $this->orderBy($idCol,'DESC');
        $rows = $this->get();
        $next = $rows ? (int)$rows[count($rows)-1][$idCol] : null;
        return ['data'=>$rows, 'next'=>$next];
    }

    // === Writes ===
    public function insert(array $data): int
    {
        $this->assertWritable();
        [$sql, $bind] = $this->compileInsert($data);
        if ($this->isTestMode()) { $this->storeLast($sql, $bind); return 0; }

        $pdo = $this->db->choosePdo('insert');
        $ctx = ['sql'=>$sql,'params'=>$bind,'type'=>'insert','table'=>$this->table,'action'=>'insert','route'=>'write'];
        $runner = $this->dbRunner(function($ctx) use ($pdo) {
            [$stmt, $ms] = $this->dbExec($pdo, $ctx['sql'], $ctx['params']);
            $id = $pdo->lastInsertId();
            $this->dbEmit($ctx, $ms, $stmt->rowCount());
            return is_numeric($id) ? (int)$id : 0;
        });
        return $runner($ctx);
    }

    public function insertMany(array $rows): int
    {
        $this->assertWritable();
        if (empty($rows)) return 0;

        $pdo = $this->db->choosePdo('insert');

        if ($this->isTestMode()) {
            $cols = array_keys($rows[0]);
            $place = '('.implode(',', array_fill(0, count($cols), '?')).')';
            $table = $this->compileTable();
            $qi = fn($id) => $this->db->qi($id, $pdo);
            $sql = "INSERT INTO {$table} (".implode(', ', array_map($qi,$cols)).") VALUES {$place}";
            $this->storeLast($sql, array_values($rows[0]));
            return 0;
        }

        $count = 0;
        $this->tx(function() use ($pdo, $rows, &$count) {
            $cols = array_keys($rows[0]);
            $place = '('.implode(',', array_fill(0, count($cols), '?')).')';
            $table = $this->compileTable();
            $qi = fn($id) => $this->db->qi($id, $pdo);
            $sql = "INSERT INTO {$table} (".implode(', ', array_map($qi,$cols)).") VALUES {$place}";
            $stmt = $pdo->prepare($sql);
            foreach ($rows as $r) {
                $i = 1; foreach ($cols as $c) { $stmt->bindValue($i++, $r[$c] ?? null); }
                $stmt->execute(); $count += $stmt->rowCount();
            }
        });
        return $count;
    }

    public function insertGet(array $data, array $returning=['*']): array
    {
        $this->assertWritable();
        $pdo = $this->db->choosePdo('insert');
        $qi = fn($id) => $this->db->qi($id, $pdo);
        $table = $this->compileTable();
        $cols = array_keys($data);
        $place = implode(',', array_fill(0, count($cols), '?'));
        $sql = "INSERT INTO {$table} (".implode(', ', array_map($qi,$cols)).") VALUES ({$place})";
        $driver = (string)$pdo->getAttribute(PDO::ATTR_DRIVER_NAME);

        if ($this->isTestMode()) { $this->storeLast($sql.(in_array($driver,['pgsql','sqlite'])?" RETURNING ".implode(', ', array_map($qi,$returning)):""), array_values($data)); return []; }

        if (in_array($driver, ['pgsql','sqlite'], true)) {
            $sel = implode(', ', array_map($qi,$returning));
            $sql .= " RETURNING {$sel}";
            $ctx = ['sql'=>$sql,'params'=>array_values($data),'type'=>'insert','table'=>$this->table,'action'=>'insert','route'=>'write'];
            $runner = $this->dbRunner(function($ctx) use ($pdo) {
                [$stmt, $ms] = $this->dbExec($pdo, $ctx['sql'], $ctx['params']);
                $row = $stmt->fetch() ?: [];
                $this->dbEmit($ctx, $ms, $stmt->rowCount());
                return $row;
            });
            return $runner($ctx);
        }
        $id = $this->insert($data);
        if (!$id) return [];
        return $this->where('id','=', $id)->first() ?? [];
    }

    public function upsert(array $data, array $conflict, array $updateColumns): int
    {
        $this->assertWritable();
        $pdo = $this->db->choosePdo('insert');
        $driver = (string)$pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
        $qi = fn($id) => $this->db->qi($id, $pdo);
        $table = $this->compileTable();
        $cols = array_keys($data);
        $vals = array_values($data);
        $place = '('.implode(',', array_fill(0, count($cols), '?')).')';

        if ($driver === 'mysql') {
            $sql = "INSERT INTO {$table} (".implode(', ', array_map($qi,$cols)).") VALUES {$place} "
                  ."ON DUPLICATE KEY UPDATE ".implode(', ', array_map(fn($c)=>$qi($c)."=VALUES(".$qi($c).")", $updateColumns));
            if ($this->isTestMode()) { $this->storeLast($sql, $vals); return 0; }

            $ctx = ['sql'=>$sql,'params'=>$vals,'type'=>'insert','table'=>$this->table,'action'=>'upsert','route'=>'write'];
            $runner = $this->dbRunner(function($ctx) use ($pdo) {
                [$stmt, $ms] = $this->dbExec($pdo, $ctx['sql'], $ctx['params']);
                $this->dbEmit($ctx, $ms, $stmt->rowCount());
                return $stmt->rowCount();
            });
            return $runner($ctx);
        }

        if ($driver === 'pgsql' || $driver === 'sqlite') {
            $conf = '('.implode(', ', array_map($qi,$conflict)).')';
            $updates = implode(', ', array_map(fn($c)=>$qi($c).'=EXCLUDED.'.$qi($c), $updateColumns));
            $sql = "INSERT INTO {$table} (".implode(', ', array_map($qi,$cols)).") VALUES {$place} "
                  ."ON CONFLICT {$conf} DO UPDATE SET {$updates}";
            if ($this->isTestMode()) { $this->storeLast($sql, $vals); return 0; }

            $ctx = ['sql'=>$sql,'params'=>$vals,'type'=>'insert','table'=>$this->table,'action'=>'upsert','route'=>'write'];
            $runner = $this->dbRunner(function($ctx) use ($pdo) {
                [$stmt, $ms] = $this->dbExec($pdo, $ctx['sql'], $ctx['params']);
                $this->dbEmit($ctx, $ms, $stmt->rowCount());
                return $stmt->rowCount();
            });
            return $runner($ctx);
        }

        // Fallback
        if ($this->isTestMode()) {
            $existsQ = clone $this;
            foreach ($conflict as $c) { $existsQ->where($c,'=',$data[$c] ?? null); }
            $this->storeLast('-- upsert fallback (select+update/insert)', []);
            return 0;
        }
        return $this->tx(function() use ($conflict, $updateColumns, $data) {
            $existsQ = clone $this;
            foreach ($conflict as $c) { $existsQ->where($c,'=',$data[$c] ?? null); }
            if ($existsQ->exists()) {
                $patch = []; foreach ($updateColumns as $c) { $patch[$c] = $data[$c] ?? null; }
                return $this->update($patch);
            } else {
                $this->insert($data); return 1;
            }
        });
    }

    public function update(array $patch): int
    {
        $this->assertWritable();
        $pdo = $this->db->choosePdo('update');
        [$whereSql, $whereBind] = $this->compileWhere($pdo, true);
        $qi = fn($id) => $this->db->qi($id, $pdo);
        $table = $this->compileTable();
        $sets = []; $bind = [];
        foreach ($patch as $k=>$v) { $sets[] = $qi($k).'=?'; $bind[] = $v; }
        $sql = "UPDATE {$table} SET ".implode(', ', $sets).($whereSql? ' WHERE '.$whereSql : '');
        if ($this->isTestMode()) { $this->storeLast($sql, array_merge($bind, $whereBind)); return 0; }

        $ctx = ['sql'=>$sql,'params'=>array_merge($bind,$whereBind),'type'=>'update','table'=>$this->table,'action'=>'update','route'=>'write'];
        $runner = $this->dbRunner(function($ctx) use ($pdo) {
            [$stmt, $ms] = $this->dbExec($pdo, $ctx['sql'], $ctx['params']);
            $this->dbEmit($ctx, $ms, $stmt->rowCount());
            return $stmt->rowCount();
        });
        return $runner($ctx);
    }

    /** Soft delete by default if enabled; use forceDelete() to hard delete */
    public function delete(): int
    {
        if ($this->softDelete['enabled']) { return $this->softDelete(); }
        $this->assertWritable();
        $pdo = $this->db->choosePdo('delete');
        [$whereSql, $whereBind] = $this->compileWhere($pdo, true);
        $table = $this->compileTable();
        $sql = "DELETE FROM {$table}".($whereSql? ' WHERE '.$whereSql : '');
        if ($this->isTestMode()) { $this->storeLast($sql, $whereBind); return 0; }

        $ctx = ['sql'=>$sql,'params'=>$whereBind,'type'=>'delete','table'=>$this->table,'action'=>'delete','route'=>'write'];
        $runner = $this->dbRunner(function($ctx) use ($pdo) {
            [$stmt, $ms] = $this->dbExec($pdo, $ctx['sql'], $ctx['params']);
            $this->dbEmit($ctx, $ms, $stmt->rowCount());
            return $stmt->rowCount();
        });
        return $runner($ctx);
    }

    public function forceDelete(): int
    {
        $orig = $this->softDelete;
        $this->softDelete['enabled'] = false;
        $n = $this->delete();
        $this->softDelete = $orig;
        return $n;
    }

    public function restore(): int
    {
        if (!$this->softDelete['enabled']) return 0;
        $col = $this->softDelete['column'];
        $val = $this->softDelete['mode'] === 'boolean' ? 0 : null;
        return $this->update([$col => $val]);
    }

    private function softDelete(): int
    {
        $this->assertWritable();
        $col = $this->softDelete['column'];
        $val = $this->softDelete['mode'] === 'boolean' ? $this->softDelete['deleted_value'] : date('Y-m-d H:i:s');
        $sqlValNote = $val; // for test mode visualize
        if ($this->isTestMode()) {
            $pdo = $this->dbPdoWrite();
            $table = $this->compileTable();
            $qi = fn($id) => $this->db->qi($id, $pdo);
            $sql = "UPDATE {$table} SET ".$qi($col)."=?". $this->whereSqlOnly($pdo);
            $this->storeLast($sql, array_merge([$sqlValNote], $this->whereBindOnly($pdo)));
            return 0;
        }
        return $this->update([$col => $val]);
    }

    // ===== Compile helpers =================================================

    private function compileSelect(): array
    {
        $pdo = $this->db->choosePdo('select');
        $qi = fn($id) => $this->db->qi($id, $pdo);
        $table = $this->compileTable();
        $cols = $this->columns === ['*'] ? '*' : implode(', ', array_map($qi, $this->columns ?? ['*']));
        $sql = "SELECT {$cols} FROM {$table}";

        foreach ($this->joins as $j) {
            $jt = $this->db->qi(($j['table']), $pdo);
            $alias = $j['alias'] ? ' AS '.$this->db->qi($j['alias'], $pdo) : '';
            $on = $this->db->qi($j['on']['l'], $pdo) . ' ' . $j['on']['op'] . ' ' . $this->db->qi($j['on']['r'], $pdo);
            $sql .= " {$j['type']} JOIN {$jt}{$alias} ON {$on}";
        }

        [$whereSql, $whereBind] = $this->compileWhere($pdo, true, /*forSelect*/true);
        if ($whereSql) $sql .= " WHERE {$whereSql}";

        if ($this->groupBys) { $sql .= " GROUP BY ".implode(', ', array_map($qi, $this->groupBys)); }
        if ($this->havings) {
            $hParts = []; $bindH = [];
            foreach ($this->havings as $idx => $h) {
                $prefix = $idx === 0 ? '' : ' '.$h['bool'].' ';
                $hParts[] = $prefix.$h['expr'].' '.$h['op'].' ?';
                $bindH[] = $h['val'];
            }
            $sql .= " HAVING ".implode('', $hParts);
            $whereBind = array_merge($whereBind, $bindH);
        }

        if ($this->orderBys) {
            $ord = array_map(fn($ob)=> $qi($ob[0]).' '.$ob[1], $this->orderBys);
            $sql .= ' ORDER BY '.implode(', ', $ord);
        }
        $sql .= $this->db->compileLimitOffset($this->limit, $this->offset, $pdo);

        return [$sql, $whereBind, ($pdo === $this->dbPdoRead() ? 'read' : 'write')];
    }

    private function compileInsert(array $data): array
    {
        $pdo = $this->db->choosePdo('insert');
        $qi = fn($id) => $this->db->qi($id, $pdo);
        $table = $this->compileTable();
        $cols = array_keys($data);
        $place = implode(',', array_fill(0, count($cols), '?'));
        $sql = "INSERT INTO {$table} (".implode(', ', array_map($qi,$cols)).") VALUES ({$place})";
        return [$sql, array_values($data)];
    }

    private function compileWhere(PDO $pdo, bool $includeScope, bool $forSelect=false): array
    {
        $bind = []; $parts = [];

        if ($includeScope && $this->scope) {
            foreach ($this->scope as $k=>$v) { $parts[] = $this->db->qi($k, $pdo)." = ?"; $bind[] = $v; }
        }

        // Soft delete filter on SELECT if enabled and not withTrashed
        if ($forSelect && $this->softDelete['enabled'] && !$this->withTrashed && !$this->onlyTrashed) {
            $col = $this->softDelete['column'];
            if ($this->softDelete['mode'] === 'timestamp') {
                $parts[] = $this->db->qi($col, $pdo)." IS NULL";
            } else {
                $parts[] = $this->db->qi($col, $pdo)." = 0";
            }
        }
        if ($forSelect && $this->softDelete['enabled'] && $this->onlyTrashed) {
            $col = $this->softDelete['column'];
            if ($this->softDelete['mode'] === 'timestamp') {
                $parts[] = $this->db->qi($col, $pdo)." IS NOT NULL";
            } else {
                $parts[] = $this->db->qi($col, $pdo)." = ".$this->softDelete['deleted_value'];
            }
        }

        $guard = $this->db->guardMaxIn();
        foreach ($this->wheres as $idx => $w) {
            $prefix = (empty($parts) && $idx===0) ? '' : ' '.$w['bool'].' ';
            switch ($w['type']) {
                case 'basic':
                    $parts[] = $prefix.$this->db->qi($w['col'], $pdo).' '.$w['op'].' ?'; $bind[] = $w['val']; break;
                case 'group':
                    [$sql,$b] = $w['query']->compileWhere($pdo, false, $forSelect); $parts[] = $prefix.'('.$sql.')'; $bind = array_merge($bind, $b); break;
                case 'in':
                    $vals = $w['vals']; if (count($vals) > $guard) throw new \LengthException("whereIn list exceeds {$guard} items");
                    if (empty($vals)) { $parts[] = $prefix.($w['not'] ? '1=1' : '1=0'); break; }
                    $qs = implode(',', array_fill(0, count($vals), '?'));
                    $parts[] = $prefix.$this->db->qi($w['col'],$pdo).($w['not']?' NOT IN (':' IN (').$qs.')';
                    $bind = array_merge($bind, array_values($vals)); break;
                case 'null':
                    $parts[] = $prefix.$this->db->qi($w['col'],$pdo).($w['not']?' IS NOT NULL':' IS NULL'); break;
                case 'between':
                    $pair = $w['pair']; if (!is_array($pair) || count($pair)!==2) throw new \InvalidArgumentException('whereBetween requires [min,max]');
                    $parts[] = $prefix.$this->db->qi($w['col'],$pdo).($w['not']?' NOT BETWEEN ? AND ?':' BETWEEN ? AND ?'); $bind[]=$pair[0]; $bind[]=$pair[1]; break;
            }
        }
        $sql = implode('', $parts) ?: '';
        return [$sql, $bind];
    }

    private function compileTable(): string
    {
        $prefix = $this->db->getPrefix();
        $t = $prefix ? $prefix.$this->table : $this->table;
        return $this->db->qi($t);
    }

    // ===== Exec helpers =====

    private function dbRunner(callable $core): callable
    {
        $mwCore = $this->dbBuildRunner($core);
        return $mwCore;
    }

    private function dbBuildRunner(callable $core): callable
    {
        $middlewares = (new \ReflectionProperty($this->db, 'middlewares')); $middlewares->setAccessible(true); $stack = $middlewares->getValue($this->db);
        $runner = array_reduce(array_reverse($stack), function($next, $mw) { return function($ctx) use ($mw, $next) { return $mw($ctx, $next); }; }, $core);
        $policyProp = (new \ReflectionProperty($this->db, 'policy')); $policyProp->setAccessible(true); $policy = $policyProp->getValue($this->db);
        return function($ctx) use ($runner, $policy) { if ($policy) { $policy($ctx); } return $runner($ctx); };
    }

    private function dbExec(PDO $pdo, string $sql, array $params, int $timeoutMs=0): array
    {
        $m = new \ReflectionMethod($this->db, 'execPreparedOn'); $m->setAccessible(true);
        return $m->invoke($this->db, $pdo, $sql, $params, $timeoutMs);
    }

    private function dbEmit(array $ctx, float $ms, int $count): void
    {
        $m = new \ReflectionMethod($this->db, 'emitMetrics'); $m->setAccessible(true);
        $m->invoke($this->db, $ctx, $ms, $count);
    }

    private function dbPdoRead(): ?PDO { $prop = new \ReflectionProperty($this->db, 'pdoRead'); $prop->setAccessible(true); return $prop->getValue($this->db); }
    private function dbPdoWrite(): PDO  { $prop = new \ReflectionProperty($this->db, 'pdoWrite'); $prop->setAccessible(true); return $prop->getValue($this->db); }
    private function tx(callable $fn): mixed { $m = new \ReflectionMethod($this->db, 'tx'); $m->setAccessible(true); return $m->invoke($this->db, $fn, 1); }
    private function assertWritable(): void { if ($this->isReadonly()) throw new \RuntimeException('Readonly mode: write operation blocked.'); }

    private function isReadonly(): bool { $m = new \ReflectionMethod($this->db, 'isReadonly'); $m->setAccessible(true); return (bool)$m->invoke($this->db); }
    private function isTestMode(): bool  { $m = new \ReflectionMethod($this->db, 'isTestMode'); $m->setAccessible(true); return (bool)$m->invoke($this->db); }
    private function storeLast(string $sql, array $params): void
    {
        $p1 = new \ReflectionProperty($this->db, 'lastQueryString'); $p1->setAccessible(true); $p1->setValue($this->db, $sql);
        $p2 = new \ReflectionProperty($this->db, 'lastQueryParams');  $p2->setAccessible(true); $p2->setValue($this->db, $params);
        // optional: call logger in test mode for visibility
        $l = new \ReflectionProperty($this->db, 'logger'); $l->setAccessible(true); $logger = $l->getValue($this->db);
        if ($logger) { $logger($sql, $params, 0.0); }
    }

    // Helpers to build softDelete test-mode SQL quickly
    private function whereSqlOnly(PDO $pdo): string { [$wsql] = $this->compileWhere($pdo, true, false); return $wsql ? " WHERE {$wsql}" : ""; }
    private function whereBindOnly(PDO $pdo): array { [, $b] = $this->compileWhere($pdo, true, false); return $b; }
}

// ===== Entrypoint for CLI if executed directly =====
if (php_sapi_name() === 'cli' && realpath($argv[0] ?? '') === __FILE__) {
    \ndtan\DBF::cli($argv);
}
