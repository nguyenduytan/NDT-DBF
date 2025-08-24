<?php
/**
 * NDT DBF - Simple, Lightweight PHP Database Framework (Enterprise+)
 *
 * @version   0.4.0
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
 * - NEW (v0.4.0): Row locking (forUpdate/skipLocked), chunk/stream for large datasets, JSON helpers (whereJson/jsonSet), cast DSL, Oracle support
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

    /** @var array Cache table columns per connection */
    private array $schemaCache = [];

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
        'column'  => 'deleted_at',
        'mode'    => 'timestamp',
        'deleted_value' => 1,
    ];

    /** --- Test Mode & last query tracking --- */
    private bool $testMode = false;
    private string $lastQueryString = '';
    private array $lastQueryParams = [];

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
            if (isset($configOrUri['prefix'])) $this->prefix = (string)$configOrUri['prefix'];
            if (isset($configOrUri['readonly'])) $this->readonly = (bool)$configOrUri['readonly'];
            if (isset($configOrUri['logger'])) $this->logger = $configOrUri['logger'];
            if (isset($configOrUri['metrics'])) $this->metrics = $configOrUri['metrics'];
            if (isset($configOrUri['features'])) {
                $features = $configOrUri['features'];
                if (isset($features['soft_delete'])) $this->softDelete = array_merge($this->softDelete, $features['soft_delete']);
                if (isset($features['max_in_params'])) $this->maxInParams = (int)$features['max_in_params'];
            }
        } else {
            throw new \InvalidArgumentException('Invalid configuration');
        }
    }

    private function connectFromUri(string $uri): array
    {
        $parsed = parse_url($uri);
        $driver = $parsed['scheme'] ?? 'mysql';
        $user = $parsed['user'] ?? '';
        $pass = $parsed['pass'] ?? '';
        $host = $parsed['host'] ?? 'localhost';
        $port = $parsed['port'] ?? ($driver === 'pgsql' ? 5432 : 3306);
        $db = ltrim($parsed['path'] ?? '/app', '/');
        $query = [];
        parse_str($parsed['query'] ?? '', $query);
        $charset = $query['charset'] ?? 'utf8mb4';
        $attrs = [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC];

        switch ($driver) {
            case 'mysql':
                $pdo = new PDO("mysql:host={$host};port={$port};dbname={$db};charset={$charset}", $user, $pass, $attrs);
                return [$pdo, 'mysql'];
            case 'pgsql':
                $pdo = new PDO("pgsql:host={$host};port={$port};dbname={$db}", $user, $pass, $attrs);
                return [$pdo, 'pgsql'];
            case 'sqlite':
                $pdo = new PDO("sqlite:{$db}", null, null, $attrs);
                return [$pdo, 'sqlite'];
            case 'sqlsrv':
                $pdo = new PDO("sqlsrv:Server={$host},{$port};Database={$db}", $user, $pass, $attrs);
                return [$pdo, 'sqlsrv'];
            case 'oracle':
                $pdo = new PDO("oci:dbname={$host}/{$db};charset={$charset}", $user, $pass, $attrs);
                return [$pdo, 'oracle'];
            default:
                throw new \InvalidArgumentException('Unsupported driver');
        }
    }

    private function connectFromArray(array $config): array
    {
        $driver = $config['type'] ?? 'mysql';
        $attrs = [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC];

        switch ($driver) {
            case 'mysql':
                $host = $config['host'] ?? 'localhost';
                $port = $config['port'] ?? 3306;
                $db = $config['database'] ?? 'app';
                $user = $config['username'] ?? '';
                $pass = $config['password'] ?? '';
                $charset = $config['charset'] ?? 'utf8mb4';
                $pdo = new PDO("mysql:host={$host};port={$port};dbname={$db};charset={$charset}", $user, $pass, $attrs);
                return [$pdo, 'mysql'];
            case 'pgsql':
                $host = $config['host'] ?? 'localhost';
                $port = $config['port'] ?? 5432;
                $db = $config['database'] ?? 'app';
                $user = $config['username'] ?? '';
                $pass = $config['password'] ?? '';
                $pdo = new PDO("pgsql:host={$host};port={$port};dbname={$db}", $user, $pass, $attrs);
                return [$pdo, 'pgsql'];
            case 'sqlite':
                $db = $config['database'] ?? ':memory:';
                $pdo = new PDO("sqlite:{$db}", null, null, $attrs);
                return [$pdo, 'sqlite'];
            case 'sqlsrv':
                $host = $config['host'] ?? 'localhost';
                $port = $config['port'] ?? 1433;
                $db = $config['database'] ?? 'app';
                $user = $config['username'] ?? '';
                $pass = $config['password'] ?? '';
                $pdo = new PDO("sqlsrv:Server={$host},{$port};Database={$db}", $user, $pass, $attrs);
                return [$pdo, 'sqlsrv'];
            case 'oracle':
                $host = $config['host'] ?? 'localhost';
                $db = $config['database'] ?? 'app';
                $user = $config['username'] ?? '';
                $pass = $config['password'] ?? '';
                $charset = $config['charset'] ?? 'UTF8';
                $pdo = new PDO("oci:dbname={$host}/{$db};charset={$charset}", $user, $pass, $attrs);
                return [$pdo, 'oracle'];
            default:
                throw new \InvalidArgumentException('Unsupported driver');
        }
    }

    private function initMasterReplica(array $config): void
    {
        if (isset($config['write'])) {
            [$this->pdoWrite, $this->driverWrite] = $this->connectFromArray($config['write']);
        }
        if (isset($config['read'])) {
            [$this->pdoRead, $this->driverRead] = $this->connectFromArray($config['read']);
        }
        $this->routing = $config['routing'] ?? 'auto';
    }

    public function qi(string $identifier, ?PDO $pdo = null): string
    {
        $driver = $pdo ? $pdo->getAttribute(PDO::ATTR_DRIVER_NAME) : $this->driverWrite;
        switch ($driver) {
            case 'mysql':
                return '`' . str_replace('.', '`.`', $identifier) . '`';
            case 'pgsql':
            case 'sqlite':
                return '"' . str_replace('.', '"."', $identifier) . '"';
            case 'sqlsrv':
            case 'oracle':
                return '[' . str_replace('.', '].[', $identifier) . ']';
            default:
                return $identifier;
        }
    }

    public function getPrefix(): string
    {
        return $this->prefix;
    }

    public function guardMaxIn(): int
    {
        return $this->maxInParams;
    }

    public function getSoftDeleteConfig(): array
    {
        return $this->softDelete;
    }

    public function getScope(): array
    {
        return $this->scope;
    }

    public function execPreparedOn(PDO $pdo, string $sql, array $params, int $timeoutMs = 0): PDOStatement
    {
        if ($timeoutMs > 0) {
            $driver = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
            if ($driver === 'mysql') $pdo->exec("SET SESSION MAX_EXECUTION_TIME = {$timeoutMs}");
            if ($driver === 'pgsql') $pdo->exec("SET LOCAL statement_timeout = {$timeoutMs}");
            if ($driver === 'sqlite') $pdo->exec("PRAGMA busy_timeout = {$timeoutMs}");
        }
        if (isset($this->stmtCache[$sql])) {
            $stmt = $this->stmtCache[$sql];
            $stmt->execute($params);
        } else {
            $stmt = $pdo->prepare($sql);
            $stmt->execute($params);
            $this->stmtCache[$sql] = $stmt;
        }
        return $stmt;
    }

    public function emitMetrics(array $ctx, float $ms, int $count): void
    {
        if ($this->metrics) call_user_func($this->metrics, array_merge($ctx, ['ms' => $ms, 'count' => $count]));
    }

    public function choosePdo(string $type): PDO
    {
        if ($this->routing === 'single') return $this->pdoWrite;
        if ($this->routing === 'manual') return $this->currentRoute === 'read' && $this->pdoRead ? $this->pdoRead : $this->pdoWrite;
        return in_array($type, ['select', 'aggregate']) && $this->pdoRead ? $this->pdoRead : $this->pdoWrite;
    }

    public function getColumns(string $table, ?PDO $pdo = null): array
    {
        $pdo ??= $this->pdoWrite;
        $key = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME) . ':' . $this->prefix . $table;
        if (isset($this->schemaCache[$key])) return $this->schemaCache[$key];
        $driver = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
        $fullTable = $this->prefix . $table;
        switch ($driver) {
            case 'sqlite':
                $stmt = $pdo->query("PRAGMA table_info('$fullTable')");
                $cols = array_column($stmt->fetchAll(), 'name');
                break;
            case 'mysql':
                $stmt = $pdo->query("SHOW COLUMNS FROM `$fullTable`");
                $cols = array_column($stmt->fetchAll(), 'Field');
                break;
            case 'pgsql':
                $stmt = $pdo->prepare("SELECT column_name FROM information_schema.columns WHERE table_name = ?");
                $stmt->execute([$fullTable]);
                $cols = array_column($stmt->fetchAll(), 'column_name');
                break;
            case 'sqlsrv':
                $stmt = $pdo->prepare("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?");
                $stmt->execute([$fullTable]);
                $cols = array_column($stmt->fetchAll(), 'COLUMN_NAME');
                break;
            case 'oracle':
                $stmt = $pdo->prepare("SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = UPPER(?)");
                $stmt->execute([$fullTable]);
                $cols = array_column($stmt->fetchAll(), 'COLUMN_NAME');
                break;
            default:
                $cols = [];
        }
        $this->schemaCache[$key] = $cols;
        return $cols;
    }

    public function tx(callable $fn, int $attempts = 3): mixed
    {
        for ($i = 1; $i <= $attempts; $i++) {
            try {
                $this->pdoWrite->beginTransaction();
                $res = $fn($this);
                $this->pdoWrite->commit();
                return $res;
            } catch (Throwable $e) {
                $this->pdoWrite->rollBack();
                if ($i === $attempts || !in_array($e->getCode(), [40001, '40001', '1213'])) throw $e;
                usleep((2 ** $i) * 100000 + mt_rand(0, 100000));
            }
        }
        return null;
    }

    public function using(?string $route): self
    {
        if ($this->routing !== 'manual') throw new \RuntimeException('Using only for manual routing');
        $this->currentRoute = $route ?? 'write';
        return $this;
    }

    public function setReadonly(bool $on): void
    {
        $this->readonly = $on;
    }

    public function isReadonly(): bool
    {
        return $this->readonly;
    }

    public function setTestMode(bool $on): void
    {
        $this->testMode = $on;
    }

    public function isTestMode(): bool
    {
        return $this->testMode;
    }

    public function queryString(): string
    {
        return $this->lastQueryString;
    }

    public function queryParams(): array
    {
        return $this->lastQueryParams;
    }

    public function withScope(array $scope): self
    {
        $clone = clone $this;
        $clone->scope = $scope;
        return $clone;
    }

    public function policy(callable $cb): self
    {
        $clone = clone $this;
        $clone->policy = $cb;
        return $clone;
    }

    public function use(callable $mw): self
    {
        $clone = clone $this;
        $clone->middlewares[] = $mw;
        return $clone;
    }

    public function setLogger(callable $cb): void
    {
        $this->logger = $cb;
    }

    public function setMetrics(callable $cb): void
    {
        $this->metrics = $cb;
    }

    public function info(): array
    {
        return [
            'driver' => $this->driverWrite,
            'routing' => $this->routing,
            'readonly' => $this->readonly,
            'test_mode' => $this->testMode,
            'soft_delete' => $this->softDelete,
        ];
    }

    public function table(string $name): Query
    {
        return new Query($this, $name);
    }

    public function raw(string $sql, array $params = []): array
    {
        $pdo = $this->choosePdo('raw');
        $ctx = ['type' => 'raw', 'sql' => $sql];
        $runner = $this->dbBuildRunner(function($ctx) use ($pdo, $sql, $params) {
            if ($this->isTestMode()) {
                $this->storeLast($sql, $params);
                return [];
            }
            $start = microtime(true);
            $stmt = $this->execPreparedOn($pdo, $sql, $params);
            $ms = (microtime(true) - $start) * 1000;
            if ($this->logger) call_user_func($this->logger, $sql, $params, $ms);
            $res = $stmt->fetchAll();
            $this->emitMetrics($ctx, $ms, count($res));
            return $res;
        });
        return $runner($ctx);
    }

    public function dbBuildRunner(callable $core): callable
    {
        $stack = $this->middlewares;
        $runner = array_reduce(array_reverse($stack), function($next, $mw) {
            return function($ctx) use ($mw, $next) { return $mw($ctx, $next); };
        }, $core);
        return function($ctx) use ($runner) {
            if ($this->policy) call_user_func($this->policy, $ctx);
            return $runner($ctx);
        };
    }

    public function storeLast(string $sql, array $params): void
    {
        $this->lastQueryString = $sql;
        $this->lastQueryParams = $params;
        if ($this->logger) call_user_func($this->logger, $sql, $params, 0.0);
    }
}

class Query
{
    private DBF $db;
    private string $table;
    private array $select = ['*'];
    private array $wheres = [];
    private array $joins = [];
    private array $groups = [];
    private array $havings = [];
    private array $orders = [];
    private ?int $limit = null;
    private ?int $offset = null;
    private int $timeoutMs = 0;
    private bool $withTrashed = false;
    private bool $onlyTrashed = false;
    private array $softDelete;
    private array $scope;
    private bool $forUpdate = false;
    private bool $skipLocked = false;

    public function __construct(DBF $db, string $table)
    {
        $this->db = $db;
        $this->table = $table;
        $this->softDelete = $db->getSoftDeleteConfig();
        $this->scope = $db->getScope();
    }

    public function select(array $cols): self
    {
        $this->select = $cols;
        return $this;
    }

    public function where(string $col, string $op, mixed $val, bool $or = false): self
    {
        $this->wheres[] = [
            'type' => 'basic',
            'bool' => $or ? 'OR' : 'AND',
            'col' => $col,
            'op' => $op,
            'val' => $val
        ];
        return $this;
    }

    public function orWhere(string $col, string $op, mixed $val): self
    {
        return $this->where($col, $op, $val, true);
    }

    public function whereIn(string $col, array $vals, bool $not = false, bool $or = false): self
    {
        $this->wheres[] = [
            'type' => 'in',
            'bool' => $or ? 'OR' : 'AND',
            'col' => $col,
            'vals' => $vals,
            'not' => $not
        ];
        return $this;
    }

    public function whereBetween(string $col, array $range, bool $not = false, bool $or = false): self
    {
        $this->wheres[] = [
            'type' => 'between',
            'bool' => $or ? 'OR' : 'AND',
            'col' => $col,
            'pair' => $range,
            'not' => $not
        ];
        return $this;
    }

    public function whereNull(string $col, bool $not = false, bool $or = false): self
    {
        $this->wheres[] = [
            'type' => 'null',
            'bool' => $or ? 'OR' : 'AND',
            'col' => $col,
            'not' => $not
        ];
        return $this;
    }

    public function join(string $table, string $left, string $op, string $right, string $type = 'INNER'): self
    {
        $this->joins[] = [
            'type' => $type,
            'table' => $table,
            'left' => $left,
            'op' => $op,
            'right' => $right
        ];
        return $this;
    }

    public function leftJoin(string $table, string $left, string $op, string $right): self
    {
        return $this->join($table, $left, $op, $right, 'LEFT');
    }

    public function rightJoin(string $table, string $left, string $op, string $right): self
    {
        return $this->join($table, $left, $op, $right, 'RIGHT');
    }

    public function groupBy(array $cols): self
    {
        $this->groups = $cols;
        return $this;
    }

    public function having(string $expr, string $op, mixed $val): self
    {
        $this->havings[] = [
            'expr' => $expr,
            'op' => $op,
            'val' => $val
        ];
        return $this;
    }

    public function orderBy(string $col, string $dir = 'asc'): self
    {
        $this->orders[] = [$col, strtoupper($dir)];
        return $this;
    }

    public function limit(int $n): self
    {
        $this->limit = $n;
        return $this;
    }

    public function offset(int $n): self
    {
        $this->offset = $n;
        return $this;
    }

    public function timeout(int $ms): self
    {
        $this->timeoutMs = $ms;
        return $this;
    }

    public function withTrashed(): self
    {
        $this->withTrashed = true;
        return $this;
    }

    public function onlyTrashed(): self
    {
        $this->onlyTrashed = true;
        return $this;
    }

    public function forUpdate(): self
    {
        $this->forUpdate = true;
        return $this;
    }

    public function skipLocked(): self
    {
        $this->skipLocked = true;
        return $this;
    }

    private function compileSelect(PDO $pdo): array
    {
        $select = 'SELECT ' . implode(', ', array_map(fn($c) => is_string($c) ? $this->db->qi($c, $pdo) : $c, $this->select));
        $from = 'FROM ' . $this->compileTable($pdo);
        $join = '';
        foreach ($this->joins as $j) {
            $join .= ' ' . $j['type'] . ' JOIN ' . $this->db->qi($j['table'], $pdo) . ' ON ' . $this->db->qi($j['left'], $pdo) . ' ' . $j['op'] . ' ' . $this->db->qi($j['right'], $pdo);
        }
        [$whereSql, $bind] = $this->compileWhere($pdo, true, true);
        $where = $whereSql ? ' WHERE ' . $whereSql : '';
        $group = $this->groups ? ' GROUP BY ' . implode(', ', array_map(fn($g) => $this->db->qi($g, $pdo), $this->groups)) : '';
        $having = '';
        if ($this->havings) {
            $hParts = [];
            $hBind = [];
            foreach ($this->havings as $h) {
                $hParts[] = $h['expr'] . ' ' . $h['op'] . ' ?';
                $hBind[] = $h['val'];
            }
            $having = ' HAVING ' . implode(' AND ', $hParts);
            $bind = array_merge($bind, $hBind);
        }
        $order = '';
        if ($this->orders) {
            $oParts = [];
            foreach ($this->orders as $o) {
                $oParts[] = $this->db->qi($o[0], $pdo) . ' ' . $o[1];
            }
            $order = ' ORDER BY ' . implode(', ', $oParts);
        }
        $limit = $this->limit !== null ? ' LIMIT ' . $this->limit : '';
        $offset = $this->offset !== null ? ' OFFSET ' . $this->offset : '';
        $locking = $this->forUpdate ? ' FOR UPDATE' . ($this->skipLocked ? ' SKIP LOCKED' : '') : '';
        $sql = $select . $from . $join . $where . $group . $having . $order . $limit . $offset . $locking;
        return [$sql, $bind];
    }

    private function compileWhere(PDO $pdo, bool $includeScope, bool $forSelect = false): array
    {
        $bind = [];
        $andConditions = [];

        if ($includeScope && $this->scope) {
            foreach ($this->scope as $k => $v) {
                $andConditions[] = $this->db->qi($k, $pdo) . " = ?";
                $bind[] = $v;
            }
        }

        $sdCol = $this->softDelete['column'];
        if ($forSelect && $this->softDelete['enabled'] && $this->hasColumn($sdCol)) {
            if (!$this->withTrashed && !$this->onlyTrashed) {
                $andConditions[] = $this->db->qi($sdCol, $pdo) . ($this->softDelete['mode'] === 'timestamp' ? " IS NULL" : " = 0");
            }
            if ($this->onlyTrashed) {
                $andConditions[] = $this->db->qi($sdCol, $pdo) . ($this->softDelete['mode'] === 'timestamp' ? " IS NOT NULL" : " = " . $this->softDelete['deleted_value']);
            }
        }

        $baseWhere = $andConditions ? implode(' AND ', $andConditions) : '';
        $parts = $baseWhere ? [$baseWhere] : [];

        foreach ($this->wheres as $idx => $w) {
            $prefix = (empty($parts) && $idx === 0) ? '' : ' ' . $w['bool'] . ' ';
            switch ($w['type']) {
                case 'basic':
                    $parts[] = $prefix . $this->db->qi($w['col'], $pdo) . ' ' . $w['op'] . ' ?';
                    $bind[] = $w['val'];
                    break;
                case 'in':
                    $vals = $w['vals'];
                    if (count($vals) > $this->db->guardMaxIn()) throw new \LengthException("whereIn list exceeds " . $this->db->guardMaxIn() . " items");
                    if (empty($vals)) {
                        $parts[] = $prefix . ($w['not'] ? '1=1' : '1=0');
                        break;
                    }
                    $qs = implode(',', array_fill(0, count($vals), '?'));
                    $parts[] = $prefix . $this->db->qi($w['col'], $pdo) . ($w['not'] ? ' NOT IN (' : ' IN (') . $qs . ')';
                    $bind = array_merge($bind, array_values($vals));
                    break;
                case 'null':
                    $parts[] = $prefix . $this->db->qi($w['col'], $pdo) . ($w['not'] ? ' IS NOT NULL' : ' IS NULL');
                    break;
                case 'between':
                    $pair = $w['pair'];
                    if (!is_array($pair) || count($pair) !== 2) throw new \InvalidArgumentException('whereBetween requires [min,max]');
                    $parts[] = $prefix . $this->db->qi($w['col'], $pdo) . ($w['not'] ? ' NOT BETWEEN ? AND ?' : ' BETWEEN ? AND ?');
                    $bind[] = $pair[0];
                    $bind[] = $pair[1];
                    break;
                case 'json':
                    $driver = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
                    $jsonPath = explode('->', $w['path']);
                    $col = array_shift($jsonPath);
                    if ($driver === 'mysql') {
                        $path = implode('.', $jsonPath);
                        $parts[] = $prefix . "JSON_EXTRACT(" . $this->db->qi($col, $pdo) . ", '$.{$path}') " . $w['op'] . " ?";
                    } elseif ($driver === 'pgsql') {
                        $path = implode('->>', $jsonPath);
                        $parts[] = $prefix . $this->db->qi($col, $pdo) . "->>'{$path}' " . $w['op'] . " ?";
                    } else {
                        throw new \RuntimeException('whereJson not supported on ' . $driver);
                    }
                    $bind[] = $w['val'];
                    break;
            }
        }
        $sql = implode('', $parts) ?: '';
        return [$sql, $bind];
    }

    private function compileTable(PDO $pdo): string
    {
        $prefix = $this->db->getPrefix();
        $t = $prefix ? $prefix . $this->table : $this->table;
        return $this->db->qi($t, $pdo);
    }

    private function dbRunner(callable $core): callable
    {
        return $this->db->dbBuildRunner($core);
    }

    private function dbExec(PDO $pdo, string $sql, array $params, int $timeoutMs = 0): PDOStatement
    {
        return $this->db->execPreparedOn($pdo, $sql, $params, $timeoutMs);
    }

    private function dbEmit(array $ctx, float $ms, int $count): void
    {
        $this->db->emitMetrics($ctx, $ms, $count);
    }

    private function assertWritable(): void
    {
        if ($this->db->isReadonly()) throw new \RuntimeException('Readonly mode: write operation blocked.');
    }

    private function hasColumn(string $column): bool
    {
        $pdo = $this->db->choosePdo('select');
        $cols = $this->db->getColumns($this->table, $pdo);
        return in_array($column, $cols, true);
    }

    public function get(): array
    {
        $pdo = $this->db->choosePdo('select');
        [$sql, $params] = $this->compileSelect($pdo);
        $ctx = ['type' => 'select', 'table' => $this->table];
        $runner = $this->dbRunner(function($ctx) use ($pdo, $sql, $params) {
            if ($this->db->isTestMode()) {
                $this->db->storeLast($sql, $params);
                return [];
            }
            $start = microtime(true);
            $stmt = $this->dbExec($pdo, $sql, $params, $this->timeoutMs);
            $ms = (microtime(true) - $start) * 1000;
            if ($this->db->logger) call_user_func($this->db->logger, $sql, $params, $ms);
            $res = $stmt->fetchAll();
            $this->dbEmit($ctx, $ms, count($res));
            return $res;
        });
        return $runner($ctx);
    }

    public function first(): ?array
    {
        $this->limit(1);
        $rows = $this->get();
        return $rows[0] ?? null;
    }

    public function exists(): bool
    {
        $pdo = $this->db->choosePdo('select');
        [$sql, $params] = $this->compileSelect($pdo);
        $sql = 'SELECT EXISTS (' . $sql . ') AS "exists"';
        $ctx = ['type' => 'select', 'table' => $this->table];
        $runner = $this->dbRunner(function($ctx) use ($pdo, $sql, $params) {
            if ($this->db->isTestMode()) {
                $this->db->storeLast($sql, $params);
                return false;
            }
            $start = microtime(true);
            $stmt = $this->dbExec($pdo, $sql, $params, $this->timeoutMs);
            $ms = (microtime(true) - $start) * 1000;
            $res = $stmt->fetchColumn(0);
            $this->dbEmit($ctx, $ms, 1);
            return (bool)$res;
        });
        return $runner($ctx);
    }

    public function count(): int
    {
        $pdo = $this->db->choosePdo('aggregate');
        [$whereSql, $params] = $this->compileWhere($pdo, true, true);
        $where = $whereSql ? ' WHERE ' . $whereSql : '';
        $sql = 'SELECT COUNT(*) FROM ' . $this->compileTable($pdo) . $where;
        $ctx = ['type' => 'aggregate', 'table' => $this->table];
        $runner = $this->dbRunner(function($ctx) use ($pdo, $sql, $params) {
            if ($this->db->isTestMode()) {
                $this->db->storeLast($sql, $params);
                return 0;
            }
            $start = microtime(true);
            $stmt = $this->dbExec($pdo, $sql, $params, $this->timeoutMs);
            $ms = (microtime(true) - $start) * 1000;
            $res = (int)$stmt->fetchColumn();
            $this->dbEmit($ctx, $ms, 1);
            return $res;
        });
        return $runner($ctx);
    }

    public function sum(string $col): float
    {
        $pdo = $this->db->choosePdo('aggregate');
        [$whereSql, $params] = $this->compileWhere($pdo, true, true);
        $where = $whereSql ? ' WHERE ' . $whereSql : '';
        $sql = 'SELECT SUM(' . $this->db->qi($col, $pdo) . ') FROM ' . $this->compileTable($pdo) . $where;
        $ctx = ['type' => 'aggregate', 'table' => $this->table];
        $runner = $this->dbRunner(function($ctx) use ($pdo, $sql, $params) {
            if ($this->db->isTestMode()) {
                $this->db->storeLast($sql, $params);
                return 0.0;
            }
            $start = microtime(true);
            $stmt = $this->dbExec($pdo, $sql, $params, $this->timeoutMs);
            $ms = (microtime(true) - $start) * 1000;
            $res = $stmt->fetchColumn();
            $this->dbEmit($ctx, $ms, 1);
            return $res === null ? 0.0 : (float)$res;
        });
        return $runner($ctx);
    }

    public function avg(string $col): float
    {
        $pdo = $this->db->choosePdo('aggregate');
        [$whereSql, $params] = $this->compileWhere($pdo, true, true);
        $where = $whereSql ? ' WHERE ' . $whereSql : '';
        $sql = 'SELECT AVG(' . $this->db->qi($col, $pdo) . ') FROM ' . $this->compileTable($pdo) . $where;
        $ctx = ['type' => 'aggregate', 'table' => $this->table];
        $runner = $this->dbRunner(function($ctx) use ($pdo, $sql, $params) {
            if ($this->db->isTestMode()) {
                $this->db->storeLast($sql, $params);
                return 0.0;
            }
            $start = microtime(true);
            $stmt = $this->dbExec($pdo, $sql, $params, $this->timeoutMs);
            $ms = (microtime(true) - $start) * 1000;
            $res = $stmt->fetchColumn();
            $this->dbEmit($ctx, $ms, 1);
            return $res === null ? 0.0 : (float)$res;
        });
        return $runner($ctx);
    }

    public function min(string $col): mixed
    {
        $pdo = $this->db->choosePdo('aggregate');
        [$whereSql, $params] = $this->compileWhere($pdo, true, true);
        $where = $whereSql ? ' WHERE ' . $whereSql : '';
        $sql = 'SELECT MIN(' . $this->db->qi($col, $pdo) . ') FROM ' . $this->compileTable($pdo) . $where;
        $ctx = ['type' => 'aggregate', 'table' => $this->table];
        $runner = $this->dbRunner(function($ctx) use ($pdo, $sql, $params) {
            if ($this->db->isTestMode()) {
                $this->db->storeLast($sql, $params);
                return null;
            }
            $start = microtime(true);
            $stmt = $this->dbExec($pdo, $sql, $params, $this->timeoutMs);
            $ms = (microtime(true) - $start) * 1000;
            $res = $stmt->fetchColumn();
            $this->dbEmit($ctx, $ms, 1);
            return $res;
        });
        return $runner($ctx);
    }

    public function max(string $col): mixed
    {
        $pdo = $this->db->choosePdo('aggregate');
        [$whereSql, $params] = $this->compileWhere($pdo, true, true);
        $where = $whereSql ? ' WHERE ' . $whereSql : '';
        $sql = 'SELECT MAX(' . $this->db->qi($col, $pdo) . ') FROM ' . $this->compileTable($pdo) . $where;
        $ctx = ['type' => 'aggregate', 'table' => $this->table];
        $runner = $this->dbRunner(function($ctx) use ($pdo, $sql, $params) {
            if ($this->db->isTestMode()) {
                $this->db->storeLast($sql, $params);
                return null;
            }
            $start = microtime(true);
            $stmt = $this->dbExec($pdo, $sql, $params, $this->timeoutMs);
            $ms = (microtime(true) - $start) * 1000;
            $res = $stmt->fetchColumn();
            $this->dbEmit($ctx, $ms, 1);
            return $res;
        });
        return $runner($ctx);
    }

    public function pluck(string $col, ?string $key = null): array
    {
        $pdo = $this->db->choosePdo('select');
        [$sql, $params] = $this->compileSelect($pdo);
        $ctx = ['type' => 'select', 'table' => $this->table];
        $runner = $this->dbRunner(function($ctx) use ($pdo, $sql, $params, $col, $key) {
            if ($this->db->isTestMode()) {
                $this->db->storeLast($sql, $params);
                return [];
            }
            $start = microtime(true);
            $stmt = $this->dbExec($pdo, $sql, $params, $this->timeoutMs);
            $ms = (microtime(true) - $start) * 1000;
            $res = [];
            if ($key) {
                while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
                    $res[$row[$key]] = $row[$col];
                }
            } else {
                while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
                    $res[] = $row[$col];
                }
            }
            $this->dbEmit($ctx, $ms, count($res));
            return $res;
        });
        return $runner($ctx);
    }

    public function insert(array $data): int
    {
        $this->assertWritable();
        $pdo = $this->db->choosePdo('insert');
        $cols = array_keys($data);
        $placeholders = implode(',', array_fill(0, count($cols), '?'));
        $sql = 'INSERT INTO ' . $this->compileTable($pdo) . ' (' . implode(',', array_map(fn($c) => $this->db->qi($c, $pdo), $cols)) . ') VALUES (' . $placeholders . ')';
        $params = array_values($data);
        $ctx = ['type' => 'insert', 'table' => $this->table];
        $runner = $this->dbRunner(function($ctx) use ($pdo, $sql, $params) {
            if ($this->db->isTestMode()) {
                $this->db->storeLast($sql, $params);
                return 0;
            }
            $start = microtime(true);
            $stmt = $this->dbExec($pdo, $sql, $params, $this->timeoutMs);
            $ms = (microtime(true) - $start) * 1000;
            $id = (int)$pdo->lastInsertId();
            $this->dbEmit($ctx, $ms, 1);
            return $id;
        });
        return $runner($ctx);
    }

    public function insertMany(array $rows): array
    {
        $this->assertWritable();
        $pdo = $this->db->choosePdo('insert');
        if (empty($rows)) return [];
        $cols = array_keys($rows[0]);
        $placeholders = '(' . implode(',', array_fill(0, count($cols), '?')) . ')';
        $sql = 'INSERT INTO ' . $this->compileTable($pdo) . ' (' . implode(',', array_map(fn($c) => $this->db->qi($c, $pdo), $cols)) . ') VALUES ' . implode(',', array_fill(0, count($rows), $placeholders));
        $params = [];
        foreach ($rows as $row) {
            $params = array_merge($params, array_values($row));
        }
        $ctx = ['type' => 'insert', 'table' => $this->table];
        $runner = $this->dbRunner(function($ctx) use ($pdo, $sql, $params) {
            if ($this->db->isTestMode()) {
                $this->db->storeLast($sql, $params);
                return [];
            }
            $start = microtime(true);
            $stmt = $this->dbExec($pdo, $sql, $params, $this->timeoutMs);
            $ms = (microtime(true) - $start) * 1000;
            $count = $stmt->rowCount();
            $ids = [];
            if ($count > 0) {
                $lastId = (int)$pdo->lastInsertId();
                $ids = range($lastId - $count + 1, $lastId);
            }
            $this->dbEmit($ctx, $ms, $count);
            return $ids;
        });
        return $runner($ctx);
    }

    public function insertGet(array $data, array $returning): array
    {
        $this->assertWritable();
        $pdo = $this->db->choosePdo('insert');
        $cols = array_keys($data);
        $placeholders = implode(',', array_fill(0, count($cols), '?'));
        $returnCols = implode(',', array_map(fn($c) => $this->db->qi($c, $pdo), $returning));
        $driver = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
        $sql = 'INSERT INTO ' . $this->compileTable($pdo) . ' (' . implode(',', array_map(fn($c) => $this->db->qi($c, $pdo), $cols)) . ') VALUES (' . $placeholders . ')';
        if ($driver === 'pgsql' || $driver === 'sqlite') {
            $sql .= ' RETURNING ' . $returnCols;
        }
        $params = array_values($data);
        $ctx = ['type' => 'insert', 'table' => $this->table];
        $runner = $this->dbRunner(function($ctx) use ($pdo, $sql, $params, $driver, $returning) {
            if ($this->db->isTestMode()) {
                $this->db->storeLast($sql, $params);
                return [];
            }
            $start = microtime(true);
            $stmt = $this->dbExec($pdo, $sql, $params, $this->timeoutMs);
            $ms = (microtime(true) - $start) * 1000;
            if ($driver === 'pgsql' || $driver === 'sqlite') {
                $res = $stmt->fetch(PDO::FETCH_ASSOC);
            } else {
                $id = (int)$pdo->lastInsertId();
                $res = $this->select($returning)->where('id', '=', $id)->first();
            }
            $this->dbEmit($ctx, $ms, 1);
            return $res ?: [];
        });
        return $runner($ctx);
    }

    public function update(array $data): int
    {
        $this->assertWritable();
        $pdo = $this->db->choosePdo('update');
        $sets = [];
        $params = [];
        foreach ($data as $col => $val) {
            $sets[] = $this->db->qi($col, $pdo) . ' = ?';
            $params[] = $val;
        }
        $setClause = implode(',', $sets);
        [$whereSql, $whereParams] = $this->compileWhere($pdo, true);
        $where = $whereSql ? ' WHERE ' . $whereSql : '';
        $sql = 'UPDATE ' . $this->compileTable($pdo) . ' SET ' . $setClause . $where;
        $params = array_merge($params, $whereParams);
        $ctx = ['type' => 'update', 'table' => $this->table];
        $runner = $this->dbRunner(function($ctx) use ($pdo, $sql, $params) {
            if ($this->db->isTestMode()) {
                $this->db->storeLast($sql, $params);
                return 0;
            }
            $start = microtime(true);
            $stmt = $this->dbExec($pdo, $sql, $params, $this->timeoutMs);
            $ms = (microtime(true) - $start) * 1000;
            $count = $stmt->rowCount();
            $this->dbEmit($ctx, $ms, $count);
            return $count;
        });
        return $runner($ctx);
    }

    public function delete(): int
    {
        $this->assertWritable();
        if ($this->softDelete['enabled'] && $this->hasColumn($this->softDelete['column'])) {
            return $this->softDelete();
        }
        $pdo = $this->db->choosePdo('delete');
        [$whereSql, $params] = $this->compileWhere($pdo, true);
        $where = $whereSql ? ' WHERE ' . $whereSql : '';
        $sql = 'DELETE FROM ' . $this->compileTable($pdo) . $where;
        $ctx = ['type' => 'delete', 'table' => $this->table];
        $runner = $this->dbRunner(function($ctx) use ($pdo, $sql, $params) {
            if ($this->db->isTestMode()) {
                $this->db->storeLast($sql, $params);
                return 0;
            }
            $start = microtime(true);
            $stmt = $this->dbExec($pdo, $sql, $params, $this->timeoutMs);
            $ms = (microtime(true) - $start) * 1000;
            $count = $stmt->rowCount();
            $this->dbEmit($ctx, $ms, $count);
            return $count;
        });
        return $runner($ctx);
    }

    private function softDelete(): int
    {
        $col = $this->softDelete['column'];
        $val = $this->softDelete['mode'] === 'timestamp' ? date('c') : $this->softDelete['deleted_value'];
        return $this->update([$col => $val]);
    }

    public function restore(): int
    {
        $this->assertWritable();
        if (!$this->softDelete['enabled'] || !$this->hasColumn($this->softDelete['column'])) return 0;
        $col = $this->softDelete['column'];
        $val = $this->softDelete['mode'] === 'timestamp' ? null : 0;
        $this->onlyTrashed = true;
        return $this->update([$col => $val]);
    }

    public function forceDelete(): int
    {
        $this->assertWritable();
        $this->withTrashed = true;
        return $this->delete();
    }

    public function upsert(array $data, array $conflict, array $updateColumns): int
    {
        $this->assertWritable();
        $pdo = $this->db->choosePdo('insert');
        $driver = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
        $cols = array_keys($data);
        $placeholders = implode(',', array_fill(0, count($cols), '?'));
        $conflictCols = implode(',', array_map(fn($c) => $this->db->qi($c, $pdo), $conflict));
        $updateSets = [];
        $params = array_values($data);
        foreach ($updateColumns as $col) {
            $updateSets[] = $this->db->qi($col, $pdo) . ' = EXCLUDED.' . $this->db->qi($col, $pdo);
        }
        $updateClause = implode(',', $updateSets);

        if ($driver === 'pgsql') {
            $sql = 'INSERT INTO ' . $this->compileTable($pdo) . ' (' . implode(',', array_map(fn($c) => $this->db->qi($c, $pdo), $cols)) . ') VALUES (' . $placeholders . ') ON CONFLICT (' . $conflictCols . ') DO UPDATE SET ' . $updateClause . ' RETURNING id';
        } elseif ($driver === 'mysql') {
            $sql = 'INSERT INTO ' . $this->compileTable($pdo) . ' (' . implode(',', array_map(fn($c) => $this->db->qi($c, $pdo), $cols)) . ') VALUES (' . $placeholders . ') ON DUPLICATE KEY UPDATE ' . $updateClause;
        } elseif ($driver === 'sqlite') {
            $sql = 'INSERT INTO ' . $this->compileTable($pdo) . ' (' . implode(',', array_map(fn($c) => $this->db->qi($c, $pdo), $cols)) . ') VALUES (' . $placeholders . ') ON CONFLICT (' . $conflictCols . ') DO UPDATE SET ' . $updateClause . ' RETURNING id';
        } else {
            $sql = 'INSERT INTO ' . $this->compileTable($pdo) . ' (' . implode(',', array_map(fn($c) => $this->db->qi($c, $pdo), $cols)) . ') VALUES (' . $placeholders . ')';
            $params = array_values($data);
        }

        $ctx = ['type' => 'insert', 'table' => $this->table];
        $runner = $this->dbRunner(function($ctx) use ($pdo, $sql, $params) {
            if ($this->db->isTestMode()) {
                $this->db->storeLast($sql, $params);
                return 0;
            }
            $start = microtime(true);
            $stmt = $this->dbExec($pdo, $sql, $params, $this->timeoutMs);
            $ms = (microtime(true) - $start) * 1000;
            $count = $stmt->rowCount();
            $this->dbEmit($ctx, $ms, $count);
            return $count;
        });
        return $runner($ctx);
    }

    public function getKeyset(?string $cursor, string $key): array
    {
        $pdo = $this->db->choosePdo('select');
        if ($cursor) {
            $decoded = json_decode(base64_decode($cursor), true);
            if ($decoded && isset($decoded['last'])) {
                $last = $decoded['last'];
                $this->where($key, '>', $last);
            }
        }
        $rows = $this->get();
        $next = null;
        if ($rows && count($rows) === ($this->limit ?? PHP_INT_MAX)) {
            $last = end($rows)[$key] ?? null;
            if ($last !== null) {
                $next = base64_encode(json_encode(['last' => $last]));
            }
        }
        return ['data' => $rows, 'next' => $next];
    }

    public function chunk(int $size, callable $callback): void
    {
        if ($size <= 0) throw new \InvalidArgumentException('Chunk size must be positive');
        $offset = 0;
        while (true) {
            $chunk = (clone $this)->offset($offset)->limit($size)->get();
            if (empty($chunk)) break;
            $callback($chunk);
            $offset += $size;
        }
    }

    public function stream(): \Generator
    {
        $pdo = $this->db->choosePdo('select');
        [$sql, $params] = $this->compileSelect($pdo);
        $ctx = ['type' => 'select', 'table' => $this->table];
        $runner = $this->dbRunner(function($ctx) use ($pdo, $sql, $params) {
            if ($this->db->isTestMode()) {
                $this->db->storeLast($sql, $params);
                return (function() { yield from []; })();
            }
            $start = microtime(true);
            $stmt = $this->dbExec($pdo, $sql, $params, $this->timeoutMs);
            $ms = (microtime(true) - $start) * 1000;
            $count = 0;
            while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
                $count++;
                yield $row;
            }
            $this->dbEmit($ctx, $ms, $count);
        });
        yield from $runner($ctx);
    }

    public function whereJson(string $path, string $op, mixed $val, bool $or = false): self
    {
        $this->wheres[] = [
            'type' => 'json',
            'bool' => $or ? 'OR' : 'AND',
            'path' => $path,
            'op' => $op,
            'val' => $val
        ];
        return $this;
    }

    public function cast(string $expr): string
    {
        return 'CAST(' . $expr . ')';
    }

    public function jsonSet(string $col, array $updates): self
    {
        $pdo = $this->db->choosePdo('update');
        $driver = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
        $updatesSql = [];
        $params = [];
        foreach ($updates as $path => $val) {
            if ($driver === 'mysql') {
                $updatesSql[] = $this->db->qi($col, $pdo) . ' = JSON_SET(' . $this->db->qi($col, $pdo) . ', \'$.' . $path . '\', ?)';
                $params[] = $val;
            } elseif ($driver === 'pgsql') {
                $updatesSql[] = $this->db->qi($col, $pdo) . ' = JSONB_SET(' . $this->db->qi($col, $pdo) . ', \'{' . str_replace('.', ',', $path) . '}\', ?)';
                $params[] = json_encode($val);
            } else {
                throw new \RuntimeException('jsonSet not supported on ' . $driver);
            }
        }
        $sql = 'UPDATE ' . $this->compileTable($pdo) . ' SET ' . implode(',', $updatesSql);
        [$whereSql, $whereParams] = $this->compileWhere($pdo, true);
        if ($whereSql) $sql .= ' WHERE ' . $whereSql;
        $params = array_merge($params, $whereParams);
        $ctx = ['type' => 'update', 'table' => $this->table];
        $runner = $this->dbRunner(function($ctx) use ($pdo, $sql, $params) {
            if ($this->db->isTestMode()) {
                $this->db->storeLast($sql, $params);
                return $this;
            }
            $start = microtime(true);
            $stmt = $this->dbExec($pdo, $sql, $params, $this->timeoutMs);
            $ms = (microtime(true) - $start) * 1000;
            $count = $stmt->rowCount();
            $this->dbEmit($ctx, $ms, $count);
            return $this;
        });
        return $runner($ctx);
    }
}

if (php_sapi_name() === 'cli' && realpath($argv[0] ?? '') === __FILE__) {
    \ndtan\DBF::cli($argv);
}