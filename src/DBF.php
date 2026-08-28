<?php
/**
 * NDT DBF - Simple, Lightweight PHP Database Framework (Enterprise+)
 *
 * @version   0.2.0
 * @package   NDT DBF
 * @description Single-file, secure PHP Database Framework with PRO & Advanced++ features.
 * @author    Tony Nguyen
 * @link      https://ndtan.net
 * @license   MIT
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
    public const VERSION = '0.2.0';
    private PDO $pdoWrite;
    private ?PDO $pdoRead = null;
    private string $driverWrite;
    private ?string $driverRead = null;
    private string $prefix = '';
    private bool $readonly = false;
    private bool $testMode = false; // Added to fix TypeError

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

    /** @var int Current write transaction nesting depth. */
    private int $transactionDepth = 0;
    private ?PDO $transactionPdo = null;
    private int $savepointCounter = 0;

    /** @var array Soft delete configuration */
    private array $softDelete = [
        'enabled' => false,
        'column'  => 'deleted_at',
        'mode'    => 'timestamp',
        'deleted_value' => 1,
    ];

    /** @var array Last query tracking for debugging */
    private string $lastQueryString = '';
    private array $lastQueryParams = [];

    public function __construct(string|array|null $configOrUri = null)
    {
        if ($configOrUri === null) {
            $env = getenv('NDTAN_DBF_URL') ?: throw new \InvalidArgumentException('No configuration provided. Pass URI/array/PDO or set NDTAN_DBF_URL.');
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
        $user = isset($parsed['user']) ? rawurldecode($parsed['user']) : '';
        $pass = isset($parsed['pass']) ? rawurldecode($parsed['pass']) : '';
        $host = $parsed['host'] ?? 'localhost';
        $port = $parsed['port'] ?? ($driver === 'pgsql' ? 5432 : 3306);
        $db = $parsed['path'] ?? '/app';
        if ($driver !== 'sqlite') $db = ltrim($db, '/');
        if ($driver === 'sqlite' && $db === '/:memory:') $db = ':memory:';
        if ($driver === 'sqlite' && $db === '') $db = ':memory:';
        $query = [];
        parse_str($parsed['query'] ?? '', $query);
        $charset = $query['charset'] ?? 'utf8mb4';
        $attrs = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::ATTR_EMULATE_PREPARES => false,
        ];

        switch ($driver) {
            case 'mysql':
                $pdo = new PDO("mysql:host={$host};port={$port};dbname={$db};charset={$charset}", $user, $pass, $attrs);
                return [$pdo, 'mysql'];
            case 'pgsql':
                $pdo = new PDO("pgsql:host={$host};port={$port};dbname={$db}", $user, $pass, $attrs);
                return [$pdo, 'pgsql'];
            case 'sqlite':
                $pdo = new PDO("sqlite:{$db}", null, null, $attrs);
                try {
                    @$pdo->exec('PRAGMA foreign_keys = ON'); // Suppress warning
                    @$pdo->exec('PRAGMA journal_mode = WAL'); // Suppress warning
                } catch (Throwable $e) {
                    throw new \RuntimeException("SQLite PRAGMA failed: " . $e->getMessage());
                }
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

    private function connectFromArray(array|string|PDO $config): array
    {
        if ($config instanceof PDO) {
            $config->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            return [$config, (string)$config->getAttribute(PDO::ATTR_DRIVER_NAME)];
        }
        if (is_string($config)) return $this->connectFromUri($config);

        $driver = $config['type'] ?? 'mysql';
        $attrs = [
            PDO::ATTR_ERRMODE => $config['error'] ?? PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::ATTR_EMULATE_PREPARES => $config['emulate_prepares'] ?? false,
        ];
        if (isset($config['options']) && is_array($config['options'])) {
            $attrs = $config['options'] + $attrs;
        } elseif (isset($config['option']) && is_array($config['option'])) {
            $attrs = $config['option'] + $attrs;
        }

        if (isset($config['pdo']) && $config['pdo'] instanceof PDO) {
            $config['pdo']->setAttribute(PDO::ATTR_ERRMODE, $attrs[PDO::ATTR_ERRMODE]);
            return [$config['pdo'], (string)$config['pdo']->getAttribute(PDO::ATTR_DRIVER_NAME)];
        }

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
                @$pdo->exec('PRAGMA foreign_keys = ON'); // Suppress warning
                @$pdo->exec('PRAGMA journal_mode = WAL'); // Suppress warning
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
        if (!isset($this->pdoWrite)) {
            if ($this->pdoRead) {
                $this->pdoWrite = $this->pdoRead;
                $this->driverWrite = $this->driverRead;
            } else {
                throw new \InvalidArgumentException('A write or read connection is required.');
            }
        }
        $this->routing = $config['routing'] ?? 'auto';
        if (!in_array($this->routing, ['single', 'auto', 'manual'], true)) {
            throw new \InvalidArgumentException('Invalid routing mode.');
        }
    }

    public function qi(string $identifier, PDO $pdo): string
    {
        $identifier = trim($identifier);
        if ($identifier === '*') return '*';
        if ($identifier === '' || str_contains($identifier, "\0")) {
            throw new \InvalidArgumentException('Invalid SQL identifier.');
        }
        $driver = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
        $quote = match ($driver) {
            'mysql' => ['`', '`'],
            'sqlsrv' => ['[', ']'],
            default => ['"', '"'],
        };
        $parts = explode('.', $identifier);
        foreach ($parts as $part) {
            if ($part === '*' && count($parts) > 1) continue;
            if (!preg_match('/^[A-Za-z_][A-Za-z0-9_$]*$|^\d+$/', $part)) {
                throw new \InvalidArgumentException("Invalid SQL identifier segment: {$part}");
            }
        }
        return implode('.', array_map(fn($part) => $part === '*' ? '*' : $quote[0] . $part . $quote[1], $parts));
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

    public function getLogger(): ?callable
    {
        return $this->logger;
    }

    public function hasUniqueConstraint(string $table, array $columns): bool
    {
        if (!preg_match('/^[A-Za-z_][A-Za-z0-9_$]*(?:\.[A-Za-z_][A-Za-z0-9_$]*)?$/', $table)) {
            throw new \InvalidArgumentException('Invalid table name.');
        }
        $pdo = $this->pdoWrite;
        $driver = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
        $fullTable = $this->prefix . $table;
        try {
            switch ($driver) {
                case 'sqlite':
                    $stmt = @$pdo->query("PRAGMA index_list('$fullTable')"); // Suppress warning
                    if (!$stmt) return false;
                    $indexes = $stmt->fetchAll(PDO::FETCH_ASSOC);
                    foreach ($indexes as $index) {
                        if ($index['unique']) {
                            $stmt = @$pdo->query("PRAGMA index_info('{$index['name']}')"); // Suppress warning
                            if (!$stmt) continue;
                            $indexCols = array_column($stmt->fetchAll(), 'name');
                            if (count(array_intersect($indexCols, $columns)) === count($columns)) {
                                return true;
                            }
                        }
                    }
                    break;
                case 'mysql':
                    $stmt = @$pdo->query("SHOW INDEXES FROM `$fullTable` WHERE Key_name != 'PRIMARY' AND Non_unique = 0"); // Suppress warning
                    if (!$stmt) return false;
                    $indexes = $stmt->fetchAll(PDO::FETCH_ASSOC);
                    $indexCols = array_column($indexes, 'Column_name');
                    if (count(array_intersect($indexCols, $columns)) === count($columns)) {
                        return true;
                    }
                    break;
                case 'pgsql':
                    $stmt = @$pdo->prepare("SELECT indexdef FROM pg_indexes WHERE tablename = ? AND indexdef LIKE '%UNIQUE%'"); // Suppress warning
                    if (!$stmt) return false;
                    $stmt->execute([$fullTable]);
                    $indexes = $stmt->fetchAll(PDO::FETCH_COLUMN);
                    foreach ($indexes as $index) {
                        foreach ($columns as $col) {
                            if (strpos($index, $col) !== false) {
                                return true;
                            }
                        }
                    }
                    break;
            }
        } catch (Throwable $e) {
            // Silent catch to avoid warnings
        }
        return false;
    }

    public function execPreparedOn(PDO $pdo, string $sql, array $params, int $timeoutMs = 0): PDOStatement
    {
        if ($timeoutMs > 0) {
            $driver = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
            if ($driver === 'mysql') @$pdo->exec("SET SESSION MAX_EXECUTION_TIME = {$timeoutMs}"); // Suppress warning
            if ($driver === 'pgsql') @$pdo->exec("SET LOCAL statement_timeout = {$timeoutMs}"); // Suppress warning
            if ($driver === 'sqlite') @$pdo->exec("PRAGMA busy_timeout = {$timeoutMs}"); // Suppress warning
        }
        $cacheKey = spl_object_id($pdo) . ':' . $sql;
        if (isset($this->stmtCache[$cacheKey])) {
            $stmt = $this->stmtCache[$cacheKey];
            try {
                $stmt->execute($params);
            } catch (Throwable) {
                unset($this->stmtCache[$cacheKey]);
                $stmt = $pdo->prepare($sql);
                $stmt->execute($params);
                $this->stmtCache[$cacheKey] = $stmt;
            }
        } else {
            $stmt = $pdo->prepare($sql);
            $stmt->execute($params);
            // Keep the cache bounded; SQL generated from user-selected columns
            // must not be allowed to grow this array without limit.
            if (count($this->stmtCache) >= 256) array_shift($this->stmtCache);
            $this->stmtCache[$cacheKey] = $stmt;
        }
        return $stmt;
    }

    public function emitMetrics(array $ctx, float $ms, int $count): void
    {
        if ($this->metrics) call_user_func($this->metrics, array_merge($ctx, ['ms' => $ms, 'count' => $count]));
    }

    public function choosePdo(string $type): PDO
    {
        if ($this->transactionPdo) return $this->transactionPdo;
        if ($this->routing === 'single') return $this->pdoWrite;
        if ($this->routing === 'manual') return $this->currentRoute === 'read' && $this->pdoRead ? $this->pdoRead : $this->pdoWrite;
        return in_array($type, ['select', 'aggregate', 'raw_read'], true) && $this->pdoRead ? $this->pdoRead : $this->pdoWrite;
    }

    public function getColumns(string $table, ?PDO $pdo = null): array
    {
        $pdo ??= $this->pdoWrite;
        if (!preg_match('/^[A-Za-z_][A-Za-z0-9_$]*(?:\.[A-Za-z_][A-Za-z0-9_$]*)?$/', $table)) {
            throw new \InvalidArgumentException('Invalid table name.');
        }
        $key = spl_object_id($pdo) . ':' . $pdo->getAttribute(PDO::ATTR_DRIVER_NAME) . ':' . $this->prefix . $table;
        if (isset($this->schemaCache[$key])) return $this->schemaCache[$key];
        $driver = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
        $fullTable = $this->prefix . $table;
        $cols = [];
        try {
            switch ($driver) {
                case 'sqlite':
                    $stmt = $pdo->query("PRAGMA table_info('$fullTable')");
                    $cols = $stmt ? array_column($stmt->fetchAll(), 'name') : [];
                    break;
                case 'mysql':
                    $stmt = $pdo->query("SHOW COLUMNS FROM `$fullTable`");
                    $cols = $stmt ? array_column($stmt->fetchAll(), 'Field') : [];
                    break;
                case 'pgsql':
                    $stmt = $pdo->prepare("SELECT column_name FROM information_schema.columns WHERE table_name = ?");
                    if ($stmt) {
                        $stmt->execute([$fullTable]);
                        $cols = array_column($stmt->fetchAll(), 'column_name');
                    }
                    break;
                case 'sqlsrv':
                    $stmt = $pdo->prepare("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?");
                    if ($stmt) {
                        $stmt->execute([$fullTable]);
                        $cols = array_column($stmt->fetchAll(), 'COLUMN_NAME');
                    }
                    break;
                case 'oracle':
                    $stmt = $pdo->prepare("SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = UPPER(?)");
                    if ($stmt) {
                        $stmt->execute([$fullTable]);
                        $cols = array_column($stmt->fetchAll(), 'COLUMN_NAME');
                    }
                    break;
            }
        } catch (Throwable $e) {
            throw new \RuntimeException("Failed to get columns for table '$fullTable': " . $e->getMessage());
        }
        $this->schemaCache[$key] = $cols;
        return $cols;
    }

    public function tx(callable $fn, int $attempts = 3): mixed
    {
        if ($attempts < 1) throw new \InvalidArgumentException('Transaction attempts must be positive.');
        if ($this->transactionDepth > 0) {
            $savepoint = 'ndtan_sp_' . (++$this->savepointCounter);
            $this->pdoWrite->exec('SAVEPOINT ' . $savepoint);
            ++$this->transactionDepth;
            try {
                $result = $fn($this);
                $this->pdoWrite->exec('RELEASE SAVEPOINT ' . $savepoint);
                --$this->transactionDepth;
                return $result;
            } catch (Throwable $e) {
                $this->pdoWrite->exec('ROLLBACK TO SAVEPOINT ' . $savepoint);
                $this->pdoWrite->exec('RELEASE SAVEPOINT ' . $savepoint);
                --$this->transactionDepth;
                throw $e;
            }
        }
        for ($i = 1; $i <= $attempts; $i++) {
            try {
                $this->pdoWrite->beginTransaction();
                $this->transactionPdo = $this->pdoWrite;
                $this->transactionDepth = 1;
                $res = $fn($this);
                $this->pdoWrite->commit();
                $this->transactionDepth = 0;
                $this->transactionPdo = null;
                return $res;
            } catch (Throwable $e) {
                if ($this->pdoWrite->inTransaction()) $this->pdoWrite->rollBack();
                $this->transactionDepth = 0;
                $this->transactionPdo = null;
                if ($i === $attempts || !in_array($e->getCode(), [40001, '40001', '1213'])) throw $e;
                usleep((2 ** $i) * 100000 + mt_rand(0, 100000));
            }
        }
        return null;
    }

    public function using(?string $route): self
    {
        if ($this->routing !== 'manual') throw new \RuntimeException('Using only for manual routing');
        $route = $route ?? 'write';
        if (!in_array($route, ['write', 'read'], true)) throw new \InvalidArgumentException('Route must be write or read.');
        $this->currentRoute = $route;
        if ($this->currentRoute === 'read' && !$this->pdoRead) throw new \RuntimeException('Read connection is not configured.');
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
        $clone->scope = array_merge($this->scope, $scope);
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
            'driver_read' => $this->driverRead,
            'version' => self::VERSION,
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

    public function raw(string $sql, array $params = []): array|int
    {
        $verb = strtoupper(strtok(ltrim($sql), " \t\r\n") ?: '');
        $isRead = in_array($verb, ['SELECT', 'WITH', 'SHOW', 'DESCRIBE', 'DESC', 'EXPLAIN', 'PRAGMA'], true);
        if (!$isRead && $this->readonly) throw new \RuntimeException('Readonly mode: raw write operation blocked.');
        $pdo = $this->choosePdo($isRead ? 'raw_read' : 'raw');
        $ctx = ['type' => $isRead ? 'select' : 'statement', 'sql' => $sql];
        $runner = $this->dbBuildRunner(function($ctx) use ($pdo, $sql, $params) {
            if ($this->isTestMode()) {
                $this->storeLast($sql, $params);
                return ($ctx['type'] ?? '') === 'select' ? [] : 0;
            }
            $start = microtime(true);
            $stmt = $this->execPreparedOn($pdo, $sql, $params);
            $ms = (microtime(true) - $start) * 1000;
            if ($this->getLogger()) call_user_func($this->getLogger(), $sql, $params, $ms);
            if (($ctx['type'] ?? '') === 'select') {
                $res = $stmt->fetchAll();
                $this->emitMetrics($ctx, $ms, count($res));
                return $res;
            }
            $count = $stmt->rowCount();
            $this->emitMetrics($ctx, $ms, $count);
            return $count;
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
        if ($this->getLogger()) call_user_func($this->getLogger(), $sql, $params, 0.0);
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
        $op = $this->normalizeOperator($op);
        if ($val === null && in_array($op, ['=', '!=', '<>'], true)) {
            return $this->whereNull($col, $op === '!=', $or);
        }
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
        $type = strtoupper(trim($type));
        if (!in_array($type, ['INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS'], true)) {
            throw new \InvalidArgumentException('Unsupported join type.');
        }
        $op = $this->normalizeOperator($op);
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
        $op = $this->normalizeOperator($op);
        $this->havings[] = [
            'expr' => $expr,
            'op' => $op,
            'val' => $val
        ];
        return $this;
    }

    public function orderBy(string $col, string $dir = 'asc'): self
    {
        $dir = strtoupper(trim($dir));
        if (!in_array($dir, ['ASC', 'DESC'], true)) throw new \InvalidArgumentException('Order direction must be ASC or DESC.');
        $this->orders[] = [$col, $dir];
        return $this;
    }

    public function limit(int $n): self
    {
        if ($n < 0) throw new \InvalidArgumentException('Limit must be non-negative.');
        $this->limit = $n;
        return $this;
    }

    public function offset(int $n): self
    {
        if ($n < 0) throw new \InvalidArgumentException('Offset must be non-negative.');
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
        $this->withTrashed = true;
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
        $selectCols = array_map(fn($c) => $this->compileSelectColumn((string)$c, $pdo), $this->select);
        $select = 'SELECT ' . implode(', ', $selectCols);
        $from = ' FROM ' . $this->compileTable($pdo);
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
                $hParts[] = $this->compileExpression($h['expr'], $pdo) . ' ' . $h['op'] . ' ?';
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
        $driver = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
        $locking = '';
        if ($this->forUpdate) {
            if (in_array($driver, ['mysql', 'pgsql'], true)) {
                $locking = ' FOR UPDATE' . ($this->skipLocked ? ' SKIP LOCKED' : '');
            } else {
                throw new \RuntimeException("forUpdate is not supported by {$driver}.");
            }
        } elseif ($this->skipLocked) {
            throw new \LogicException('skipLocked requires forUpdate.');
        }
        $sql = $select . $from . $join . $where . $group . $having . $order . $limit . $offset . $locking;
        return [$sql, $bind];
    }

    private function compileSelectColumn(string $column, PDO $pdo): string
    {
        $column = trim($column);
        if ($column === '*') return '*';
        if (preg_match('/^(COUNT|SUM|AVG|MIN|MAX)\s*\(\s*([A-Za-z_][A-Za-z0-9_$]*(?:\.[A-Za-z_][A-Za-z0-9_$]*)?|\*)\s*\)(?:\s+AS\s+([A-Za-z_][A-Za-z0-9_$]*))?$/i', $column, $m)) {
            $result = strtoupper($m[1]) . '(' . $this->db->qi($m[2], $pdo) . ')';
            return !empty($m[3]) ? $result . ' AS ' . $this->db->qi($m[3], $pdo) : $result;
        }
        if (preg_match('/^([A-Za-z_][A-Za-z0-9_$]*(?:\.[A-Za-z_][A-Za-z0-9_$]*)?)\s+AS\s+([A-Za-z_][A-Za-z0-9_$]*)$/i', $column, $m)) {
            return $this->db->qi($m[1], $pdo) . ' AS ' . $this->db->qi($m[2], $pdo);
        }
        return $this->db->qi($column, $pdo);
    }

    private function compileExpression(string $expression, PDO $pdo): string
    {
        $expression = trim($expression);
        if (preg_match('/^(COUNT|SUM|AVG|MIN|MAX)\s*\(\s*([A-Za-z_][A-Za-z0-9_$]*(?:\.[A-Za-z_][A-Za-z0-9_$]*)?|\*)\s*\)$/i', $expression, $m)) {
            return strtoupper($m[1]) . '(' . $this->db->qi($m[2], $pdo) . ')';
        }
        return $this->db->qi($expression, $pdo);
    }

    private function normalizeOperator(string $operator): string
    {
        $operator = strtoupper(trim($operator));
        if (!in_array($operator, ['=', '==', '!=', '<>', '<', '<=', '>', '>=', 'LIKE', 'NOT LIKE', 'ILIKE', 'NOT ILIKE'], true)) {
            throw new \InvalidArgumentException('Unsupported SQL operator.');
        }
        return $operator === '==' ? '=' : $operator;
    }

    private function compileWhere(PDO $pdo, bool $includeScope, bool $forSelect = false): array
    {
        $bind = [];
        $non_user_conditions = [];
        $driver = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
        $sdCol = $this->softDelete['column'];

        // Apply onlyTrashed for restore operations
        if ($this->onlyTrashed && $this->softDelete['enabled'] && $this->hasColumn($sdCol, $pdo)) {
            $sdColQuoted = $this->db->qi($sdCol, $pdo);
            $non_user_conditions[] = $sdColQuoted . ($this->softDelete['mode'] === 'timestamp' ? ' IS NOT NULL' : ' = ' . $this->softDelete['deleted_value']);
        }

        // Apply scope
        if ($includeScope && $this->scope) {
            foreach ($this->scope as $k => $v) {
                $non_user_conditions[] = $this->db->qi($k, $pdo) . ' = ?';
                $bind[] = $v;
            }
        }

        // Apply soft delete for select queries (unless withTrashed or onlyTrashed)
        if ($forSelect && $this->softDelete['enabled'] && $this->hasColumn($sdCol, $pdo) && !$this->withTrashed && !$this->onlyTrashed) {
            $sdColQuoted = $this->db->qi($sdCol, $pdo);
            $non_user_conditions[] = $sdColQuoted . ($this->softDelete['mode'] === 'timestamp' ? ' IS NULL' : ' = 0');
        }

        // Join non-user conditions with AND
        $baseWhere = $non_user_conditions ? implode(' AND ', $non_user_conditions) : '';

        // User predicates are grouped so tenant/scope and soft-delete guards
        // cannot be bypassed by an OR predicate.
        $userParts = [];

        // Apply user-defined wheres
        foreach ($this->wheres as $idx => $w) {
            $prefix = empty($userParts) ? '' : ' ' . $w['bool'] . ' ';
            switch ($w['type']) {
                case 'basic':
                    $userParts[] = $prefix . $this->db->qi($w['col'], $pdo) . ' ' . $w['op'] . ' ?';
                    $bind[] = $w['val'];
                    break;
                case 'in':
                    $vals = $w['vals'];
                    if (count($vals) > $this->db->guardMaxIn()) throw new \LengthException('whereIn list exceeds ' . $this->db->guardMaxIn() . ' items');
                    if (empty($vals)) {
                        $userParts[] = $prefix . ($w['not'] ? '1=1' : '1=0');
                        break;
                    }
                    $qs = implode(',', array_fill(0, count($vals), '?'));
                    $userParts[] = $prefix . $this->db->qi($w['col'], $pdo) . ($w['not'] ? ' NOT IN (' : ' IN (') . $qs . ')';
                    $bind = array_merge($bind, array_values($vals));
                    break;
                case 'null':
                    $userParts[] = $prefix . $this->db->qi($w['col'], $pdo) . ($w['not'] ? ' IS NOT NULL' : ' IS NULL');
                    break;
                case 'between':
                    $pair = $w['pair'];
                    if (!is_array($pair) || count($pair) !== 2) throw new \InvalidArgumentException('whereBetween requires [min,max]');
                    $userParts[] = $prefix . $this->db->qi($w['col'], $pdo) . ($w['not'] ? ' NOT BETWEEN ? AND ?' : ' BETWEEN ? AND ?');
                    $bind[] = $pair[0];
                    $bind[] = $pair[1];
                    break;
                case 'json':
                    $jsonPath = explode('->', $w['path']);
                    $col = array_shift($jsonPath);
                    if (!$jsonPath || !preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $col) || count(array_filter($jsonPath, fn($part) => !preg_match('/^[A-Za-z0-9_]+$/', $part)))) {
                        throw new \InvalidArgumentException('Invalid JSON path.');
                    }
                    if ($driver === 'mysql') {
                        $path = implode('.', $jsonPath);
                        $userParts[] = $prefix . 'JSON_UNQUOTE(JSON_EXTRACT(' . $this->db->qi($col, $pdo) . ", '$.{$path}')) " . $w['op'] . ' ?';
                    } elseif ($driver === 'pgsql') {
                        $expr = $this->db->qi($col, $pdo);
                        foreach ($jsonPath as $part) $expr .= "->>'{$part}'";
                        $userParts[] = $prefix . $expr . ' ' . $w['op'] . ' ?';
                    } elseif ($driver === 'sqlite') {
                        $path = implode('.', $jsonPath);
                        $userParts[] = $prefix . 'json_extract(' . $this->db->qi($col, $pdo) . ", '$.{$path}') " . $w['op'] . ' ?';
                    } else {
                        throw new \RuntimeException('whereJson not supported on ' . $driver);
                    }
                    $bind[] = $w['val'];
                    break;
            }
        }

        $userSql = $userParts ? '(' . implode('', $userParts) . ')' : '';
        $sql = $baseWhere && $userSql ? $baseWhere . ' AND ' . $userSql : ($baseWhere ?: $userSql);
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

    private function hasColumn(string $column, ?PDO $pdo = null): bool
    {
        $pdo ??= $this->db->choosePdo('select');
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
            if ($this->db->getLogger()) call_user_func($this->db->getLogger(), $sql, $params, $ms);
            $res = $stmt->fetchAll(PDO::FETCH_ASSOC);
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

    public function sum(string $col): mixed
    {
        $pdo = $this->db->choosePdo('aggregate');
        [$whereSql, $params] = $this->compileWhere($pdo, true, true);
        $where = $whereSql ? ' WHERE ' . $whereSql : '';
        $sql = 'SELECT SUM(' . $this->db->qi($col, $pdo) . ') FROM ' . $this->compileTable($pdo) . $where;
        $ctx = ['type' => 'aggregate', 'table' => $this->table];
        $runner = $this->dbRunner(function($ctx) use ($pdo, $sql, $params) {
            if ($this->db->isTestMode()) {
                $this->db->storeLast($sql, $params);
                return 0;
            }
            $start = microtime(true);
            $stmt = $this->dbExec($pdo, $sql, $params, $this->timeoutMs);
            $ms = (microtime(true) - $start) * 1000;
            $res = $stmt->fetchColumn();
            $this->dbEmit($ctx, $ms, 1);
            $result = $res === null ? 0 : $res;
            return is_float($result) && floor($result) == $result ? (int)$result : (float)$result;
        });
        return $runner($ctx);
    }

    public function avg(string $col): mixed
    {
        $pdo = $this->db->choosePdo('aggregate');
        [$whereSql, $params] = $this->compileWhere($pdo, true, true);
        $where = $whereSql ? ' WHERE ' . $whereSql : '';
        $sql = 'SELECT AVG(' . $this->db->qi($col, $pdo) . ') FROM ' . $this->compileTable($pdo) . $where;
        $ctx = ['type' => 'aggregate', 'table' => $this->table];
        $runner = $this->dbRunner(function($ctx) use ($pdo, $sql, $params) {
            if ($this->db->isTestMode()) {
                $this->db->storeLast($sql, $params);
                return 0;
            }
            $start = microtime(true);
            $stmt = $this->dbExec($pdo, $sql, $params, $this->timeoutMs);
            $ms = (microtime(true) - $start) * 1000;
            $res = $stmt->fetchColumn();
            $this->dbEmit($ctx, $ms, 1);
            $result = $res === null ? 0 : $res;
            return is_float($result) && floor($result) == $result ? (int)$result : (float)$result;
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
        $this->select = [$col];
        if ($key) {
            $this->select[] = $key;
        }
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
            while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
                if ($key) {
                    $res[$row[$key]] = $row[$col];
                } else {
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
        if (!$data) throw new \InvalidArgumentException('Insert data cannot be empty.');
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
        if (!$cols) throw new \InvalidArgumentException('Insert rows cannot be empty.');
        foreach ($rows as $index => $row) {
            if (array_keys($row) !== $cols) {
                throw new \InvalidArgumentException("Insert row {$index} must have the same columns as row 0.");
            }
        }
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
                $res = $this->db->table($this->table)->select($returning)->withTrashed()->where('id', '=', $id)->first();
            }
            // Fallback for SQLite if lastInsertId fails
            if (!$res && $driver === 'sqlite') {
                $res = $this->db->table($this->table)->select($returning)->withTrashed()->where($cols[0], '=', $data[$cols[0]])->first();
            }
            $this->dbEmit($ctx, $ms, 1);
            return $res ?: [];
        });
        return $runner($ctx);
    }

    public function update(array $data): int
    {
        $this->assertWritable();
        if (!$data) throw new \InvalidArgumentException('Update data cannot be empty.');
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
        $pdo = $this->db->choosePdo('delete');
        if ($this->softDelete['enabled'] && $this->hasColumn($this->softDelete['column'])) {
            return $this->softDelete();
        }
        [$whereSql, $params] = $this->compileWhere($pdo, true, false);
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
            if ($this->db->getLogger()) {
                call_user_func($this->db->getLogger(), $sql, $params, $ms);
            }
            return $count;
        });
        return $runner($ctx);
    }

    private function softDelete(): int
    {
        $this->assertWritable();
        $col = $this->softDelete['column'];
        if (!$this->softDelete['enabled']) {
            throw new \RuntimeException('Soft delete is not enabled');
        }
        if (!$this->hasColumn($col)) {
            throw new \RuntimeException("Soft delete column '$col' does not exist in table '$this->table'");
        }
        $val = $this->softDelete['mode'] === 'timestamp' ? date('Y-m-d H:i:s') : $this->softDelete['deleted_value'];
        $pdo = $this->db->choosePdo('update');
        [$whereSql, $whereParams] = $this->compileWhere($pdo, true, false);
        $where = $whereSql ? ' WHERE ' . $whereSql : '';
        $sql = 'UPDATE ' . $this->compileTable($pdo) . ' SET ' . $this->db->qi($col, $pdo) . ' = ?' . $where;
        $params = [$val, ...$whereParams];
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
            if ($this->db->getLogger()) {
                call_user_func($this->db->getLogger(), $sql, $params, $ms);
            }
            $this->dbEmit($ctx, $ms, $count);
            return $count;
        });
        return $runner($ctx);
    }

    public function restore(): int
    {
        $this->assertWritable();
        if (!$this->softDelete['enabled'] || !$this->hasColumn($this->softDelete['column'])) {
            throw new \RuntimeException('Soft delete is not enabled or column does not exist');
        }
        $pdo = $this->db->choosePdo('update');
        $this->onlyTrashed();
        [$whereSql, $whereParams] = $this->compileWhere($pdo, true, false);
        $where = $whereSql ? ' WHERE ' . $whereSql : '';
        $sql = 'UPDATE ' . $this->compileTable($pdo) . ' SET ' . $this->db->qi($this->softDelete['column'], $pdo) . ' = ?' . $where;
        $val = $this->softDelete['mode'] === 'timestamp' ? null : 0;
        $params = [$val, ...$whereParams];
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
            if ($this->db->getLogger()) {
                call_user_func($this->db->getLogger(), $sql, $params, $ms);
            }
            return $count;
        });
        return $runner($ctx);
    }

    public function forceDelete(): int
    {
        $this->assertWritable();
        $pdo = $this->db->choosePdo('delete');
        [$whereSql, $params] = $this->compileWhere($pdo, true, false);
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

    public function upsert(array $data, array $conflict, array $updateColumns): int
    {
        $this->assertWritable();
        if (!$data || !$conflict) throw new \InvalidArgumentException('Upsert data and conflict columns are required.');
        $pdo = $this->db->choosePdo('insert');
        $driver = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
        if (!$this->db->hasUniqueConstraint($this->table, $conflict)) {
            throw new \RuntimeException("No UNIQUE constraint on columns: " . implode(',', $conflict));
        }
        $cols = array_keys($data);
        $placeholders = implode(',', array_fill(0, count($cols), '?'));
        $conflictCols = implode(',', array_map(fn($c) => $this->db->qi($c, $pdo), $conflict));
        $updateSets = [];
        $params = array_values($data);
        foreach ($updateColumns as $col) {
            if (!in_array($col, $cols)) {
                throw new \InvalidArgumentException("Update column '$col' not in insert data");
            }
            $updateSets[] = $this->db->qi($col, $pdo) . ' = ' . ($driver === 'mysql'
                ? 'VALUES(' . $this->db->qi($col, $pdo) . ')'
                : 'EXCLUDED.' . $this->db->qi($col, $pdo));
        }
        $updateClause = implode(',', $updateSets);

        if ($driver === 'pgsql') {
            $sql = 'INSERT INTO ' . $this->compileTable($pdo) . ' (' . implode(',', array_map(fn($c) => $this->db->qi($c, $pdo), $cols)) . ') VALUES (' . $placeholders . ') ON CONFLICT (' . $conflictCols . ') ' . ($updateClause ? 'DO UPDATE SET ' . $updateClause : 'DO NOTHING');
        } elseif ($driver === 'mysql') {
            $sql = 'INSERT INTO ' . $this->compileTable($pdo) . ' (' . implode(',', array_map(fn($c) => $this->db->qi($c, $pdo), $cols)) . ') VALUES (' . $placeholders . ')' . ($updateClause ? ' ON DUPLICATE KEY UPDATE ' . $updateClause : ' ON DUPLICATE KEY UPDATE ' . $this->db->qi($conflict[0], $pdo) . ' = ' . $this->db->qi($conflict[0], $pdo));
        } elseif ($driver === 'sqlite') {
            $sql = 'INSERT INTO ' . $this->compileTable($pdo) . ' (' . implode(',', array_map(fn($c) => $this->db->qi($c, $pdo), $cols)) . ') VALUES (' . $placeholders . ') ON CONFLICT (' . $conflictCols . ') ' . ($updateClause ? 'DO UPDATE SET ' . $updateClause : 'DO NOTHING');
        } else {
            throw new \RuntimeException("upsert is not supported by {$driver}.");
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
            if ($this->db->getLogger()) {
                call_user_func($this->db->getLogger(), $sql, $params, $ms);
            }
            return $count;
        });
        return $runner($ctx);
    }

    public function getKeyset(?string $cursor, string $key): array
    {
        $pdo = $this->db->choosePdo('select');
        if (!preg_match('/^[A-Za-z_][A-Za-z0-9_$]*(?:\.[A-Za-z_][A-Za-z0-9_$]*)?$/', $key)) throw new \InvalidArgumentException('Invalid keyset column.');
        $direction = $this->orders ? $this->orders[array_key_last($this->orders)][1] : 'ASC';
        if ($cursor) {
            $decoded = json_decode((string)base64_decode($cursor, true), true);
            if (!is_array($decoded) || !array_key_exists('last', $decoded)) throw new \InvalidArgumentException('Invalid pagination cursor.');
            if (($decoded['direction'] ?? $direction) !== $direction) throw new \InvalidArgumentException('Cursor direction does not match query order.');
            if ($decoded && array_key_exists('last', $decoded)) {
                $last = $decoded['last'];
                $this->where($key, $direction === 'DESC' ? '<' : '>', $last);
            }
        }
        $rows = $this->get();
        $next = null;
        if ($rows && count($rows) === ($this->limit ?? PHP_INT_MAX)) {
            $last = end($rows)[$key] ?? null;
            if ($last !== null) {
                $next = base64_encode(json_encode(['last' => $last, 'direction' => $direction], JSON_THROW_ON_ERROR));
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
            if ($this->db->getLogger()) {
                call_user_func($this->db->getLogger(), $sql, $params, $ms);
            }
        });
        yield from $runner($ctx);
    }

    public function whereJson(string $path, string $op, mixed $val, bool $or = false): self
    {
        $pdo = $this->db->choosePdo('select');
        $driver = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
        if ($driver === 'sqlite') {
            @$pdo->exec('PRAGMA foreign_keys = ON'); // Suppress warning
        }
        $this->wheres[] = [
            'type' => 'json',
            'bool' => $or ? 'OR' : 'AND',
            'path' => $path,
            'op' => $op,
            'val' => $val
        ];
        return $this;
    }

    public function cast(string $expr, string $type = 'TEXT'): string
    {
        if (!preg_match('/^[A-Za-z][A-Za-z0-9_]*(?:\(\d+(?:,\d+)?\))?$/', $type)) throw new \InvalidArgumentException('Invalid cast type.');
        return 'CAST(' . $expr . ' AS ' . strtoupper($type) . ')';
    }

    public function jsonSet(string $col, array $updates): self
    {
        $this->assertWritable();
        if (!$updates) throw new \InvalidArgumentException('JSON updates cannot be empty.');
        $pdo = $this->db->choosePdo('update');
        $driver = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
        if ($driver === 'sqlite') {
            @$pdo->exec('PRAGMA foreign_keys = ON'); // Suppress warning
        }
        $updatesSql = [];
        $params = [];
        foreach ($updates as $path => $val) {
            if (!preg_match('/^[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*$/', (string)$path)) throw new \InvalidArgumentException('Invalid JSON update path.');
            if ($driver === 'mysql') {
                $updatesSql[] = $this->db->qi($col, $pdo) . ' = JSON_SET(' . $this->db->qi($col, $pdo) . ', \'$.' . $path . '\', ?)';
                $params[] = $val;
            } elseif ($driver === 'pgsql') {
                $updatesSql[] = $this->db->qi($col, $pdo) . ' = JSONB_SET(' . $this->db->qi($col, $pdo) . ', \'{' . str_replace('.', ',', $path) . '}\', ?)';
                $params[] = json_encode($val);
            } elseif ($driver === 'sqlite') {
                $updatesSql[] = $this->db->qi($col, $pdo) . ' = json_set(' . $this->db->qi($col, $pdo) . ', \'$.' . $path . '\', ?)';
                $params[] = $val;
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
            if ($this->db->getLogger()) {
                call_user_func($this->db->getLogger(), $sql, $params, $ms);
            }
            return $this;
        });
        return $runner($ctx);
    }
}
