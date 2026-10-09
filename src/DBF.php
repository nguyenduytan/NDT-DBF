<?php
/**
 * NDT DBF - Single-file PHP SQL Framework
 *
 * @version   0.3.0
 * @package   NDT DBF
 * @description Single-file PDO query builder, transactions and JSON operations.
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
    public const VERSION = '0.3.0';
    private PDO $pdoWrite;
    private ?PDO $pdoRead = null;
    private string $driverWrite;
    private ?string $driverRead = null;
    private string $prefix = '';
    private bool $readonly = false;
    private bool $testMode = false;

    private $logger = null;

    private $metrics = null;

    private array $middlewares = [];

    private array $schemaCache = [];

    private array $scope = [];

    private $policy = null;

    private string $routing = 'single';

    private string $currentRoute = 'write';

    private int $maxInParams = 1000;

    private object $transactionState;

    private array $softDelete = [
        'enabled' => false,
        'column'  => 'deleted_at',
        'mode'    => 'timestamp',
        'deleted_value' => 1,
    ];

    private string $lastQueryString = '';
    private array $lastQueryParams = [];

    public function __construct(string|array|PDO|null $configOrUri = null)
    {
        $this->transactionState = (object)['depth' => 0, 'savepointCounter' => 0];
        if ($configOrUri === null) {
            $env = getenv('NDTAN_DBF_URL') ?: throw new \InvalidArgumentException('No configuration provided. Pass URI/array/PDO or set NDTAN_DBF_URL.');
            $configOrUri = $env;
        }

        if (is_string($configOrUri) || $configOrUri instanceof PDO) {
            [$pdo, $driver] = $this->connectFromArray($configOrUri);
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
            if (isset($configOrUri['readonly'])) $this->readonly = $this->readonly || (bool)$configOrUri['readonly'];
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
        if ($this->maxInParams < 1) throw new \InvalidArgumentException('max_in_params must be positive.');
        if ($this->prefix !== '' && !preg_match('/^[A-Za-z_][A-Za-z0-9_]*$/', $this->prefix)) throw new \InvalidArgumentException('Invalid table prefix.');
        if (!in_array($this->softDelete['mode'], ['timestamp', 'flag'], true)) throw new \InvalidArgumentException('Soft delete mode must be timestamp or flag.');
        $this->qi($this->softDelete['column'], $this->pdoWrite);
        foreach ([$this->logger, $this->metrics] as $hook) {
            if ($hook !== null && !is_callable($hook)) throw new \InvalidArgumentException('Logger and metrics must be callable.');
        }
        $this->pdoWrite->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        if ($this->pdoRead) $this->pdoRead->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    }

    private function connectFromUri(string $uri): array
    {
        if (preg_match('/^(mysql|pgsql|sqlsrv|oci):[^\/]/', $uri)) return $this->connectFromArray(['dsn' => $uri]);
        if (str_starts_with($uri, 'sqlite:') && !str_starts_with($uri, 'sqlite://')) {
            return $this->connectFromArray(['type' => 'sqlite', 'database' => substr($uri, 7)]);
        }
        if (str_starts_with($uri, 'sqlite:///')) {
            $path = rawurldecode(substr($uri, 10));
            return $this->connectFromArray(['type' => 'sqlite', 'database' => $path === ':memory:' ? $path : '/' . $path]);
        }
        $parsed = parse_url($uri);
        if ($parsed === false || empty($parsed['scheme'])) throw new \InvalidArgumentException('Invalid database URI.');
        $driver = $parsed['scheme'] ?? 'mysql';
        $user = isset($parsed['user']) ? rawurldecode($parsed['user']) : '';
        $pass = isset($parsed['pass']) ? rawurldecode($parsed['pass']) : '';
        $host = $parsed['host'] ?? 'localhost';
        $port = $parsed['port'] ?? match ($driver) { 'pgsql' => 5432, 'sqlsrv' => 1433, 'oracle', 'oci' => 1521, default => 3306 };
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
                    @$pdo->exec('PRAGMA foreign_keys = ON');
                    @$pdo->exec('PRAGMA journal_mode = WAL');
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
            $config->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);
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
            $config['pdo']->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);
            return [$config['pdo'], (string)$config['pdo']->getAttribute(PDO::ATTR_DRIVER_NAME)];
        }
        if (isset($config['dsn'])) {
            $pdo = new PDO($config['dsn'], $config['username'] ?? null, $config['password'] ?? null, $attrs);
            return [$pdo, (string)$pdo->getAttribute(PDO::ATTR_DRIVER_NAME)];
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
                @$pdo->exec('PRAGMA foreign_keys = ON');
                @$pdo->exec('PRAGMA journal_mode = WAL');
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
                $this->readonly = true;
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
        if (!$columns || count(array_unique($columns)) !== count($columns)) throw new \InvalidArgumentException('Unique columns must be nonempty and distinct.');
        $pdo = $this->pdoWrite;
        $table = $this->prefix . $table;
        $quoted = $this->qi($table, $pdo);
        foreach ($columns as $column) $this->qi($column, $pdo);
        $driver = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
        $indexes = [];
        if ($driver === 'sqlite') {
            $primary = [];
            foreach ($pdo->query('PRAGMA table_info(' . $quoted . ')')->fetchAll(PDO::FETCH_ASSOC) as $row) {
                if ($row['pk']) $primary[(int)$row['pk']] = $row['name'];
            }
            if ($primary) { ksort($primary); $indexes[] = array_values($primary); }
            foreach ($pdo->query('PRAGMA index_list(' . $quoted . ')')->fetchAll(PDO::FETCH_ASSOC) as $index) {
                if (!$index['unique'] || $index['partial']) continue;
                $name = '"' . str_replace('"', '""', $index['name']) . '"';
                $indexes[] = array_column($pdo->query('PRAGMA index_info(' . $name . ')')->fetchAll(PDO::FETCH_ASSOC), 'name');
            }
        } elseif ($driver === 'mysql') {
            foreach ($pdo->query('SHOW INDEX FROM ' . $quoted)->fetchAll(PDO::FETCH_ASSOC) as $row) {
                if (!$row['Non_unique'] && !$row['Sub_part']) $indexes[$row['Key_name']][(int)$row['Seq_in_index']] = $row['Column_name'];
            }
        } elseif ($driver === 'pgsql') {
            $stmt = $pdo->prepare("SELECT i.indexrelid, a.attname, k.ordinality FROM pg_index i CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum, ordinality) JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum WHERE i.indrelid = CAST(? AS regclass) AND i.indisunique AND i.indpred IS NULL AND i.indexprs IS NULL AND k.ordinality <= i.indnkeyatts ORDER BY k.ordinality");
            $stmt->execute([$quoted]);
            foreach ($stmt->fetchAll(PDO::FETCH_ASSOC) as $row) $indexes[$row['indexrelid']][] = $row['attname'];
        } else {
            throw new \RuntimeException('Unique metadata is not supported by ' . $driver);
        }
        sort($columns);
        foreach ($indexes as $index) {
            $index = array_values($index);
            sort($index);
            if ($index === $columns) return true;
        }
        return false;
    }

    public function execPreparedOn(PDO $pdo, string $sql, array $params, int $timeoutMs = 0): PDOStatement
    {
        if ($this->transactionState->depth > 0 && !$this->pdoWrite->inTransaction()) throw new \LogicException('Transaction ownership was lost; further queries are blocked.');
        $restore = null;
        if ($timeoutMs > 0) {
            $driver = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
            if ($driver === 'mysql') {
                $maria = stripos((string)$pdo->getAttribute(PDO::ATTR_SERVER_VERSION), 'MariaDB') !== false;
                $setting = $maria ? 'max_statement_time' : 'max_execution_time';
                $previous = $pdo->query('SELECT @@SESSION.' . $setting)->fetchColumn();
                $pdo->exec('SET SESSION ' . $setting . ' = ' . ($maria ? $timeoutMs / 1000 : $timeoutMs));
                $restore = fn() => $pdo->exec('SET SESSION ' . $setting . ' = ' . $previous);
            } elseif ($driver === 'pgsql') {
                $previous = $pdo->query('SHOW statement_timeout')->fetchColumn();
                $set = $pdo->prepare("SELECT set_config('statement_timeout', ?, ?)");
                $local = $pdo->inTransaction() ? 'true' : 'false';
                $set->execute([$timeoutMs . 'ms', $local]);
                $restore = fn() => $set->execute([$previous, $local]);
            } elseif ($driver === 'sqlite') {
                $previous = (int)$pdo->query('PRAGMA busy_timeout')->fetchColumn();
                $pdo->exec('PRAGMA busy_timeout = ' . $timeoutMs);
                $restore = fn() => $pdo->exec('PRAGMA busy_timeout = ' . $previous);
            } else {
                throw new \RuntimeException('timeout is not supported by ' . $driver);
            }
        }
        try {
        $this->lastQueryString = $sql;
        $this->lastQueryParams = $params;
        $stmt = $pdo->prepare($sql);
        foreach ($params as $key => $value) {
            $type = match (true) {
                $value === null => PDO::PARAM_NULL,
                is_bool($value) => PDO::PARAM_BOOL,
                is_int($value) => PDO::PARAM_INT,
                is_resource($value) => PDO::PARAM_LOB,
                default => PDO::PARAM_STR,
            };
            if (is_array($value) || is_object($value)) throw new \InvalidArgumentException('SQL values must be scalar, null or stream resources.');
            $stmt->bindValue(is_int($key) ? $key + 1 : ':' . ltrim($key, ':'), $value, $type);
        }
        $stmt->execute();
        return $stmt;
        } catch (Throwable $error) {
            throw $error;
        } finally {
            if ($restore && !(isset($error) && $pdo->getAttribute(PDO::ATTR_DRIVER_NAME) === 'pgsql' && $pdo->inTransaction())) $restore();
        }
    }

    public function emitMetrics(array $ctx, float $ms, int $count): void
    {
        if ($this->metrics) call_user_func($this->metrics, array_merge($ctx, ['ms' => $ms, 'count' => $count]));
    }

    public function choosePdo(string $type): PDO
    {
        if ($this->transactionState->depth > 0 && !$this->pdoWrite->inTransaction()) throw new \LogicException('Transaction ownership was lost; further queries are blocked.');
        if ($this->pdoWrite->inTransaction()) return $this->pdoWrite;
        if (!in_array($type, ['select', 'aggregate', 'raw_read'], true)) return $this->pdoWrite;
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
        $parts = explode('.', $fullTable, 2);
        $schema = count($parts) === 2 ? $parts[0] : null;
        $name = $parts[count($parts) - 1];
        $cols = [];
        try {
            switch ($driver) {
                case 'sqlite':
                    $stmt = $pdo->query('PRAGMA ' . ($schema ? $this->qi($schema, $pdo) . '.' : '') . "table_info('$name')");
                    $cols = $stmt ? array_column($stmt->fetchAll(), 'name') : [];
                    break;
                case 'mysql':
                    $stmt = $pdo->query('SHOW COLUMNS FROM ' . $this->qi($fullTable, $pdo));
                    $cols = $stmt ? array_column($stmt->fetchAll(), 'Field') : [];
                    break;
                case 'pgsql':
                    $stmt = $pdo->prepare("SELECT column_name FROM information_schema.columns WHERE table_name = ? AND table_schema = COALESCE(?, current_schema())");
                    if ($stmt) {
                        $stmt->execute([$name, $schema]);
                        $cols = array_column($stmt->fetchAll(), 'column_name');
                    }
                    break;
                case 'sqlsrv':
                    $stmt = $pdo->prepare("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND TABLE_SCHEMA = COALESCE(?, SCHEMA_NAME())");
                    if ($stmt) {
                        $stmt->execute([$name, $schema]);
                        $cols = array_column($stmt->fetchAll(), 'COLUMN_NAME');
                    }
                    break;
                case 'oci':
                    $stmt = $pdo->prepare("SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = UPPER(?) AND OWNER = COALESCE(UPPER(?), SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA'))");
                    if ($stmt) {
                        $stmt->execute([$name, $schema]);
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
        if ($this->readonly) throw new \RuntimeException('Readonly mode: transactions are blocked.');
        if ($this->testMode) throw new \LogicException('Transactions require execution mode.');
        if (!in_array($this->driverWrite, ['mysql', 'pgsql', 'sqlite'], true)) throw new \RuntimeException('Transactions are supported on MySQL, PostgreSQL and SQLite.');
        if ($this->transactionState->depth === 0 && $this->pdoWrite->inTransaction()) throw new \LogicException('Transaction is already owned by another DBF instance or external PDO caller.');
        if ($this->transactionState->depth > 0) {
            if (!$this->pdoWrite->inTransaction()) throw new \LogicException('Transaction ownership was lost.');
            $savepoint = 'ndtan_sp_' . (++$this->transactionState->savepointCounter);
            $this->pdoWrite->exec('SAVEPOINT ' . $savepoint);
            ++$this->transactionState->depth;
            try {
                $result = $fn($this);
                $this->pdoWrite->exec('RELEASE SAVEPOINT ' . $savepoint);
                --$this->transactionState->depth;
                return $result;
            } catch (Throwable $e) {
                try {
                    if ($this->pdoWrite->inTransaction()) {
                        $this->pdoWrite->exec('ROLLBACK TO SAVEPOINT ' . $savepoint);
                        $this->pdoWrite->exec('RELEASE SAVEPOINT ' . $savepoint);
                    }
                } catch (Throwable $cleanup) {
                    if ($this->pdoWrite->inTransaction()) $this->pdoWrite->rollBack();
                } finally {
                    --$this->transactionState->depth;
                }
                throw $e;
            }
        }
        for ($i = 1; $i <= $attempts; $i++) {
            try {
                $this->pdoWrite->beginTransaction();
                $this->transactionState->depth = 1;
                $res = $fn($this);
                if (!$this->pdoWrite->inTransaction()) throw new \LogicException('Transaction ownership was lost.');
                $this->pdoWrite->commit();
                $this->transactionState->depth = 0;
                return $res;
            } catch (Throwable $e) {
                try {
                    if ($this->pdoWrite->inTransaction()) $this->pdoWrite->rollBack();
                } catch (Throwable $cleanup) {
                }
                $this->transactionState->depth = 0;
                $retryable = $e instanceof \PDOException && (
                    in_array((string)$e->getCode(), ['40001', '40P01'], true) ||
                    in_array((int)($e->errorInfo[1] ?? 0), [5, 6, 1205, 1213], true)
                );
                if ($i === $attempts || !$retryable) throw $e;
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
        if ($route === 'read' && !$this->pdoRead) throw new \RuntimeException('Read connection is not configured.');
        $this->currentRoute = $route;
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
        if (trim($sql) === '') throw new \InvalidArgumentException('SQL cannot be empty.');
        if ($this->readonly) throw new \RuntimeException('Readonly mode: use selectRaw() with a database read-only account.');
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
            if ($this->getLogger()) call_user_func($this->getLogger(), $sql, $params, $ms);
            if ($stmt->columnCount() > 0) {
                $res = $stmt->fetchAll(PDO::FETCH_ASSOC);
                $stmt->closeCursor();
                $this->emitMetrics($ctx, $ms, count($res));
                return $res;
            }
            $count = $stmt->rowCount();
            $stmt->closeCursor();
            $this->schemaCache = [];
            $this->emitMetrics($ctx, $ms, $count);
            return $count;
        });
        return $runner($ctx);
    }

    public function selectRaw(string $sql, array $params = []): array
    {
        if (!preg_match('/^\s*SELECT\b/i', $sql) || str_contains($sql, ';')) {
            throw new \InvalidArgumentException('selectRaw accepts a single SELECT without a semicolon. Use raw for CTEs and other statements.');
        }
        $pdo = $this->choosePdo('raw_read');
        $ctx = ['type' => 'select', 'sql' => $sql];
        return ($this->dbBuildRunner(function ($ctx) use ($pdo, $sql, $params): array {
            if ($this->testMode) { $this->storeLast($sql, $params); return []; }
            $start = microtime(true);
            $stmt = $this->execPreparedOn($pdo, $sql, $params);
            try { $rows = $stmt->fetchAll(PDO::FETCH_ASSOC); }
            finally { $stmt->closeCursor(); }
            $ms = (microtime(true) - $start) * 1000;
            if ($this->logger) ($this->logger)($sql, $params, $ms);
            $this->emitMetrics($ctx, $ms, count($rows));
            return $rows;
        }))($ctx);
    }

    public function execute(string $sql, array $params = []): int
    {
        $result = $this->raw($sql, $params);
        if ($this->testMode) return 0;
        if (is_array($result)) throw new \LogicException('execute cannot return rows; use raw for RETURNING or SELECT.');
        return $result;
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
    private ?array $keysetBoundary = null;
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
        if (!$cols) throw new \InvalidArgumentException('Select columns cannot be empty.');
        $this->select = $cols;
        return $this;
    }

    public function where(string $col, string $op, mixed $val, bool $or = false): self
    {
        $op = $this->normalizeOperator($op);
        if ($val === null && in_array($op, ['=', '!=', '<>'], true)) {
            return $this->whereNull($col, $op !== '=', $or);
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
        if ($ms < 0) throw new \InvalidArgumentException('Timeout must be non-negative.');
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
            $join .= ' ' . $j['type'] . ' JOIN ' . $this->db->qi($this->db->getPrefix() . $j['table'], $pdo) . ($j['type'] === 'CROSS' ? '' : ' ON ' . $this->db->qi($j['left'], $pdo) . ' ' . $j['op'] . ' ' . $this->db->qi($j['right'], $pdo));
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
        if ($driver === 'sqlsrv') {
            if (($this->limit !== null || $this->offset !== null) && !$order) throw new \LogicException('SQL Server pagination requires orderBy.');
            $offset = '';
            $limit = $this->limit !== null || $this->offset !== null ? ' OFFSET ' . ($this->offset ?? 0) . ' ROWS' : '';
            if ($this->limit !== null) {
                if ($this->limit === 0) throw new \InvalidArgumentException('SQL Server limit must be positive.');
                $limit .= ' FETCH NEXT ' . $this->limit . ' ROWS ONLY';
            }
        } elseif ($driver === 'oci') {
            $limit = ($this->offset !== null ? ' OFFSET ' . $this->offset . ' ROWS' : '') .
                ($this->limit !== null ? ' FETCH NEXT ' . $this->limit . ' ROWS ONLY' : '');
            $offset = '';
        } elseif ($this->offset !== null && $this->limit === null) {
            if ($driver === 'sqlite') $limit = ' LIMIT -1';
            if ($driver === 'mysql') $limit = ' LIMIT 18446744073709551615';
        }
        $locking = '';
        if ($this->forUpdate) {
            if (in_array($driver, ['mysql', 'pgsql'], true)) {
                if (!$pdo->inTransaction()) throw new \LogicException('forUpdate requires an active transaction.');
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

        if ($this->onlyTrashed && $this->softDelete['enabled'] && $this->hasColumn($sdCol, $pdo)) {
            $sdColQuoted = $this->db->qi($this->joins ? $this->db->getPrefix() . $this->table . '.' . $sdCol : $sdCol, $pdo);
            $non_user_conditions[] = $sdColQuoted . ($this->softDelete['mode'] === 'timestamp' ? ' IS NOT NULL' : ' = ?');
            if ($this->softDelete['mode'] !== 'timestamp') $bind[] = $this->softDelete['deleted_value'];
        }

        if ($includeScope && $this->scope) {
            foreach ($this->scope as $k => $v) {
                $non_user_conditions[] = $this->db->qi($k, $pdo) . ($v === null ? ' IS NULL' : ' = ?');
                if ($v !== null) $bind[] = $v;
            }
        }

        if ($forSelect && $this->softDelete['enabled'] && $this->hasColumn($sdCol, $pdo) && !$this->withTrashed && !$this->onlyTrashed) {
            $sdColQuoted = $this->db->qi($this->joins ? $this->db->getPrefix() . $this->table . '.' . $sdCol : $sdCol, $pdo);
            $non_user_conditions[] = $sdColQuoted . ($this->softDelete['mode'] === 'timestamp' ? ' IS NULL' : ' = 0');
        }

        $baseWhere = $non_user_conditions ? implode(' AND ', $non_user_conditions) : '';
        if ($this->keysetBoundary !== null) {
            [$key, $operator, $value] = $this->keysetBoundary;
            $boundary = $this->db->qi($key, $pdo) . ' ' . $operator . ' ?';
            $baseWhere = $baseWhere ? $baseWhere . ' AND ' . $boundary : $boundary;
            $bind[] = $value;
        }

        $userParts = [];

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
                    $pair = array_values($w['pair']);
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
                        $expr = $this->db->qi($col, $pdo) . " #>> '{" . implode(',', $jsonPath) . "}'";
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
        $pdo ??= $this->db->choosePdo('schema');
        $cols = $this->db->getColumns($this->table, $pdo);
        return in_array($column, $cols, true);
    }

    public function get(): array
    {
        $pdo = $this->db->choosePdo($this->forUpdate ? 'lock' : 'select');
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
            $stmt->closeCursor();
            $this->dbEmit($ctx, $ms, count($res));
            return $res;
        });
        return $runner($ctx);
    }

    public function first(): ?array
    {
        $rows = (clone $this)->limit(1)->get();
        return $rows[0] ?? null;
    }

    public function exists(): bool
    {
        $pdo = $this->db->choosePdo('select');
        [$sql, $params] = $this->compileSelect($pdo);
        $sql = 'SELECT CASE WHEN EXISTS (' . $sql . ') THEN 1 ELSE 0 END AS ndtan_exists';
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
        return (int)$this->aggregate('COUNT', '*');
    }

    public function sum(string $col): mixed
    {
        return $this->aggregate('SUM', $col) ?? 0;
    }

    public function avg(string $col): mixed
    {
        return $this->aggregate('AVG', $col);
    }

    public function min(string $col): mixed
    {
        return $this->aggregate('MIN', $col);
    }

    public function max(string $col): mixed
    {
        return $this->aggregate('MAX', $col);
    }

    private function aggregate(string $function, string $column): mixed
    {
        $pdo = $this->db->choosePdo('aggregate');
        $query = clone $this;
        $query->orders = [];
        $query->limit = $query->offset = null;
        $query->forUpdate = $query->skipLocked = false;
        if ($function === 'COUNT' && $query->groups) {
            $query->select = $query->groups;
            [$inner, $params] = $query->compileSelect($pdo);
            $sql = 'SELECT COUNT(*) FROM (' . $inner . ') ndtan_count';
        } else {
            $query->select = [$function . '(' . $column . ')'];
            [$sql, $params] = $query->compileSelect($pdo);
        }
        $ctx = ['type' => 'aggregate', 'table' => $this->table];
        return ($this->dbRunner(function ($ctx) use ($pdo, $sql, $params) {
            if ($this->db->isTestMode()) { $this->db->storeLast($sql, $params); return null; }
            $start = microtime(true);
            $stmt = $this->dbExec($pdo, $sql, $params, $this->timeoutMs);
            try {
                $values = $stmt->fetchAll(PDO::FETCH_COLUMN);
                if (count($values) > 1) throw new \LogicException('Grouped aggregate returns multiple values; use select with an aggregate alias.');
                $result = $values[0] ?? null;
            } finally { $stmt->closeCursor(); }
            $ms = (microtime(true) - $start) * 1000;
            if ($this->db->getLogger()) ($this->db->getLogger())($sql, $params, $ms);
            $this->dbEmit($ctx, $ms, 1);
            return $result;
        }))($ctx);
    }

    public function pluck(string $col, ?string $key = null): array
    {
        $query = clone $this;
        $query->select = $key ? [$col, $key] : [$col];
        $rows = $query->get();
        $valueName = substr($col, (int)strrpos('.' . $col, '.'));
        $keyName = $key === null ? null : substr($key, (int)strrpos('.' . $key, '.'));
        return array_column($rows, $valueName, $keyName);
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
        if (!$rows) return [];
        $rows = array_values($rows);
        if (!is_array($rows[0]) || !$rows[0]) throw new \InvalidArgumentException('Insert rows cannot be empty.');
        $columns = array_keys($rows[0]);
        foreach ($rows as &$row) {
            if (!is_array($row) || count($row) !== count($columns) || array_diff($columns, array_keys($row))) {
                throw new \InvalidArgumentException('All insert rows must have the same columns.');
            }
            $row = array_replace(array_fill_keys($columns, null), $row);
        }
        unset($row);
        if ($this->db->isTestMode()) {
            $pdo = $this->db->choosePdo('insert');
            $values = '(' . implode(',', array_fill(0, count($columns), '?')) . ')';
            $sql = 'INSERT INTO ' . $this->compileTable($pdo) . ' (' . implode(',', array_map(fn($c) => $this->db->qi($c, $pdo), $columns)) . ') VALUES ' . implode(',', array_fill(0, count($rows), $values));
            $ctx = ['type' => 'insert', 'table' => $this->table];
            return ($this->dbRunner(function () use ($sql, $rows) {
                $params = [];
                foreach ($rows as $row) array_push($params, ...array_values($row));
                $this->db->storeLast($sql, $params);
                return [];
            }))($ctx);
        }
        return $this->db->tx(function () use ($rows) {
            $ids = [];
            foreach ($rows as $row) $ids[] = $this->insert($row);
            return $ids;
        }, 1);
    }

    public function insertGet(array $data, array $returning): array
    {
        $this->assertWritable();
        if (!$data || !$returning) throw new \InvalidArgumentException('Insert data and returning columns are required.');
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
        $runner = $this->dbRunner(function($ctx) use ($pdo, $sql, $params, $driver, $returning, $data) {
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
                $identity = $data['id'] ?? $id;
                $lookup = 'SELECT ' . implode(',', array_map(fn($c) => $this->db->qi($c, $pdo), $returning)) . ' FROM ' . $this->compileTable($pdo) . ' WHERE ' . $this->db->qi('id', $pdo) . ' = ?';
                $lookupStmt = $this->dbExec($pdo, $lookup, [$identity]);
                $res = $lookupStmt->fetch(PDO::FETCH_ASSOC);
                $lookupStmt->closeCursor();
            }
            $stmt->closeCursor();
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
        if (!preg_match('/^[A-Za-z_][A-Za-z0-9_$]*$/', $key)) throw new \InvalidArgumentException('Keyset requires an unqualified unique column.');
        if ($this->limit === null || $this->limit < 1 || $this->limit === PHP_INT_MAX) throw new \InvalidArgumentException('Keyset requires a positive limit.');
        if ($this->offset || $this->groups || $this->havings) throw new \LogicException('Keyset does not support offset, grouping or having.');
        if (count($this->orders) > 1 || ($this->orders && $this->orders[0][0] !== $key)) throw new \LogicException('Keyset must be ordered only by its unique key.');
        $query = clone $this;
        if (!$query->orders) $query->orderBy($key);
        $direction = $query->orders[0][1];
        if ($cursor !== null) {
            $raw = base64_decode($cursor, true);
            $decoded = $raw === false ? null : json_decode($raw, true);
            if (!is_array($decoded) || !isset($decoded['last']) || !is_scalar($decoded['last']) || ($decoded['direction'] ?? null) !== $direction || ($decoded['key'] ?? null) !== $key) {
                throw new \InvalidArgumentException('Invalid or mismatched pagination cursor.');
            }
            $query->keysetBoundary = [$key, $direction === 'DESC' ? '<' : '>', $decoded['last']];
        }
        $rows = $query->limit($this->limit + 1)->get();
        $hasMore = count($rows) > $this->limit;
        if ($hasMore) array_pop($rows);
        $next = null;
        if ($rows && !array_key_exists($key, $rows[0])) throw new \LogicException('Select the keyset column in the result.');
        if ($hasMore) {
            $last = $rows[array_key_last($rows)][$key];
            $next = base64_encode(json_encode(['last' => $last, 'direction' => $direction, 'key' => $key], JSON_THROW_ON_ERROR));
        }
        return ['data' => $rows, 'next' => $next];
    }

    public function chunkById(int $size, callable $callback, string $key = 'id'): void
    {
        $query = clone $this;
        $query->orders = [];
        $query->offset = null;
        $query->orderBy($key)->limit($size);
        $cursor = null;
        do {
            $page = $query->getKeyset($cursor, $key);
            if (!$page['data'] || $callback($page['data']) === false) break;
            $cursor = $page['next'];
        } while ($cursor !== null);
    }

    public function chunk(int $size, callable $callback): void
    {
        if ($size <= 0) throw new \InvalidArgumentException('Chunk size must be positive');
        $offset = 0;
        while (true) {
            $chunk = (clone $this)->offset($offset)->limit($size)->get();
            if (empty($chunk)) break;
            if ($callback($chunk) === false) break;
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
                return;
            }
            $start = microtime(true);
            $stmt = $this->dbExec($pdo, $sql, $params, $this->timeoutMs);
            $ms = (microtime(true) - $start) * 1000;
            $count = 0;
            try {
                while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
                    $count++;
                    yield $row;
                }
            } finally { $stmt->closeCursor(); }
            $this->dbEmit($ctx, $ms, $count);
            if ($this->db->getLogger()) {
                call_user_func($this->db->getLogger(), $sql, $params, $ms);
            }
        });
        yield from $runner($ctx);
    }

    public function whereJson(string $path, string $op, mixed $val, bool $or = false): self
    {
        $op = $this->normalizeOperator($op);
        $pdo = $this->db->choosePdo('select');
        $driver = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
        if ($driver === 'sqlite') {
            @$pdo->exec('PRAGMA foreign_keys = ON');
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
            @$pdo->exec('PRAGMA foreign_keys = ON');
        }
        $expression = $this->db->qi($col, $pdo);
        $params = [];
        foreach ($updates as $path => $val) {
            if (!preg_match('/^[A-Za-z0-9_]+(?:\\.[A-Za-z0-9_]+)*$/', (string)$path)) throw new \InvalidArgumentException('Invalid JSON update path.');
            $json = json_encode($val, JSON_THROW_ON_ERROR);
            if ($driver === 'mysql') {
                $expression = 'JSON_SET(' . $expression . ', ?, JSON_EXTRACT(?, \'$\'))';
                array_push($params, '$.' . $path, $json);
            } elseif ($driver === 'pgsql') {
                $expression = 'jsonb_set(' . $expression . '::jsonb, ?::text[], ?::jsonb, true)';
                array_push($params, '{' . str_replace('.', ',', $path) . '}', $json);
            } elseif ($driver === 'sqlite') {
                $expression = 'json_set(' . $expression . ', ?, json(?))';
                array_push($params, '$.' . $path, $json);
            } else {
                throw new \RuntimeException('jsonSet not supported on ' . $driver);
            }
        }
        $sql = 'UPDATE ' . $this->compileTable($pdo) . ' SET ' . $this->db->qi($col, $pdo) . ' = ' . $expression;
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
