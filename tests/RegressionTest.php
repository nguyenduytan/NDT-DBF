<?php

namespace ndtan;

use PDO;
use PHPUnit\Framework\TestCase;

final class RegressionTest extends TestCase
{
    public function testQualifiedTableSoftDeleteAndJoinedTrashed(): void
    {
        $db = new DBF(['write' => new PDO('sqlite::memory:'), 'features' => ['soft_delete' => ['enabled' => true, 'mode' => 'timestamp']]]);
        $db->raw('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, deleted_at TEXT)');
        $db->table('items')->insert(['name' => 'a']);
        $this->assertSame(1, $db->table('main.items')->delete());
        $this->assertSame(1, $db->table('items')->withTrashed()->count());
        $this->assertSame(0, $db->table('main.items')->count());
        $db->raw('CREATE TABLE other (item_id INTEGER, deleted_at TEXT)');
        $db->raw('INSERT INTO other(item_id) VALUES (1)');
        $this->assertCount(1, $db->table('items')->join('other', 'items.id', '=', 'other.item_id')->onlyTrashed()->get());
    }

    public function testLostTransactionBlocksFurtherWritesAndPreservesFailure(): void
    {
        $db = $this->database();
        $scoped = $db->withScope(['tenant' => 1]);
        try {
            $db->tx(function (DBF $tx) use ($scoped): void {
                $tx->table('items')->insert(['name' => 'a']);
                try {
                    $tx->tx(fn(DBF $nested) => $nested->execute("INSERT OR ROLLBACK INTO items(name) VALUES ('a')"));
                } catch (\PDOException $e) {
                    $this->assertStringContainsString('UNIQUE', $e->getMessage());
                }
                $scoped->table('items')->insert(['name' => 'outside']);
            }, 1);
            $this->fail('Lost transaction must fail.');
        } catch (\LogicException $e) {
            $this->assertStringContainsString('Transaction', $e->getMessage());
        }
        $this->assertSame(0, $db->table('items')->count());
    }

    public function testKeysetOrPredicatesCannotBypassBoundary(): void
    {
        $db = $this->database();
        $db->table('items')->insertMany([['name' => 'a'], ['name' => 'b']]);
        $query = $db->table('items')->where('name', '=', 'a')->orWhere('name', '=', 'b')->orderBy('id')->limit(1);
        $first = $query->getKeyset(null, 'id');
        $second = $query->getKeyset($first['next'], 'id');
        $this->assertSame(['b'], array_column($second['data'], 'name'));
        $this->assertNull($second['next']);
    }

    private function database(): DBF
    {
        $db = new DBF('sqlite::memory:');
        $db->raw('CREATE TABLE items (id INTEGER PRIMARY KEY, tenant INTEGER, name TEXT UNIQUE, data TEXT, deleted_at TEXT)');
        return $db;
    }

    public function testDsnAndPdoInjection(): void
    {
        $db = new DBF(new PDO('sqlite::memory:'));
        $this->assertSame('sqlite', $db->info()['driver']);
        $this->assertSame([['n' => 1]], $db->raw('SELECT 1 AS n'));
        $this->assertSame('sqlite', (new DBF('sqlite:///:memory:'))->info()['driver']);
    }

    public function testScopeCannotBeBypassedByOr(): void
    {
        $db = $this->database();
        $db->table('items')->insertMany([
            ['tenant' => 1, 'name' => 'one'], ['tenant' => 2, 'name' => 'two'],
        ]);
        $rows = $db->withScope(['tenant' => 1])->table('items')->where('name', '=', 'one')->orWhere('name', '=', 'two')->get();
        $this->assertSame(['one'], array_column($rows, 'name'));
        $this->assertSame(2, $db->table('items')->where('deleted_at', '<>', null)->count() + $db->table('items')->where('deleted_at', '=', null)->count());
    }

    public function testJsonMultiplePathsAndNestedRead(): void
    {
        $db = $this->database();
        $db->table('items')->insert(['name' => 'json', 'data' => '{"profile":{"name":"Old"},"score":1}']);
        $db->table('items')->where('name', '=', 'json')->jsonSet('data', ['profile.name' => 'Tony', 'score' => 9]);
        $row = $db->table('items')->whereJson('data->profile->name', '=', 'Tony')->first();
        $this->assertSame(9, json_decode($row['data'], true)['score']);
        $this->expectException(\InvalidArgumentException::class);
        $db->table('items')->whereJson('data->score', '= 1 OR 1=1 --', 1)->get();
    }

    public function testRawCteAndReturning(): void
    {
        $db = $this->database();
        $this->assertSame(1, $db->raw("WITH value AS (SELECT 'cte' AS name) INSERT INTO items(name) SELECT name FROM value"));
        $this->assertSame([['name' => 'updated']], $db->raw("UPDATE items SET name='updated' RETURNING name"));
        $db->setReadonly(true);
        $this->expectException(\RuntimeException::class);
        $db->raw("WITH value AS (SELECT 'blocked' AS name) INSERT INTO items(name) SELECT name FROM value");
    }

    public function testNestedRollbackKeepsOuterTransaction(): void
    {
        $db = $this->database();
        $db->tx(function (DBF $tx): void {
            $tx->table('items')->insert(['name' => 'outer']);
            try {
                $tx->tx(function (DBF $nested): void {
                    $nested->table('items')->insert(['name' => 'inner']);
                    throw new \RuntimeException('rollback inner');
                });
            } catch (\RuntimeException $e) {
                $this->assertSame('rollback inner', $e->getMessage());
            }
            $this->assertSame(1, $tx->table('items')->count());
        });
        $this->assertSame(['outer'], $db->table('items')->pluck('name'));
    }

    public function testReplicaRoutingAndTransactionPinning(): void
    {
        $write = new PDO('sqlite::memory:');
        $read = new PDO('sqlite::memory:');
        $db = new DBF(['write' => $write, 'read' => $read, 'routing' => 'manual']);
        $db->using('read');
        $this->assertSame($read, $db->choosePdo('select'));
        $this->assertSame($write, $db->choosePdo('update'));
        $db->tx(function (DBF $tx) use ($write): void {
            $this->assertSame($write, $tx->choosePdo('select'));
        });
        $this->assertSame($read, $db->choosePdo('select'));
    }

    public function testKeysetDescendingDoesNotMutateBuilder(): void
    {
        $db = $this->database();
        $db->table('items')->insertMany([['name' => 'a'], ['name' => 'b'], ['name' => 'c']]);
        $query = $db->table('items')->orderBy('id', 'desc')->limit(2);
        $first = $query->getKeyset(null, 'id');
        $second = $query->getKeyset($first['next'], 'id');
        $this->assertSame([3, 2], array_column($first['data'], 'id'));
        $this->assertSame([1], array_column($second['data'], 'id'));
        $this->assertNull($second['next']);
        $this->assertSame([3, 2], array_column($query->get(), 'id'));
    }

    public function testBulkInsertReturnsActualIdsAndNormalizesKeys(): void
    {
        $db = $this->database();
        $ids = $db->table('items')->insertMany([
            ['id' => 10, 'name' => 'ten'], ['name' => 'thirty', 'id' => 30],
        ]);
        $this->assertSame([10, 30], $ids);
        $this->assertSame('thirty', $db->table('items')->where('id', '=', 30)->first()['name']);
    }

    public function testTerminalsDoNotMutateSelectOrLimit(): void
    {
        $db = $this->database();
        $db->table('items')->insertMany([['name' => 'a'], ['name' => 'b']]);
        $query = $db->table('items');
        $query->first();
        $query->pluck('name');
        $this->assertCount(2, $query->get());
        $this->assertArrayHasKey('tenant', $query->get()[0]);
    }

    public function testAggregateUsesJoinsAndGroups(): void
    {
        $db = $this->database();
        $db->table('items')->insertMany([['tenant' => 1, 'name' => 'a'], ['tenant' => 1, 'name' => 'b'], ['tenant' => 2, 'name' => 'c']]);
        $this->assertSame(2, $db->table('items')->groupBy(['tenant'])->count());
    }

    public function testUniqueMetadataRejectsCompositeSubset(): void
    {
        $db = $this->database();
        $db->raw('CREATE TABLE pairs(a INTEGER, b INTEGER, UNIQUE(a,b))');
        $this->assertFalse($db->hasUniqueConstraint('pairs', ['a']));
        $this->assertTrue($db->hasUniqueConstraint('pairs', ['a', 'b']));
        $this->assertTrue($db->hasUniqueConstraint('items', ['id']));
    }

    public function testStreamDryRunAndConcurrentCursor(): void
    {
        $db = $this->database();
        $db->table('items')->insertMany([['name' => 'a'], ['name' => 'b'], ['name' => 'c']]);
        $seen = [];
        foreach ($db->table('items')->orderBy('id')->stream() as $row) {
            $seen[] = $row['name'];
            $db->table('items')->orderBy('id')->get();
        }
        $this->assertSame(['a', 'b', 'c'], $seen);
        $db->setTestMode(true);
        $this->assertSame([], iterator_to_array($db->table('items')->stream()));
    }

    public function testBulkFailureRollsBackEntireBatch(): void
    {
        $db = $this->database();
        try {
            $db->table('items')->insertMany([['name' => 'duplicate'], ['name' => 'duplicate']]);
            $this->fail('Duplicate unique values must throw.');
        } catch (\PDOException $e) {
            $this->assertSame(0, $db->table('items')->count());
        }
    }

    public function testChunkByIdAllowsDeletionDuringIteration(): void
    {
        $db = $this->database();
        $db->table('items')->insertMany([['name' => 'a'], ['name' => 'b'], ['name' => 'c']]);
        $seen = [];
        $db->table('items')->chunkById(2, function (array $rows) use ($db, &$seen): void {
            array_push($seen, ...array_column($rows, 'name'));
            $db->table('items')->whereIn('id', array_column($rows, 'id'))->delete();
        });
        $this->assertSame(['a', 'b', 'c'], $seen);
        $this->assertSame(0, $db->table('items')->count());
    }

    public function testTimeoutRestoresConnectionSetting(): void
    {
        $db = $this->database();
        $pdo = $db->choosePdo('select');
        $pdo->exec('PRAGMA busy_timeout = 321');
        $db->table('items')->timeout(5)->get();
        $this->assertSame(321, (int)$pdo->query('PRAGMA busy_timeout')->fetchColumn());
    }

    public function testExplicitRawReadAndExecute(): void
    {
        $db = $this->database();
        $this->assertSame(1, $db->execute('INSERT INTO items(name) VALUES (:name)', ['name' => 'bound']));
        $db->setReadonly(true);
        $this->assertSame([['name' => 'bound']], $db->selectRaw('SELECT name FROM items WHERE id=:id', ['id' => 1]));
        $this->expectException(\InvalidArgumentException::class);
        $db->selectRaw('SELECT 1; DELETE FROM items');
    }

    public function testDialectCompilation(): void
    {
        foreach (['mysql', 'pgsql', 'sqlsrv', 'oci'] as $driver) {
            $pdo = $this->createMock(PDO::class);
            $pdo->method('getAttribute')->willReturn($driver);
            $db = new DBF($pdo);
            $db->setTestMode(true);
            $db->table('items')->select(['items.id AS identity'])->orderBy('items.id')->limit(5)->offset(2)->get();
            $sql = $db->queryString();
            $this->assertStringContainsString('SELECT', $sql);
            $this->assertStringContainsString($driver === 'sqlsrv' || $driver === 'oci' ? 'FETCH NEXT 5 ROWS ONLY' : 'LIMIT 5 OFFSET 2', $sql);
            if ($driver === 'pgsql') {
                $db->table('items')->whereJson('data->profile->name', '=', 'Tony')->get();
                $this->assertStringContainsString("#>> '{profile,name}'", $db->queryString());
            }
        }
    }
}
