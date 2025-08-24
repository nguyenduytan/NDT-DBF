<?php

namespace ndtan;

use PHPUnit\Framework\TestCase;
use PDO;

final class DBFTest extends TestCase
{
    private DBF $db;

    protected function setUp(): void
    {
        parent::setUp();
        $this->db = new DBF('sqlite::memory:', [
            'features' => [
                'soft_delete' => [
                    'enabled' => true,
                    'column' => 'deleted_at',
                    'mode' => 'timestamp',
                ],
            ],
        ]);
        // Create table with necessary columns and UNIQUE constraint
        $this->db->raw('CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            status TEXT,
            score INTEGER,
            data TEXT,
            deleted_at TEXT
        )');
    }

    public function testInsertSelectUpdateDelete(): void
    {
        // Insert a record
        $this->db->table('users')->insert([
            'email' => 'a@ndtan.net',
            'status' => 'active',
            'score' => 10,
        ]);
        // Select to verify insertion
        $row = $this->db->table('users')->select(['email'])->where('email', '=', 'a@ndtan.net')->first();
        $this->assertSame('a@ndtan.net', $row['email']);
        // Update the record
        $this->db->table('users')->where('email', '=', 'a@ndtan.net')->update(['score' => 20]);
        $row = $this->db->table('users')->select(['score'])->where('email', '=', 'a@ndtan.net')->first();
        $this->assertSame(20, $row['score']);
        // Soft delete
        $this->db->table('users')->where('email', '=', 'a@ndtan.net')->delete();
        $row = $this->db->table('users')->where('email', '=', 'a@ndtan.net')->first();
        $this->assertNull($row);
        // Verify soft delete with withTrashed
        $row = $this->db->table('users')->withTrashed()->where('email', '=', 'a@ndtan.net')->first();
        $this->assertNotNull($row['deleted_at'], 'Soft delete should set deleted_at');
    }

    public function testInsertMany(): void
    {
        $rows = [
            ['email' => 'b@ndtan.net', 'status' => 'active', 'score' => 10],
            ['email' => 'c@ndtan.net', 'status' => 'inactive', 'score' => 20],
        ];
        $ids = $this->db->table('users')->insertMany($rows);
        $this->assertCount(2, $ids);
        $count = $this->db->table('users')->count();
        $this->assertSame(2, $count);
    }

    public function testInsertGet(): void
    {
        $row = $this->db->table('users')->insertGet(
            ['email' => 'd@ndtan.net', 'status' => 'active', 'score' => 30],
            ['id', 'email']
        );
        $this->assertIsArray($row);
        $this->assertSame('d@ndtan.net', $row['email']);
        $this->assertGreaterThan(0, $row['id']);
    }

    public function testUpsert(): void
    {
        // Initial insert
        $this->db->table('users')->insert([
            'email' => 'e@ndtan.net',
            'status' => 'active',
            'score' => 40,
        ]);
        // Upsert to update status
        $count = $this->db->table('users')->upsert(
            ['email' => 'e@ndtan.net', 'status' => 'vip', 'score' => 50],
            ['email'],
            ['status', 'score']
        );
        $this->assertSame(1, $count);
        $row = $this->db->table('users')->select(['status', 'score'])->where('email', '=', 'e@ndtan.net')->first();
        $this->assertSame('vip', $row['status']);
        $this->assertSame(50, $row['score']);
    }

    public function testScopeAndPolicy(): void
    {
        // Insert test data
        $this->db->table('users')->insertMany([
            ['email' => 's1@ndtan.net', 'status' => 'active', 'score' => 100],
            ['email' => 's2@ndtan.net', 'status' => 'inactive', 'score' => 200],
        ]);
        // Apply scope
        $db = $this->db->withScope(['status' => 'active']);
        $row = $db->table('users')->select(['email'])->where('email', '=', 's1@ndtan.net')->first();
        $this->assertSame('s1@ndtan.net', $row['email']);
        // Verify scope filters out inactive
        $row = $db->table('users')->select(['email'])->where('email', '=', 's2@ndtan.net')->first();
        $this->assertNull($row);
        // Test policy
        $db = $db->policy(function($ctx) {
            if ($ctx['type'] === 'select' && $ctx['table'] === 'users') {
                return;
            }
            throw new \RuntimeException('Policy denied');
        });
        $row = $db->table('users')->select(['email'])->where('email', '=', 's1@ndtan.net')->first();
        $this->assertSame('s1@ndtan.net', $row['email']);
        $this->expectException(\RuntimeException::class);
        $db->table('users')->insert(['email' => 's3@ndtan.net']);
    }

    public function testWhereIn(): void
    {
        $this->db->table('users')->insertMany([
            ['email' => 'f@ndtan.net', 'status' => 'active', 'score' => 60],
            ['email' => 'g@ndtan.net', 'status' => 'active', 'score' => 70],
        ]);
        $rows = $this->db->table('users')->select(['email'])->whereIn('email', ['f@ndtan.net', 'g@ndtan.net'])->get();
        $this->assertCount(2, $rows);
        $this->assertSame(['f@ndtan.net', 'g@ndtan.net'], array_column($rows, 'email'));
    }

    public function testWhereBetween(): void
    {
        $this->db->table('users')->insertMany([
            ['email' => 'h@ndtan.net', 'status' => 'active', 'score' => 80],
            ['email' => 'i@ndtan.net', 'status' => 'active', 'score' => 90],
            ['email' => 'j@ndtan.net', 'status' => 'active', 'score' => 100],
        ]);
        $rows = $this->db->table('users')->select(['score'])->whereBetween('score', [85, 95])->get();
        $this->assertCount(1, $rows);
        $this->assertSame(90, $rows[0]['score']);
    }

    public function testWhereNull(): void
    {
        $this->db->table('users')->insert([
            'email' => 'k@ndtan.net',
            'status' => null,
            'score' => 110,
        ]);
        $row = $this->db->table('users')->select(['email'])->whereNull('status')->first();
        $this->assertSame('k@ndtan.net', $row['email']);
    }

    public function testPluck(): void
    {
        $this->db->table('users')->insertMany([
            ['email' => 'l@ndtan.net', 'status' => 'active', 'score' => 120],
            ['email' => 'm@ndtan.net', 'status' => 'active', 'score' => 130],
        ]);
        $emails = $this->db->table('users')->pluck('email', 'id');
        $this->assertArrayHasKey(1, $emails);
        $this->assertSame('l@ndtan.net', $emails[1]);
        $this->assertSame('m@ndtan.net', $emails[2]);
    }

    public function testSoftDeleteAndRestore(): void
    {
        $this->db->table('users')->insert(['email' => 'n@ndtan.net', 'status' => 'active']);
        $this->db->table('users')->where('email', '=', 'n@ndtan.net')->delete();
        $row = $this->db->table('users')->where('email', '=', 'n@ndtan.net')->first();
        $this->assertNull($row);
        $count = $this->db->table('users')->onlyTrashed()->where('email', '=', 'n@ndtan.net')->restore();
        $this->assertSame(1, $count, 'Restore should affect one record');
        $row = $this->db->table('users')->where('email', '=', 'n@ndtan.net')->first();
        $this->assertSame('n@ndtan.net', $row['email']);
        $this->assertNull($row['deleted_at'], 'Restored record should have null deleted_at');
    }

    public function testForceDelete(): void
    {
        $this->db->table('users')->insert(['email' => 'o@ndtan.net', 'status' => 'active']);
        $this->db->table('users')->where('email', '=', 'o@ndtan.net')->forceDelete();
        $row = $this->db->table('users')->withTrashed()->where('email', '=', 'o@ndtan.net')->first();
        $this->assertNull($row);
    }

    public function testWhereJson(): void
    {
        // Check for SQLite json1 extension
        $pdo = $this->db->choosePdo('select');
        if ($pdo->getAttribute(PDO::ATTR_DRIVER_NAME) === 'sqlite' && !in_array('json1', $pdo->query('PRAGMA compile_options')->fetchAll(PDO::FETCH_COLUMN))) {
            $this->markTestSkipped('SQLite json1 extension not enabled');
        }
        $this->db->table('users')->insert([
            'email' => 'p@ndtan.net',
            'data' => json_encode(['name' => 'John']),
        ]);
        $row = $this->db->table('users')->select(['email'])->whereJson('data->name', '=', 'John')->first();
        $this->assertSame('p@ndtan.net', $row['email']);
    }

    public function testJsonSet(): void
    {
        // Check for SQLite json1 extension
        $pdo = $this->db->choosePdo('select');
        if ($pdo->getAttribute(PDO::ATTR_DRIVER_NAME) === 'sqlite' && !in_array('json1', $pdo->query('PRAGMA compile_options')->fetchAll(PDO::FETCH_COLUMN))) {
            $this->markTestSkipped('SQLite json1 extension not enabled');
        }
        $this->db->table('users')->insert([
            'email' => 'q@ndtan.net',
            'data' => json_encode(['name' => 'Jane']),
        ]);
        $this->db->table('users')->where('email', '=', 'q@ndtan.net')->jsonSet('data', ['name' => 'Doe']);
        $row = $this->db->table('users')->select(['data'])->where('email', '=', 'q@ndtan.net')->first();
        $this->assertSame('Doe', json_decode($row['data'], true)['name']);
    }

    public function testTransaction(): void
    {
        $this->db->tx(function($db) {
            $db->table('users')->insert(['email' => 'r@ndtan.net', 'status' => 'active']);
            $db->table('users')->insert(['email' => 's@ndtan.net', 'status' => 'active']);
        });
        $count = $this->db->table('users')->count();
        $this->assertSame(2, $count);
    }

    public function testKeysetPagination(): void
    {
        $this->db->table('users')->insertMany([
            ['email' => 't1@ndtan.net', 'score' => 1],
            ['email' => 't2@ndtan.net', 'score' => 2],
            ['email' => 't3@ndtan.net', 'score' => 3],
        ]);
        $result = $this->db->table('users')->select(['email', 'score'])->orderBy('score')->limit(2)->getKeyset(null, 'score');
        $this->assertCount(2, $result['data']);
        $this->assertNotNull($result['next']);
        $cursor = $result['next'];
        $result = $this->db->table('users')->select(['email', 'score'])->orderBy('score')->limit(2)->getKeyset($cursor, 'score');
        $this->assertCount(1, $result['data']);
        $this->assertNull($result['next']);
    }
}