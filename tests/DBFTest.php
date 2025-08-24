<?php declare(strict_types=1);

use PHPUnit\Framework\TestCase;
use ndtan\DBF;

final class DBFTest extends TestCase
{
    private DBF $db;

    protected function setUp(): void
    {
        // Enable soft delete on 'deleted_at' (timestamp mode)
        $this->db = new DBF([
            'type' => 'sqlite',
            'database' => ':memory:',
            'logging' => false,
            'features' => [
                'soft_delete' => ['enabled'=>true,'column'=>'deleted_at','mode'=>'timestamp'],
                'max_in_params' => 1000
            ]
        ]);

        // Minimal schema
        $this->db->raw("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT UNIQUE, status TEXT, deleted_at TEXT NULL)");
        $this->db->raw("CREATE TABLE orders (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, total INTEGER, created_at TEXT)");
        $this->db->raw("CREATE TABLE order_items (id INTEGER PRIMARY KEY AUTOINCREMENT, order_id INTEGER, sku TEXT, qty INTEGER)");
    }

    public function testInsertSelectUpdateDelete(): void
    {
        $id = $this->db->table('users')->insert(['email'=>'a@x.com','status'=>'active']);
        $this->assertGreaterThan(0, $id);

        $row = $this->db->table('users')->where('id','=', $id)->first();
        $this->assertSame('a@x.com', $row['email']);

        $aff = $this->db->table('users')->where('id','=',$id)->update(['status'=>'vip']);
        $this->assertSame(1, $aff);

        $deleted = $this->db->table('users')->where('id','=',$id)->delete(); // soft delete
        $this->assertSame(1, $deleted);

        // Default SELECT filters soft deleted rows
        $this->assertNull($this->db->table('users')->where('id','=',$id)->first());

        // withTrashed shows it
        $trashed = $this->db->table('users')->withTrashed()->where('id','=',$id)->first();
        $this->assertNotNull($trashed);

        // restore
        $restored = $this->db->table('users')->where('id','=',$id)->restore();
        $this->assertSame(1, $restored);

        $hard = $this->db->table('users')->where('id','=',$id)->forceDelete();
        $this->assertSame(1, $hard);
    }

    public function testInsertManyAndJoin(): void
    {
        $u1 = $this->db->table('users')->insert(['email'=>'u1@x.com','status'=>'active']);
        $u2 = $this->db->table('users')->insert(['email'=>'u2@x.com','status'=>'active']);

        $o1 = $this->db->table('orders')->insert(['user_id'=>$u1,'total'=>100,'created_at'=>'2025-01-01']);
        $o2 = $this->db->table('orders')->insert(['user_id'=>$u2,'total'=>200,'created_at'=>'2025-01-02']);

        $this->db->table('order_items')->insertMany([
            ['order_id'=>$o1,'sku'=>'A','qty'=>1],
            ['order_id'=>$o1,'sku'=>'B','qty'=>2],
            ['order_id'=>$o2,'sku'=>'A','qty'=>3],
        ]);

        $out = $this->db->table('orders')->select(['orders.id','users.email'])
            ->join('users','orders.user_id','=','users.id')
            ->orderBy('orders.id','asc')->get();

        $this->assertCount(2, $out);
        $this->assertSame('u1@x.com', $out[0]['email']);
    }

    public function testUpsert(): void
    {
        $this->db->table('users')->upsert(
            ['email'=>'a@x.com','status'=>'active'],
            conflict: ['email'],
            updateColumns: ['status']
        );

        // Re-run with different status => should update
        $this->db->table('users')->upsert(
            ['email'=>'a@x.com','status'=>'vip'],
            conflict: ['email'],
            updateColumns: ['status']
        );

        $row = $this->db->table('users')->where('email','=','a@x.com')->first();
        $this->assertSame('vip', $row['status']);
    }

    public function testTransactionRollbackOnException(): void
    {
        $this->expectException(RuntimeException::class);

        $this->db->tx(function(DBF $tx){
            $tx->table('users')->insert(['email'=>'tx@x.com','status'=>'active']);
            // force error
            throw new RuntimeException('boom');
        });

        // rollback should remove the user
        $exists = $this->db->table('users')->where('email','=','tx@x.com')->exists();
        $this->assertFalse($exists);
    }

    public function testAggregatesAndPluck(): void
    {
        $this->db->table('orders')->insertMany([
            ['user_id'=>1,'total'=>10,'created_at'=>'2025-01-01'],
            ['user_id'=>1,'total'=>20,'created_at'=>'2025-01-02'],
            ['user_id'=>1,'total'=>30,'created_at'=>'2025-01-03'],
        ]);

        $this->assertSame(60, $this->db->table('orders')->sum('total'));
        $this->assertEquals(20.0, $this->db->table('orders')->avg('total'));
        $this->assertSame(10, $this->db->table('orders')->min('total'));
        $this->assertSame(30, $this->db->table('orders')->max('total'));

        $this->db->table('users')->insert(['email'=>'p1@x.com','status'=>'active']);
        $this->db->table('users')->insert(['email'=>'p2@x.com','status'=>'active']);
        $emails = $this->db->table('users')->pluck('email');
        $this->assertContains('p1@x.com', $emails);
        $map = $this->db->table('users')->pluck('email','id');
        $this->assertArrayHasKey(1, $map);
    }

    public function testRawNamedAndPositional(): void
    {
        $this->db->table('users')->insert(['email'=>'r@x.com','status'=>'active']);
        $r1 = $this->db->raw('SELECT * FROM users WHERE email LIKE ?', ['%x.com']);
        $this->assertNotEmpty($r1);
        $r2 = $this->db->raw('SELECT * FROM users WHERE email = :e', ['e'=>'r@x.com']);
        $this->assertSame('r@x.com', $r2[0]['email']);
    }

    public function testReadonlyMode(): void
    {
        $this->db->setReadonly(true);
        $this->expectException(RuntimeException::class);
        $this->db->table('users')->insert(['email'=>'ro@x.com','status'=>'active']);
    }

    public function testTestMode(): void
    {
        $this->db->setTestMode(true);
        $this->assertSame([], $this->db->table('users')->where('status','=','active')->get());
        $this->assertStringContainsString('SELECT', $this->db->queryString());
        $aff = $this->db->table('users')->where('id','=',123)->update(['status'=>'x']);
        $this->assertSame(0, $aff);
        $this->assertStringContainsString('UPDATE', $this->db->queryString());
    }

    public function testWhereInGuard(): void
    {
        $big = range(1, 1001);
        $this->expectException(LengthException::class);
        $this->db->table('users')->whereIn('id', $big)->get();
    }

    public function testScopeAndPolicy(): void
    {
        $dbScoped = $this->db->withScope(['status'=>'active']);
        $dbScoped->table('users')->insert(['email'=>'s1@x.com','status'=>'active']);
        $this->db->table('users')->insert(['email'=>'s2@x.com','status'=>'vip']);
        $rows = $dbScoped->table('users')->get();
        $this->assertCount(1, $rows);

        $blocked = (clone $dbScoped)->policy(function(array $ctx){
            if (($ctx['table'] ?? null) === 'users' && ($ctx['action'] ?? '') === 'update') {
                throw new RuntimeException('policy_block');
            }
        });
        $this->expectException(RuntimeException::class);
        $blocked->table('users')->where('email','=','s1@x.com')->update(['status'=>'x']);
    }

    public function testKeysetPagination(): void
    {
        for ($i=0; $i<120; $i++) {
            $this->db->table('orders')->insert(['user_id'=>1,'total'=>$i+1,'created_at'=>'2025-01-01']);
        }
        $p1 = $this->db->table('orders')->orderBy('id','desc')->limit(50)->getKeyset(null,'id');
        $this->assertCount(50, $p1['data']);
        $p2 = $this->db->table('orders')->orderBy('id','desc')->limit(50)->getKeyset($p1['next'],'id');
        $this->assertCount(50, $p2['data']);
    }
}
