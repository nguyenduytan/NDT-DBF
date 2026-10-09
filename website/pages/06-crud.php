<div id="crud" class="doc-section">
  <h2>CRUD</h2>
  <pre ><code class="language-php">$id = $db->table('users')->insert(['email'=>'a@ndtan.net','status'=>'active']);
$db->table('users')->where('id','=', $id)->update(['status'=>'vip']);
$db->table('users')->where('id','=', $id)->delete();

$row = $db->table('users')->insertGet(['email'=>'b@ndtan.net','status'=>'vip'], ['id','email']);

$db->table('users')->insertMany([
  ['email'=>'p1@ndtan.net','status'=>'active'],
  ['email'=>'p2@ndtan.net','status'=>'vip'],
]);</code></pre>
</div>
<p>Use WHERE conditions for updates and deletes. Soft deletion requires an enabled feature and its configured column. See <a href="#api-insert">insert return-value limits</a>.</p>
