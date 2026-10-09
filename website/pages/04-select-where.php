<div id="select-where" class="doc-section">
  <h2>Select &amp; Where</h2>
  <pre ><code class="language-php">$rows = $db->table('users')
  ->select(['id', 'email'])
  ->where('status', '=', 'active')
  ->orWhere('email', '=', 'a@ndtan.net')
  ->orderBy('id','desc')
  ->limit(20)
  ->get();

$one = $db->table('users')->where('id','=',10)->first();
$exists = $db->table('users')->where('email','=','a@ndtan.net')->exists();</code></pre>

  <h3>IN / BETWEEN / NULL</h3>
  <pre ><code class="language-php">$top = $db->table('users')->whereIn('id', [1,2,3])->get();
$range = $db->table('orders')->whereBetween('total', [100, 500])->get();
$nulls = $db->table('users')->whereNull('deleted_at')->get();</code></pre>
</div>
