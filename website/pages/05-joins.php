<div id="joins" class="doc-section">
  <h2>Joins</h2>
  <pre ><code class="language-php">$rows = $db->table('orders')
  ->join('users', 'orders.user_id', '=', 'users.id')
  ->select(['orders.id','users.email'])
  ->orderBy('orders.id','asc')
  ->get();</code></pre>
</div>
