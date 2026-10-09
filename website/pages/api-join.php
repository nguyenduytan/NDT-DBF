<div id="api-join" class="doc-section">
  <h2>join()</h2>
  <p>Combine rows from related tables with ON conditions.</p>
  <pre ><code class="language-php">$rows = $db->table('orders')
  ->join('users','orders.user_id','=','users.id')
  ->select(['orders.id','users.email'])
  ->orderBy('orders.id','asc')
  ->get();</code></pre>

  <details class="spoiler"><summary>Notes</summary>
    <ul>
      <li>Available joins: <code>join()</code>, <code>leftJoin()</code> (others depend on driver).</li>
      <li>Paths like <code>users.id</code> are quoted part-by-part.</li>
    </ul>
  </details>
</div>
