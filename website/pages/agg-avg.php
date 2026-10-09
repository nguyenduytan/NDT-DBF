<div id="agg-avg" class="doc-section">
  <h2>avg()</h2>
  <p>Returns a scalar. Grouped queries producing multiple rows throw; use an AVG alias with select() and get().</p>
  <pre ><code class="language-php">$v = $db->table('orders')->avg('total');</code></pre>
</div>
