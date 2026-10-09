<div id="agg-sum" class="doc-section">
  <h2>sum()</h2>
  <p>Returns a scalar. Grouped queries producing multiple rows throw; use a SUM alias with select() and get().</p>
  <pre ><code class="language-php">$v = $db->table('orders')->sum('total');</code></pre>
</div>
