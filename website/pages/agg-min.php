<div id="agg-min" class="doc-section">
  <h2>min()</h2>
  <p>Returns a scalar. Grouped queries producing multiple rows throw; use a MIN alias with select() and get().</p>
  <pre ><code class="language-php">$v = $db->table('orders')->min('total');</code></pre>
</div>
