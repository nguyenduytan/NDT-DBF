<div id="agg-max" class="doc-section">
  <h2>max()</h2>
  <p>Returns a scalar. Grouped queries producing multiple rows throw; use a MAX alias with select() and get().</p>
  <pre ><code class="language-php">$v = $db->table('orders')->max('total');</code></pre>
</div>
