<div id="api-order-limit" class="doc-section">
  <h2>orderBy() · limit()</h2>
  <p>Sorting and windowing.</p>
  <pre ><code class="language-php">$rows = $db->table('users')
  ->orderBy('id','desc')
  ->limit(20)
  ->get();</code></pre>

  <details class="spoiler"><summary>Tip</summary>
    <ul><li>For large datasets, prefer <b>keyset pagination</b> instead of deep offsets.</li></ul>
  </details>
</div>
<p>Directions are ASC or DESC. Limit and offset must be non-negative; SQL Server requires a positive limit and an explicit order for pagination. Use deterministic ordering on every driver.</p>
