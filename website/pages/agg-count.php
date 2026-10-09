<div id="agg-count" class="doc-section">
  <h2>count()</h2>
  <p>Counts matching rows, preserving joins. With GROUP BY, counts the result groups.</p>
  <pre ><code class="language-php">$n = $db->table('users')->where('status','=','active')->count();</code></pre>
</div>
