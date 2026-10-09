<section id="api-select" class="doc-section">
  <h2>select(), groupBy() and having()</h2>
<p><code>select(array $columns)</code> accepts identifiers, qualified identifiers, aliases and supported aggregates: COUNT, SUM, AVG, MIN and MAX. Arbitrary SQL expressions belong in <a href="#rawsql">raw SQL</a>.</p>
<pre><code class="language-php">$rows = $db-&gt;table('users')-&gt;select(['id', 'email'])-&gt;get();

$groups = $db-&gt;table('orders')
    -&gt;select(['user_id', 'COUNT(*) AS cnt'])
    -&gt;groupBy(['user_id'])
    -&gt;having('COUNT(*)', '&gt;', 1)
    -&gt;get();</code></pre>
<p><code>groupBy()</code> takes an array. Use the aggregate expression in <code>having()</code> for portable SQL; alias support varies by database.</p>
</section>
