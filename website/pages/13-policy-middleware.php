<section id="policy" class="doc-section">
  <h2>Policies and scopes</h2>
<p><code>policy()</code> returns a cloned DBF instance. Keep that instance to apply the guard. Context uses <code>type</code>, not <code>action</code>.</p>
<pre><code class="language-php">$guarded = $db-&gt;policy(function (array $ctx) {
    if (($ctx['type'] ?? '') === 'delete'
        &amp;&amp; str_starts_with($ctx['table'] ?? '', 'system_')) {
        throw new RuntimeException('Deleting system tables is not allowed.');
    }
});
$guarded-&gt;table('system_settings')-&gt;where('id', '=', 10)-&gt;delete();</code></pre>
<p>Raw SQL context contains <code>sql</code> instead of a builder table. A table-name guard alone does not protect raw operations. Soft deletion executes an update, so policies must cover that operation when required.</p>
<h3>Tenant scope</h3>
<pre><code class="language-php">$tenantDb = $db-&gt;withScope(['tenant_id' =&gt; 42]);
$users = $tenantDb-&gt;table('users')
    -&gt;where('status', '=', 'active')
    -&gt;orWhere('status', '=', 'vip')
    -&gt;get();</code></pre>
<p>Scope equality conditions are ANDed with the grouped user predicates. Scopes constrain builder WHERE clauses; they do not inject tenant values into inserts, constrain upsert conflicts or rewrite raw SQL. Set those values and enforce authorization explicitly.</p>
</section>
<section id="middleware" class="doc-section">
  <h2>Middleware and logging</h2>
<p><code>use()</code> returns a clone. Middleware receives execution context and must return the result from <code>$next($ctx)</code>.</p>
<pre><code class="language-php">$instrumented = $db-&gt;use(function (array $ctx, callable $next) {
    $start = microtime(true);
    $result = $next($ctx);
    error_log(sprintf(
        '[dbf] %s %s %.1fms',
        $ctx['type'] ?? 'query',
        $ctx['table'] ?? 'raw',
        (microtime(true) - $start) * 1000
    ));
    return $result;
});
$users = $instrumented-&gt;table('users')-&gt;get();

$db-&gt;setLogger(function (string $sql, array $params, float $ms) {
    error_log(sprintf('[dbf] %.1fms %s', $ms, $sql));
});
$db-&gt;setMetrics(function (array $metrics) {
    error_log(json_encode($metrics, JSON_THROW_ON_ERROR));
});</code></pre>
<p>For streaming queries, execution is lazy and middleware results may be generators. Avoid logging sensitive parameter values. Inspect the last query with <code>queryString()</code> and <code>queryParams()</code>.</p>
</section>
