<section id="api-middleware" class="doc-section">
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
