<section id="api-policy" class="doc-section">
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
