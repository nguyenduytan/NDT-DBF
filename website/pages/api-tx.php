<section id="api-tx" class="doc-section">
  <h2>Transactions</h2>
<p><code>tx(callable $callback, int $attempts = 3): mixed</code> commits on success, rolls back on failure and returns the callback result. Queries inside the transaction use the writer.</p>
<pre><code class="language-php">$orderId = $db-&gt;tx(function (ndtan\DBF $tx) {
    $id = $tx-&gt;table('orders')-&gt;insert(['user_id' =&gt; 10, 'total' =&gt; 200]);
    $tx-&gt;table('order_items')-&gt;insert(['order_id' =&gt; $id, 'sku' =&gt; 'A', 'qty' =&gt; 1]);
    return $id;
}, attempts: 3);</code></pre>
<p><code>attempts</code> must be positive. Only recognized retryable database errors trigger a retry of the outer transaction. Other errors are rethrown immediately; exhausted retries rethrow the last exception. Do not put email, HTTP calls or other non-transactional side effects inside a callback that may run again.</p>
<h3>Nested transactions</h3>
<pre><code class="language-php">$db-&gt;tx(function (ndtan\DBF $tx) {
    $tx-&gt;table('users')-&gt;where('id', '=', 10)-&gt;update(['status' =&gt; 'active']);
    $tx-&gt;tx(function (ndtan\DBF $nested) {
        $nested-&gt;table('audit')-&gt;insert(['message' =&gt; 'User activated']);
    });
});</code></pre>
<p>Nested calls use savepoints on MySQL, PostgreSQL and SQLite. <code>tx()</code> is unsupported on SQL Server and Oracle and throws. Readonly mode and test mode also reject transactions. A nested call does not retry independently. DBF must own the transaction; do not mix <code>tx()</code> with transaction control on an injected PDO connection. Statements such as MySQL DDL may commit implicitly.</p>
</section>
