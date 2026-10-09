<section id="rawsql" class="doc-section">
  <h2>Raw SQL</h2>
<p>Bind values and keep the SQL itself trusted. Raw SQL does not automatically add builder scopes or soft-delete filters.</p>
<pre><code class="language-php">$rows = $db-&gt;selectRaw(
    'SELECT id, email FROM users WHERE status = ? AND (email = ? OR email = ?)',
    ['active', 'a@ndtan.net', 'b@ndtan.net']
);

$affected = $db-&gt;execute(
    'UPDATE users SET status = :status WHERE id = :id',
    ['status' =&gt; 'vip', 'id' =&gt; 10]
);</code></pre>
<p><code>selectRaw(): array</code> fetches rows from a single SELECT starting with SELECT, without semicolons. Use <code>raw()</code> for CTEs and other statements. <code>execute(): int</code> executes a statement on the writer and returns its affected row count; DDL counts are driver-dependent. Statements returning rows throw after execution; use <code>raw()</code> for INSERT/UPDATE RETURNING.</p>
<p><code>raw(): array|int</code> returns associative rows when the statement has result columns (<code>columnCount() &gt; 0</code>), otherwise an affected row count. It always uses the writer and is blocked in readonly mode, including SELECT, WITH and PRAGMA. Use <code>selectRaw()</code> for explicit SELECT fetching with read routing. SQL text cannot guarantee read-only behavior: use a database read-only account. Never pass write statements to <code>selectRaw()</code>.</p>
</section>
