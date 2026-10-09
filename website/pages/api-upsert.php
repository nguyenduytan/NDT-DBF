<section id="api-upsert" class="doc-section">
<h2>upsert()</h2>
<p><code>upsert(array $data, array $conflict, array $updateColumns): int</code> inserts a row or updates a conflicting row. The conflict columns need a real unique constraint.</p>
<pre><code class="language-php">$affected = $db-&gt;table('users')-&gt;upsert(
    ['email' =&gt; 'a@ndtan.net', 'status' =&gt; 'vip'],
    conflict: ['email'],
    updateColumns: ['status']
);</code></pre>
<ul><li>MySQL/MariaDB use ON DUPLICATE KEY UPDATE. Any unique key can trigger the update, not only the named conflict columns.</li><li>PostgreSQL and SQLite use ON CONFLICT.</li><li>SQL Server and Oracle are unsupported and throw.</li></ul>
<p>Update columns must exist in the insert data. An empty update list preserves the existing row. Affected counts follow the database driver and are not a portable inserted-versus-updated indicator. Scopes do not constrain conflict handling.</p>
</section>
