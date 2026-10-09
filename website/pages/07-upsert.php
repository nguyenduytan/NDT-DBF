<div id="upsert" class="doc-section">
  <h2>Upsert</h2>
  <pre ><code class="language-php">$db->table('users')->upsert(
  ['email'=>'a@ndtan.net','status'=>'vip'],
  conflict: ['email'],
  updateColumns: ['status']
);</code></pre>
</div>
<p>Requires a unique constraint on the conflict columns. Supported on MySQL/MariaDB, PostgreSQL and SQLite; SQL Server and Oracle throw.</p>
