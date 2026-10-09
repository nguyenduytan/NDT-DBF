<section id="intro" class="doc-section">
  <h2>Introduction</h2>
  <p>NDT DBF provides a query builder and raw SQL execution in one PHP file. This reference covers v<?= e($config['version']) ?>; examples use <code>$db</code> from the connection section.</p>
  <p>Query builders are mutable: start with <code>table()</code> for a fresh query, or clone an existing builder before changing it. <code>first()</code> and <code>pluck()</code> use clones internally. <code>withScope()</code>, <code>policy()</code> and <code>use()</code> return a cloned DBF instance; retain their return value.</p>
  <p>MySQL/MariaDB, PostgreSQL and SQLite support upsert and JSON operations. SQL Server and Oracle support connections and basic pagination; unsupported advanced operations throw. DBF does not include a model or migration layer.</p>
</section>
