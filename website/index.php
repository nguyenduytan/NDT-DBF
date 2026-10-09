<?php
$config = require __DIR__ . '/config.php';
$pageTitle = $config['brand_name'];
include __DIR__ . '/includes/header.php';
?>
<main id="main" class="home wrap">
  <section class="overview">
    <h1>NDT DBF</h1>
    <p class="lead">A single-file PHP database framework.</p>
    <p>Write SQL with a compact query builder or bound raw queries. One PHP file, PDO, and no runtime dependencies beyond your database driver.</p>
    <?php include __DIR__ . '/includes/badges.php'; ?>
    <div class="actions">
      <a class="btn btn-primary" href="<?= e($config['docs_url']) ?>#installation">Get started</a>
      <a href="<?= e($config['download_raw']) ?>">Download DBF.php v<?= e($config['version']) ?></a>
      <a href="<?= e($config['release_url']) ?>">Release notes</a>
    </div>
  </section>
  <section class="quickstart">
    <h2>Quick start</h2>
    <p>Download <code>DBF.php</code> into your project. This SQLite example creates its own in-memory table.</p>
    <pre><code class="language-php">&lt;?php
require __DIR__ . '/DBF.php';

$db = new ndtan\DBF('sqlite::memory:');
$db-&gt;execute('CREATE TABLE users (
    id INTEGER PRIMARY KEY, email TEXT UNIQUE, status TEXT
)');
$db-&gt;table('users')-&gt;insert([
    'email' =&gt; 'hello@ndtan.net',
    'status' =&gt; 'active',
]);

$users = $db-&gt;table('users')
    -&gt;select(['id', 'email'])
    -&gt;where('status', '=', 'active')
    -&gt;orderBy('id', 'asc')
    -&gt;get();</code></pre>
    <p>Prefer Composer? <code>composer require ndtan/dbf:0.3.1</code>, then load <code>vendor/autoload.php</code>.</p>
  </section>
  <section class="guide-index">
    <h2>Explore the documentation</h2>
    <dl>
      <div><dt><a href="docs.php#connection">Connect your database</a></dt><dd>PDO, DSN and URI configuration; read and write routing.</dd></div>
      <div><dt><a href="docs.php#where-syntax">Build a query</a></dt><dd>Conditions, joins, aggregates and pagination.</dd></div>
      <div><dt><a href="docs.php#api-tx">Work with transactions</a></dt><dd>Writer pinning, retries and nested savepoints.</dd></div>
      <div><dt><a href="docs.php#json">Query and update JSON</a></dt><dd>JSON paths and the supported database dialects.</dd></div>
    </dl>
    <p>MySQL/MariaDB, PostgreSQL and SQLite support the core feature set. SQL Server and Oracle support connections and basic pagination; advanced features have explicit driver limits.</p>
  </section>
</main>
<?php include __DIR__ . '/includes/footer.php'; ?>
