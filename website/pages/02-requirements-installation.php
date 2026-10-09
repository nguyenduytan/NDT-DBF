<section id="requirements" class="doc-section">
  <h2>Requirements</h2>
<ul><li>PHP <?= e($config['php_version']) ?> or newer.</li><li>PDO and a database driver: <code>pdo_mysql</code>, <code>pdo_pgsql</code>, <code>pdo_sqlite</code>, <code>pdo_sqlsrv</code> or <code>pdo_oci</code>.</li><li>Database versions must support the SQL features you use. SQLite JSON operations require JSON functions.</li></ul>
<p>The release CI covers PHP 8.1&ndash;8.5, with real MySQL 8.4 and PostgreSQL 16 integration tests. See the <a href="<?= e($config['ci_url']) ?>">CI workflow and run results</a>.</p>
</section>
<section id="installation" class="doc-section">
  <h2>Installation</h2>
<h3>Single file</h3><p><a href="<?= e($config['download_raw']) ?>">Download DBF.php v<?= e($config['version']) ?></a> into your project.</p>
<pre><code class="language-php">&lt;?php
require __DIR__ . '/DBF.php';
$db = new ndtan\DBF('sqlite::memory:');</code></pre>
<h3>Composer</h3><pre><code class="language-bash">composer require ndtan/dbf:<?= e($config['version']) ?></code></pre>
<pre><code class="language-php">&lt;?php
require __DIR__ . '/vendor/autoload.php';
$db = new ndtan\DBF('mysql://user:password@127.0.0.1/app?charset=utf8mb4');</code></pre>
</section>
