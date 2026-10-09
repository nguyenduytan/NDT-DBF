<section id="configref" class="doc-section">
  <h2>Configuration reference</h2>
<p>Connection settings can be supplied as an array, URI, DSN or existing PDO. MariaDB uses the <code>mysql</code> driver; Oracle configuration uses <code>oracle</code> and the PDO OCI extension.</p>
<pre><code class="language-php">$db = new ndtan\DBF([
    'type' =&gt; 'mysql',
    'host' =&gt; '127.0.0.1',
    'port' =&gt; 3306,
    'database' =&gt; 'app',
    'username' =&gt; 'user',
    'password' =&gt; 'password',
    'charset' =&gt; 'utf8mb4',
    'prefix' =&gt; 'ndt_',
    'readonly' =&gt; false,
    'options' =&gt; [PDO::ATTR_TIMEOUT =&gt; 5],
    'features' =&gt; [
        'soft_delete' =&gt; [
            'enabled' =&gt; true,
            'column' =&gt; 'deleted_at',
            'mode' =&gt; 'timestamp',
        ],
        'max_in_params' =&gt; 1000,
    ],
]);</code></pre>
<div class="table-scroll"><table><thead><tr><th>Setting</th><th>Meaning</th></tr></thead><tbody>
<tr><td><code>type</code></td><td>mysql, pgsql, sqlite, sqlsrv or oracle. Default: mysql.</td></tr>
<tr><td><code>host</code>, <code>port</code>, <code>database</code></td><td>Driver connection settings. SQLite database is a file path or :memory:.</td></tr>
<tr><td><code>username</code>, <code>password</code>, <code>charset</code></td><td>Credentials and driver-specific character set.</td></tr>
<tr><td><code>dsn</code>, <code>pdo</code></td><td>Explicit PDO DSN or an existing PDO connection.</td></tr>
<tr><td><code>options</code> / <code>option</code></td><td>PDO attribute map; option is the legacy alias. Attributes depend on your driver.</td></tr>
<tr><td><code>emulate_prepares</code></td><td>Defaults to false. DBF enforces PDO exception mode for reliable failure handling.</td></tr>
<tr><td><code>write</code>, <code>read</code>, <code>routing</code></td><td>Connection definitions plus single, auto or manual routing. Routing is configured for a read/write setup.</td></tr>
<tr><td><code>prefix</code>, <code>readonly</code></td><td>Table prefix and write guard. Defaults: empty prefix and false.</td></tr>
<tr><td><code>logger</code>, <code>metrics</code></td><td>Callbacks; also available through setLogger() and setMetrics().</td></tr>
<tr><td><code>features.max_in_params</code></td><td>Maximum IN-list size; default 1000.</td></tr>
<tr><td><code>features.soft_delete</code></td><td>enabled, column, mode and deleted_value. Defaults: false, deleted_at, timestamp and 1. Flag mode uses deleted_value.</td></tr>
</tbody></table></div>
<p>Register policies and middleware through <code>policy()</code> and <code>use()</code>. No configuration keys for migrations, models, pooling or automatic caching are provided.</p>
<h3>Timeout and test mode</h3>
<pre><code class="language-php">$rows = $db-&gt;table('users')-&gt;timeout(1000)-&gt;limit(20)-&gt;get();
$db-&gt;setTestMode(true);
$db-&gt;table('users')-&gt;where('id', '=', 10)-&gt;get();
$sql = $db-&gt;queryString();
$params = $db-&gt;queryParams();
$db-&gt;setTestMode(false);</code></pre>
<p><code>timeout()</code> is best-effort and driver-dependent, not a portable execution deadline. Test mode suppresses query execution, but construction still connects and schema inspection may query the database; it is not an offline SQL compiler.</p>
</section>
