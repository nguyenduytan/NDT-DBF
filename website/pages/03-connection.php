<section id="connection" class="doc-section">
  <h2>Connections</h2>
<h3>URI or PDO DSN</h3>
<pre><code class="language-php">$db = new ndtan\DBF('sqlite::memory:');
$db = new ndtan\DBF('mysql://user:password@127.0.0.1:3306/app?charset=utf8mb4');
$db = new ndtan\DBF('pgsql://user:password@127.0.0.1:5432/app');
$db = new ndtan\DBF([
    'dsn' =&gt; 'mysql:host=127.0.0.1;dbname=app;charset=utf8mb4',
    'username' =&gt; 'user',
    'password' =&gt; 'password',
]);</code></pre>
<p>Percent-encode credentials in a URI. Use a DSN configuration array when credentials are separate, or when the driver needs its own DSN options.</p>
<p>Direct PDO DSNs support <code>mysql:</code>, <code>pgsql:</code>, <code>sqlsrv:</code> and <code>oci:</code>, as well as SQLite. For example, <code>new ndtan\DBF('pgsql:host=localhost;dbname=app')</code>. Supply separate credentials with the <code>dsn</code> array form.</p>
<h3>Existing PDO</h3>
<pre><code class="language-php">$pdo = new PDO('sqlite::memory:');
$db = new ndtan\DBF(['pdo' =&gt; $pdo]);</code></pre>
<h3>Read and write connections</h3>
<pre><code class="language-php">$db = new ndtan\DBF([
    'write' =&gt; 'mysql://user:password@primary/app',
    'read' =&gt; 'mysql://user:password@replica/app',
    'routing' =&gt; 'auto',
]);</code></pre>
<p><code>auto</code> routes reads to the reader and writes to the writer. Transactions pin queries to the writer. In <code>manual</code> mode use <code>$db-&gt;using('read')</code> or <code>$db-&gt;using('write')</code>; writes still use the writer. <code>using(null)</code> resets to the writer. Read replicas can lag behind writes.</p>
<p>With no argument, DBF reads the <code>NDTAN_DBF_URL</code> environment variable. Keep credentials outside source control.</p>
</section>
