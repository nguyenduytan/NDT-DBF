<section id="api-insert" class="doc-section">
<h2>Insert rows</h2>
<p><code>insert(array $data): int</code> returns the last insert ID as an integer. Use a schema with an auto-generated integer key; driver behavior for generated IDs varies.</p>
<pre><code class="language-php">$id = $db-&gt;table('users')-&gt;insert([
    'email' =&gt; 'a@ndtan.net',
    'status' =&gt; 'active',
]);</code></pre>
<h3>insertMany()</h3>
<pre><code class="language-php">$ids = $db-&gt;table('users')-&gt;insertMany([
    ['email' =&gt; 'p1@ndtan.net', 'status' =&gt; 'active'],
    ['email' =&gt; 'p2@ndtan.net', 'status' =&gt; 'vip'],
]);</code></pre>
<p><code>insertMany(array $rows): array</code> inserts rows individually inside one transaction and returns their actual insert IDs in input order. A failure rolls back the batch. This favors correct IDs and atomicity over bulk-insert performance. Every row must have the same columns in the same order. Transaction support is required: MySQL/MariaDB, PostgreSQL or SQLite.</p>
<h3>insertGet()</h3>
<pre><code class="language-php">$row = $db-&gt;table('users')-&gt;insertGet(
    ['email' =&gt; 'b@ndtan.net', 'status' =&gt; 'vip'],
    ['id', 'email']
);</code></pre>
<p>PostgreSQL and SQLite use RETURNING. The fallback on other drivers selects by an integer column named <code>id</code>; use a schema that matches that assumption. Inserts do not inherit scope values automatically.</p>
</section>
