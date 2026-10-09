<section id="json" class="doc-section">
<h2>JSON queries and updates</h2>
<p>MySQL/MariaDB, PostgreSQL and SQLite support JSON helpers. SQL Server and Oracle throw for these operations. Use valid JSON in the column; PostgreSQL updates operate on JSONB.</p>
<h3>whereJson()</h3>
<pre><code class="language-php">$users = $db-&gt;table('users')
    -&gt;whereJson('profile-&gt;preferences-&gt;theme', '=', 'dark')
    -&gt;get();</code></pre>
<p>Read paths use <code>column-&gt;key-&gt;nested_key</code>. Keys contain letters, digits or underscores; arbitrary JSONPath expressions are not supported. MySQL and PostgreSQL extract text, while SQLite returns scalar values according to JSON type; numeric and boolean comparisons are not identical across drivers.</p>
<h3>jsonSet()</h3>
<pre><code class="language-php">$db-&gt;table('users')
    -&gt;where('id', '=', 10)
    -&gt;jsonSet('profile', [
        'preferences.theme' =&gt; 'dark',
        'preferences.notifications' =&gt; true,
        'tags' =&gt; ['php', 'sql'],
    ]);</code></pre>
<p>Update paths use dot notation relative to the column. Values are JSON encoded, preserving strings, numbers, booleans, arrays and null. Multiple paths are applied in one UPDATE. PostgreSQL requires intermediate objects to exist for nested paths; this example assumes <code>preferences</code> is present.</p>
<p><code>jsonSet()</code> executes immediately and returns the query builder, not an affected row count. Use a WHERE condition to target the intended rows.</p>
</section>
