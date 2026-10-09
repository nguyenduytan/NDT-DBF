<section id="api-keyset" class="doc-section">
  <h2>Keyset pagination</h2>
<p><code>getKeyset(?string $cursor, string $key): array</code> returns <code>['data' =&gt; $rows, 'next' =&gt; $cursor]</code>.</p>
<pre><code class="language-php">$page1 = $db-&gt;table('posts')
    -&gt;select(['id', 'title'])
    -&gt;orderBy('id', 'desc')
    -&gt;limit(50)
    -&gt;getKeyset(null, 'id');

if ($page1['next'] !== null) {
    $page2 = $db-&gt;table('posts')
        -&gt;select(['id', 'title'])
        -&gt;orderBy('id', 'desc')
        -&gt;limit(50)
        -&gt;getKeyset($page1['next'], 'id');
}</code></pre>
<p>Use one unqualified, non-null unique key, select that key, order only by that key and set a positive limit. ASC and DESC are supported; multiple sort keys, offsets and grouped queries are not supported. Keep the same filters and direction across pages. Invalid cursors throw.</p>
<p><code>next === null</code> ends pagination. DBF fetches one extra row to determine whether another page exists. Cursors are encoded positions, not authorization tokens or signed values; validate request input and enforce access control on every request.</p>
</section>
