<section id="api-delete" class="doc-section">
<h2>Delete and restore</h2>
<p><code>delete(): int</code> returns affected rows. It performs a soft delete only when the feature is enabled and the configured column exists. Otherwise it physically deletes rows.</p>
<pre><code class="language-php">$affected = $db-&gt;table('users')-&gt;where('id', '=', $id)-&gt;delete();
$restored = $db-&gt;table('users')-&gt;where('id', '=', $id)-&gt;restore();
$removed = $db-&gt;table('users')-&gt;where('id', '=', $id)-&gt;forceDelete();</code></pre>
<p><code>restore()</code> requires soft-delete configuration and its column. <code>forceDelete()</code> physically deletes matching rows. Supply a WHERE condition: DBF does not automatically forbid deleting every row.</p>
</section>
