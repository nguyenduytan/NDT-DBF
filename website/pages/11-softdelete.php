<section id="softdelete" class="doc-section">
<h2>Soft delete</h2>
<p>Enable <code>features.soft_delete</code> and create the configured column in your schema. The default column is <code>deleted_at</code>. Without that column, <code>delete()</code> performs a physical delete.</p>
<pre><code class="language-php">$db = new ndtan\DBF([
    'type' =&gt; 'sqlite',
    'database' =&gt; 'app.sqlite',
    'features' =&gt; [
        'soft_delete' =&gt; [
            'enabled' =&gt; true,
            'column' =&gt; 'deleted_at',
            'mode' =&gt; 'timestamp',
        ],
    ],
]);

$active = $db-&gt;table('users')-&gt;get();
$all = $db-&gt;table('users')-&gt;withTrashed()-&gt;get();
$deleted = $db-&gt;table('users')-&gt;onlyTrashed()-&gt;get();</code></pre>
<p>Timestamp mode uses NULL for active rows and a timestamp for deleted rows. Flag mode uses 0 for active rows and <code>deleted_value</code> (default 1) for deleted rows. Use <code>restore()</code> to restore rows and <code>forceDelete()</code> to physically remove them.</p>
<p>Automatic filtering applies to builder reads. Raw SQL must include its own soft-delete conditions. With joins, do not assume related tables receive their own soft-delete filters.</p>
</section>
