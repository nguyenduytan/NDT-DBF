<section id="streaming" class="doc-section">
<h2>Streaming and chunks</h2>
<pre><code class="language-php">$stream = $db-&gt;table('users')-&gt;orderBy('id')-&gt;stream();
foreach ($stream as $user) {
    processUser($user);
}
unset($stream);

$db-&gt;table('users')-&gt;orderBy('id')-&gt;chunk(500, function (array $rows) {
    foreach ($rows as $row) {
        processUser($row);
    }
});

$db-&gt;table('users')-&gt;chunkById(500, function (array $rows) {
    foreach ($rows as $row) {
        processUser($row);
    }
});</code></pre>
<p><code>stream()</code> yields associative rows lazily. PDO buffering depends on the driver. The cursor closes when the generator finishes or is destroyed; release a retained generator after stopping early.</p>
<p><code>chunk()</code> uses offsets and requires a positive size and deterministic order. Changes to the result set during iteration may skip or repeat rows. <code>chunkById(int $size, callable $callback, string $key = 'id')</code> uses an ascending unique key and replaces existing order and offset. It supports deleting processed rows; do not change the pagination key while iterating. Return <code>false</code> from a chunk callback to stop.</p>
</section>
