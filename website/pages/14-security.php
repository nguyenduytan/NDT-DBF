<section id="security" class="doc-section">
<h2>Security</h2>
<ul>
<li>Values are bound to prepared statements. Parameters cannot represent identifiers: allowlist user-selectable tables, columns and sort directions.</li>
<li>Use trusted SQL with raw methods. Builder scopes and soft-delete filters are not added to raw queries.</li>
<li><code>setReadonly(true)</code> blocks builder writes, <code>raw()</code>, <code>execute()</code> and transactions. <code>selectRaw()</code> is an explicit read API; database privileges provide the final protection.</li>
<li>Set WHERE conditions before updating or deleting. Unfiltered writes can affect every row.</li>
<li>Keep database credentials and sensitive query parameters out of public logs.</li>
<li>Apply authorization on every request. Encoded pagination cursors and tenant scopes are not complete access-control systems.</li>
</ul>
<pre><code class="language-php">$db-&gt;setReadonly(true);
$rows = $db-&gt;table('users')-&gt;select(['id', 'email'])-&gt;get();</code></pre>
</section>
