<div id="api-get" class="doc-section">
  <h2>get() · first() · exists()</h2>
  <pre ><code class="language-php">$list   = $db->table('users')->limit(50)->get();
$first  = $db->table('users')->where('id','=',1)->first();
$exists = $db->table('users')->where('email','=','a@ndtan.net')->exists();</code></pre>
</div>
<p><code>get()</code> returns associative rows, <code>first()</code> returns a row or NULL, and <code>exists()</code> returns a boolean. <code>first()</code> uses a clone and does not replace the original limit.</p>
