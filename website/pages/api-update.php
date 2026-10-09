<div id="api-update" class="doc-section">
  <h2>update()</h2>
  <p>Modify rows matching the current WHERE.</p>
  <pre ><code class="language-php">$affected = $db->table('users')
  ->where('id','=', $id)
  ->update(['status'=>'vip']);</code></pre>

  <details class="spoiler"><summary>Notes</summary>
    <ul>
      <li>Readonly mode or policy guard can block updates.</li>
      <li>Values are parameterized; identifiers quoted.</li>
    </ul>
  </details>
</div>
<p>Returns the driver affected row count, which may count matched or changed rows. Without a WHERE condition the update can affect every row.</p>
