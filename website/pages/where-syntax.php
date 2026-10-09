<section id="where-syntax" class="doc-section">
  <h2>WHERE conditions</h2>
<p><code>where(string $column, string $operator, mixed $value)</code> takes three arguments. It does not accept an array or a grouping closure. Values are bound; column names and operators are validated.</p>
<h3>AND and OR</h3>
<pre><code class="language-php">$rows = $db-&gt;table('users')
    -&gt;where('status', '=', 'active')
    -&gt;where('email', 'LIKE', '%@ndtan.net')
    -&gt;get();

$rows = $db-&gt;table('users')
    -&gt;where('status', '=', 'active')
    -&gt;orWhere('status', '=', 'vip')
    -&gt;get();</code></pre>
<p>SQL operator precedence applies: AND binds more tightly than OR. Use <a href="#rawsql">bound raw SQL</a> for explicit nested predicate groups.</p>
<h3>IN, BETWEEN and NULL</h3>
<pre><code class="language-php">$users = $db-&gt;table('users')-&gt;whereIn('id', [1, 2, 3])-&gt;get();
$orders = $db-&gt;table('orders')-&gt;whereBetween('total', [100, 500])-&gt;get();
$missing = $db-&gt;table('users')-&gt;whereNull('deleted_at')-&gt;get();
$present = $db-&gt;table('users')-&gt;whereNull('email', not: true)-&gt;get();</code></pre>
<p><code>whereIn()</code> and <code>whereBetween()</code> accept <code>not: true</code> and <code>or: true</code>; <code>whereNull()</code> accepts the same flags. BETWEEN requires two values. An empty IN list matches no rows; an empty NOT IN list adds no restriction. Lists exceeding <code>features.max_in_params</code> throw <code>LengthException</code>.</p>
<p>Operators include <code>=</code>, <code>!=</code>, <code>&lt;&gt;</code>, <code>&lt;</code>, <code>&lt;=</code>, <code>&gt;</code>, <code>&gt;=</code>, <code>LIKE</code> and <code>NOT LIKE</code>. ILIKE and NOT ILIKE require PostgreSQL. Equality with <code>null</code> becomes IS NULL; inequality becomes IS NOT NULL.</p>
<p>Scope and soft-delete conditions are ANDed with the entire group of user predicates, so an OR condition cannot bypass those guards. Scope is a column-to-value map passed to <code>withScope()</code>, not an array syntax for <code>where()</code>.</p>
</section>
