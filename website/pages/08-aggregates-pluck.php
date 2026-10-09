<section id="aggregates" class="doc-section">
<h2>Aggregates and pluck</h2>
<pre><code class="language-php">$sum = $db-&gt;table('orders')-&gt;sum('total');
$avg = $db-&gt;table('orders')-&gt;avg('total');
$min = $db-&gt;table('orders')-&gt;min('total');
$max = $db-&gt;table('orders')-&gt;max('total');
$count = $db-&gt;table('users')-&gt;count();

$emails = $db-&gt;table('users')-&gt;pluck('email');
$map = $db-&gt;table('users')-&gt;pluck('email', 'id');</code></pre>
<p><code>count()</code> returns an integer. Other aggregates return driver values, which may be numeric strings or NULL for an empty set. <code>pluck()</code> returns a list, or a map when a key column is provided; duplicate keys overwrite earlier values. It uses a clone so the original selection stays intact.</p>
<p><code>count()</code> honors joins and grouping: a grouped query counts its result groups. Scalar helpers such as <code>sum()</code> and <code>avg()</code> throw when grouping produces multiple result rows. Select an aggregate alias and fetch the groups instead.</p>
<pre><code class="language-php">$totals = $db-&gt;table('orders')
    -&gt;select(['user_id', 'SUM(total) AS total_sum'])
    -&gt;groupBy(['user_id'])
    -&gt;get();</code></pre>
</section>
