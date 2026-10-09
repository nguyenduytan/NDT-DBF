<?php
$config = require __DIR__ . '/config.php';
$pageTitle = $config['brand_name'] . ' Documentation';
include __DIR__ . '/includes/header.php';
?>
<main id="main" class="docs-shell wrap">
  <aside class="docs-sidebar" aria-label="Documentation navigation">
    <details class="mobile-nav" open>
      <summary>Documentation contents</summary>
      <div class="sidebar-inner">
        <label for="docs-search">Search documentation</label>
        <input id="docs-search" type="search" placeholder="Search topics or code" autocomplete="off" aria-controls="search-results">
        <div id="search-results" role="status" aria-live="polite" hidden></div>
        <nav aria-label="Topics">
          <details class="nav-group" open>
            <summary>Getting started</summary>
            <a href="#intro">Introduction</a>
            <a href="#requirements">Requirements</a>
            <a href="#installation">Installation</a>
            <a href="#connection">Connections</a>
          </details>
          <details class="nav-group" open>
            <summary>Query builder</summary>
            <a href="#where-syntax">WHERE conditions</a>
            <a href="#api-select">Select and group</a>
            <a href="#api-join">Joins</a>
            <a href="#api-order-limit">Order and limit</a>
            <a href="#api-get">Get, first and exists</a>
          </details>
          <details class="nav-group" open>
            <summary>Write data</summary>
            <a href="#api-insert">Insert</a>
            <a href="#api-update">Update</a>
            <a href="#api-delete">Delete and restore</a>
            <a href="#api-upsert">Upsert</a>
            <a href="#softdelete">Soft delete</a>
          </details>
          <details class="nav-group" open>
            <summary>Read data</summary>
            <a href="#aggregates">Aggregates and pluck</a>
            <a href="#agg-count">Count</a>
            <a href="#agg-sum">Sum</a>
            <a href="#agg-avg">Average</a>
            <a href="#agg-min">Minimum</a>
            <a href="#agg-max">Maximum</a>
            <a href="#api-keyset">Keyset pagination</a>
            <a href="#streaming">Stream and chunk</a>
          </details>
          <details class="nav-group" open>
            <summary>Advanced</summary>
            <a href="#api-tx">Transactions</a>
            <a href="#rawsql">Raw SQL</a>
            <a href="#json">JSON</a>
            <a href="#api-policy">Policies and scopes</a>
            <a href="#api-middleware">Middleware and logging</a>
            <a href="#security">Security</a>
            <a href="#configref">Configuration</a>
          </details>
        </nav>
      </div>
    </details>
  </aside>
  <article class="docs-content">
    <div class="docs-heading">
      <h1>Documentation</h1>
      <p>NDT DBF v<?= e($config['version']) ?></p>
      <?php include __DIR__ . '/includes/badges.php'; ?>
    </div>
    <?php include __DIR__ . '/pages/01-intro.php'; ?>
    <?php include __DIR__ . '/pages/02-requirements-installation.php'; ?>
    <?php include __DIR__ . '/pages/03-connection.php'; ?>
    <?php include __DIR__ . '/pages/where-syntax.php'; ?>
    <?php include __DIR__ . '/pages/api-select.php'; ?>
    <?php include __DIR__ . '/pages/api-join.php'; ?>
    <?php include __DIR__ . '/pages/api-order-limit.php'; ?>
    <?php include __DIR__ . '/pages/api-get.php'; ?>
    <?php include __DIR__ . '/pages/api-insert.php'; ?>
    <?php include __DIR__ . '/pages/api-update.php'; ?>
    <?php include __DIR__ . '/pages/api-delete.php'; ?>
    <?php include __DIR__ . '/pages/api-upsert.php'; ?>
    <?php include __DIR__ . '/pages/11-softdelete.php'; ?>
    <?php include __DIR__ . '/pages/08-aggregates-pluck.php'; ?>
    <?php include __DIR__ . '/pages/agg-count.php'; ?>
    <?php include __DIR__ . '/pages/agg-sum.php'; ?>
    <?php include __DIR__ . '/pages/agg-avg.php'; ?>
    <?php include __DIR__ . '/pages/agg-min.php'; ?>
    <?php include __DIR__ . '/pages/agg-max.php'; ?>
    <?php include __DIR__ . '/pages/api-keyset.php'; ?>
    <?php include __DIR__ . '/pages/api-stream.php'; ?>
    <?php include __DIR__ . '/pages/api-tx.php'; ?>
    <?php include __DIR__ . '/pages/10-rawsql.php'; ?>
    <?php include __DIR__ . '/pages/api-json.php'; ?>
    <?php include __DIR__ . '/pages/api-policy.php'; ?>
    <?php include __DIR__ . '/pages/api-middleware.php'; ?>
    <?php include __DIR__ . '/pages/14-security.php'; ?>
    <?php include __DIR__ . '/pages/15-config-reference.php'; ?>
  </article>
</main>
<?php include __DIR__ . '/includes/footer.php'; ?>
