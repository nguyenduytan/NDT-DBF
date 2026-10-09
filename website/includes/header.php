<?php
$config = $config ?? require __DIR__ . '/../config.php';
if (!empty($config['maintenance'])) {
    http_response_code(503);
    header('Retry-After: 3600');
    include __DIR__ . '/maintenance.php';
    exit;
}
$pageTitle = $pageTitle ?? $config['brand_name'];
function e(string $value): string {
    return htmlspecialchars($value, ENT_QUOTES, 'UTF-8');
}
?>
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title><?= e($pageTitle) ?> | <?= e($config['site_tagline']) ?></title>
<meta name="description" content="NDT DBF <?= e($config['version']) ?> documentation: installation, SQL queries, transactions, JSON and connection configuration.">
<meta name="author" content="<?= e($config['author_name']) ?>">
<meta name="theme-color" content="#fafbfc">
<link rel="icon" href="assets/favicon.ico">
<link rel="stylesheet" href="assets/css/docs.css">
<?php if ($config['use_cdn']): ?>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/prismjs@1.29.0/themes/prism.min.css">
<?php endif; ?>
</head>
<body>
<a class="skip-link" href="#main">Skip to content</a>
<header class="site-header">
  <div class="wrap">
    <a class="brand" href="index.php" aria-label="NDT DBF home"><img src="assets/brand/logo.png" width="603" height="143" alt="NDT DBF"></a>
    <a class="version" href="<?= e($config['release_url']) ?>">v<?= e($config['version']) ?></a>
    <nav class="nav" aria-label="Main navigation">
      <a href="<?= e($config['docs_url']) ?>"<?= basename($_SERVER['SCRIPT_NAME'] ?? '') === 'docs.php' ? ' aria-current="page"' : '' ?>>Docs</a>
      <a href="<?= e($config['github_repo']) ?>">GitHub</a>
      <a class="btn btn-primary" href="<?= e($config['download_raw']) ?>">Download</a>
    </nav>
  </div>
</header>
