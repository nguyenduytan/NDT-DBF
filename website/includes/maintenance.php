<?php
$config = $config ?? require __DIR__ . '/../config.php';
?>
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Maintenance | <?= htmlspecialchars($config['brand_name'], ENT_QUOTES, 'UTF-8') ?></title>
<link rel="icon" href="assets/favicon.ico">
<link rel="stylesheet" href="assets/css/docs.css">
</head>
<body>
<main class="maintenance">
  <img src="assets/brand/logo.png" width="302" height="72" alt="NDT DBF">
  <h1>We&rsquo;ll be back soon</h1>
  <p><?= htmlspecialchars($config['maintenance_message'], ENT_QUOTES, 'UTF-8') ?></p>
</main>
</body>
</html>
