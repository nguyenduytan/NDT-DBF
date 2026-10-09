<div class="badges" aria-label="Project status">
  <a href="<?= e($config['release_url']) ?>"><img width="90" height="20" alt="Release v<?= e($config['version']) ?>" src="https://img.shields.io/badge/release-v<?= e($config['version']) ?>-2563eb"></a>
  <a href="<?= e($config['ci_url']) ?>"><img height="20" alt="CI status" src="<?= e($config['ci_badge']) ?>"></a>
  <a href="<?= e($config['docs_url']) ?>#requirements"><img width="80" height="20" alt="PHP <?= e($config['php_version']) ?> or newer" src="https://img.shields.io/badge/PHP-%3E%3D<?= e($config['php_version']) ?>-777bb4"></a>
  <a href="<?= e($config['license_url']) ?>"><img width="80" height="20" alt="<?= e($config['license']) ?> license" src="https://img.shields.io/badge/license-<?= e($config['license']) ?>-16845b"></a>
</div>
