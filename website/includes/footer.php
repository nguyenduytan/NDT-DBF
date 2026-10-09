<footer class="site-footer">
  <div class="wrap">
    <p><?= e($config['brand_name']) ?> v<?= e($config['version']) ?> &middot; <a href="<?= e($config['license_url']) ?>"><?= e($config['license']) ?> license</a></p>
    <p>By <a href="<?= e($config['author_website']) ?>"><?= e($config['author_name']) ?></a> &middot; <a href="<?= e($config['donate_url']) ?>">Support the project</a></p>
  </div>
</footer>
<?php if ($config['use_cdn']): ?>
<script src="https://cdn.jsdelivr.net/npm/prismjs@1.29.0/components/prism-core.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/prismjs@1.29.0/components/prism-clike.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/prismjs@1.29.0/components/prism-markup.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/prismjs@1.29.0/components/prism-markup-templating.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/prismjs@1.29.0/components/prism-php.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/prismjs@1.29.0/components/prism-bash.min.js"></script>
<?php endif; ?>
<script src="assets/js/docs.js" defer></script>
</body>
</html>
