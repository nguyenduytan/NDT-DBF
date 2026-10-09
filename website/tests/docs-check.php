<?php
$root = dirname(__DIR__);
function check(bool $condition, string $message): void {
    if (!$condition) throw new RuntimeException($message);
}
function render(array $arguments): string {
    $process = proc_open(array_merge([PHP_BINARY], $arguments), [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
    check(is_resource($process), 'Could not start PHP');
    $output = stream_get_contents($pipes[1]);
    $errors = stream_get_contents($pipes[2]);
    fclose($pipes[1]);
    fclose($pipes[2]);
    check(proc_close($process) === 0 && $errors === '', 'Render failed: ' . $errors);
    return $output;
}
$documents = [];
foreach (['index.php', 'docs.php'] as $page) {
    $html = render([$root . '/' . $page]);
    $dom = new DOMDocument();
    libxml_use_internal_errors(true);
    $dom->loadHTML($html);
    libxml_clear_errors();
    $xpath = new DOMXPath($dom);
    check($xpath->query('//h1')->length === 1, $page . ': expected one heading');
    $ids = [];
    foreach ($xpath->query('//*[@id]') as $node) {
        check(!isset($ids[$node->getAttribute('id')]), 'Duplicate anchor');
        $ids[$node->getAttribute('id')] = true;
    }
    foreach ($xpath->query('//pre/code[contains(@class, "language-php")]') as $node) {
        $snippet = $node->textContent;
        token_get_all(str_starts_with(ltrim($snippet), '<?php') ? $snippet : '<?php ' . $snippet, TOKEN_PARSE);
        check(!preg_match('/->where\s*\(\s*(\[|fn\b|function\b)/', $snippet), 'Unsupported WHERE example');
        check(!preg_match('/->groupBy\s*\(\s*[\'\"]/', $snippet), 'groupBy must take an array');
    }
    $documents[$page] = [$xpath, $ids];
}
foreach ($documents as $page => [$xpath, $ids]) {
    foreach ($xpath->query('//a[@href] | //img[@src] | //script[@src] | //link[@href]') as $node) {
        $url = $node->getAttribute($node->hasAttribute('href') ? 'href' : 'src');
        if (preg_match('~^https?://~', $url)) continue;
        check(!str_starts_with($url, '/'), 'Root-relative local route: ' . $url);
        [$path, $fragment] = array_pad(explode('#', $url, 2), 2, '');
        if ($path !== '') check(is_file($root . '/' . $path), 'Missing local path: ' . $path);
        if ($fragment !== '') check(isset($documents[$path ?: $page][1][$fragment]), 'Missing anchor: ' . $url);
    }
}
$config = require $root . '/config.php';
check($config['version'] === '0.3.1', 'Wrong release version');
check(str_contains($config['download_raw'], '/v' . $config['version'] . '/src/DBF.php'), 'Download is not pinned');
$maintenance = render(['-r', '$config = require ' . var_export($root . '/config.php', true) . '; $config["maintenance"] = true; include ' . var_export($root . '/includes/header.php', true) . '; echo "RENDER_MUST_STOP";']);
check(str_contains($maintenance, 'Maintenance') && !str_contains($maintenance, 'RENDER_MUST_STOP'), 'Maintenance did not stop rendering');
echo "PASS: page rendering, anchors, local assets, PHP examples, pinned release and maintenance exit\n";
