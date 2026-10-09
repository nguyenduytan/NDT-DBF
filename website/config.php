<?php
$repo = 'https://github.com/nguyenduytan/NDT-DBF';
$version = '0.3.0';
return [
    'brand_name' => 'NDT DBF',
    'version' => $version,
    'php_version' => '8.1',
    'license' => 'MIT',
    'site_tagline' => 'Single-file PHP database framework',
    'author_name' => 'Tony Nguyen',
    'author_website' => 'https://nguyenduytan.com',
    'github_repo' => $repo,
    'release_url' => $repo . '/releases/tag/v' . $version,
    'ci_url' => $repo . '/actions/workflows/ci.yml',
    'ci_badge' => $repo . '/actions/workflows/ci.yml/badge.svg?branch=main',
    'license_url' => $repo . '/blob/v' . $version . '/LICENSE.md',
    'docs_url' => 'docs.php',
    'download_raw' => 'https://raw.githubusercontent.com/nguyenduytan/NDT-DBF/v' . $version . '/src/DBF.php',
    'donate_url' => 'https://www.paypal.com/paypalme/copbeo',
    'maintenance' => false,
    'maintenance_message' => 'Maintenance is in progress. Please check back soon.',
    'use_cdn' => true,
];
