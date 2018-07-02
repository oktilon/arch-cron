<?php
require_once dirname(dirname(__FILE__)) . '/cron.php';
InfoPrefix(__FILE__);

if($argc < 2) {
    echo "\nUsage: php {$argv[0]} \033[1;32mbase_table\033[0m" .
         "\n\033[1;32mbase_table\033[0m = \033[1;33mTable to init changes in\033[0m" .
         "\n";
    die();
}

$tbl = $argv[1];

Changes::initByTable($tbl, 'id', 1);