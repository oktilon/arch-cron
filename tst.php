<?php
    $ignore = true;
    require_once dirname(__FILE__) . '/cron.php';
    InfoPrefix(__FILE__);
    $time = time();
    $time_mark = $time;

    // colors
    $e  = "\033[0m";
    $g  = "\033[1;32m"; // green
    $y  = "\033[1;33m"; // yellow
    $cy = "\033[1;36m"; // cyan

    if(!Copy::pidLock('tst.loc')) die('lock' . PHP_EOL);

    for($i = 0; $i < 20; $i++) {
        echo "Sleep {$g}{$i}{$e}" . PHP_EOL;
        sleep(1);
    }

    Info("Finish within " . (time() - $time) . " sec.");

    Copy::pidUnLock();