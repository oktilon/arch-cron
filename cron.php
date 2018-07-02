<?php
    $ds = DIRECTORY_SEPARATOR;
    define('PATH_CRON', dirname(__FILE__));
    define('PATH_BASE', PATH_CRON);

    $infoPrefix = '';

    function InfoPrefix($txt) {
        global $infoPrefix;
        $t = $txt;
        if(strpos($txt, '.') !== FALSE) {
            $t = basename($txt, '.php');
        }
        $infoPrefix = "{$t}: ";
    }

    function Info($txt) {
        global $infoPrefix;
        $out = $infoPrefix . $txt;
        syslog(LOG_WARNING, $out);
        echo $out . PHP_EOL;
    }

    //require_once PATH_BASE . $ds . 'error_handler.php';
    //set_error_handler('myErrorHandler');

    require_once PATH_BASE . $ds . 'config.php';
    require_once PATH_BASE . $ds . 'autoload.php';

    // Connect to bases
    $PGF = Database::PostgreSql(FAST);
    $PGA = Database::PostgreSql(ARCH);

    $err = [];
    if(!$PGF->valid()) $err[] = "Fast error : {$PGF->error}";
    if(!$PGA->valid()) $err[] = "Arch error : {$PGA->error}";

    if($err) {
        Info(implode("\n", $err));
        die();
    }