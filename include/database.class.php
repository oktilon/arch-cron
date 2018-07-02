<?php
class Database {
    /** @var PDO */
    public $conn  = null;
    public $error = '';
    public $errno = 0;
    public $sql   = '';
    public $errInfo = array('','','');
    public $trace = array();
    public $binds = [];
    public $bTransaction = false;
    public $addr = '';
    public $db_name = '';
    public $db_drv = '';
    /** @var PDOStatement */
    public $lastStmt = null;

    /**
    * Конструктор подключения к БД
    *
    *
    * @param string $h Хост
    * @param string $u Login
    * @param string $p Password
    * @param string $db_name Database name
    * @param string $driver PDO driver name
    * @param string $charset Character set
    */
    function  __construct($h = NULL, $u = NULL, $p = NULL, $db = NULL, $driver = 'mysql', $charset = 'UTF8') {
        try {
            $opt = [];
            $srv = $h; // ? $h : SQL_SERVER;
            $usr = $u; // ? $u : SQL_USER;
            $pwd = $p; // ? $p : SQL_PWD;

            $db_name = $db; // ? $db : SQL_BASE;
            $this->db_drv = $driver;
            /*$opt = [
                PDO::MYSQL_ATTR_INIT_COMMAND => "SET NAMES 'UTF8'"
            ];*/

            $dsn = "$driver:host=$srv;dbname=$db_name";
            if($charset) $dsn .= ";charset=$charset";

            if($driver == 'sqlite') {
                $dsn = "$driver:$h";
                $usr = null;
            }

            $this->addr = $srv;
            $this->db_name = $db_name;
            $this->conn = $usr ? new PDO($dsn, $usr, $pwd, $opt) : new PDO($dsn);
        }
        catch(PDOException $ex) {
            $this->error = $ex->getMessage();
            $this->errno = $ex->getCode();
            $this->trace = $ex->getTraceAsString();
        }
    }

    public static function PostgreSql($h, $u = NULL, $p = NULL, $db = NULL) {
        $srv = $h; // ? $h : PG_SERVER;
        $usr = $u ? $u : PG_USER;
        $pwd = $p ? $p : PG_PWD;
        $db_name = $db ? $db : PG_BASE;
        // pgsql:host=localhost;dbname=testdb;
        return new Database($srv, $usr, $pwd, $db_name, 'pgsql', NULL);
    }

    /**
    * Check if connection is successful
    * @return boolean
    */
    public function valid() { return $this->conn != null; }

    public function serverAddr() { return $this->addr; }
    public function serverVersion() { return $this->valid() ? $this->conn->getAttribute(PDO::ATTR_SERVER_VERSION) : 'no-conn'; }


    private function fillStmtError() {
        $err = $this->lastStmt->errorCode();
        $this->error = $err != 0 ? "Error:$err" : '';
        $this->errInfo = $this->lastStmt->errorInfo();
        if(isset($this->errInfo[2])) {
            $this->error = $this->errInfo[2];
        }
    }

    private function fillError() {
        $err = $this->conn->errorCode();
        $this->error = $err != 0 ? "Error:$err" : '';
        $this->errInfo = $this->conn->errorInfo();
        if(isset($this->errInfo[2])) {
            $this->error = $this->errInfo[2];
        }
    }

    /**
    * MySQL Query
    *
    * @param string Query
    * @return PDOStatement
    */
    public function query($query) {
        $this->sql = $query;
        $this->lastStmt = $this->conn->query($query);
        if($this->lastStmt === FALSE) {
            $this->fillError();
            return false;
        }
        return $this->lastStmt;
    }

    /**
    * Last inserted row Id
    * @returns int
    */
    public function lastInsertId($name = null) {
        return intval($this->conn->lastInsertId($name));
    }

    /**
    * Экранирует в строке SQL-символы
    *
    * @param string Входная не экранированная строка
    * @returns string
    */
    public function escapeString($str) { //, $parameter_type = PDO::PARAM_STR) {
        if(ini_get('magic_quotes_gpc') != 1) {
            @$ret = addslashes($str);// , $parameter_type);
            if($ret === FALSE) $ret = '';
        } else {
            $ret = $str;
        }
        return $ret;
    }

    public function quoteParam($str, $parameter_type = PDO::PARAM_STR) {
        $ret = $str;
        if(ini_get('magic_quotes_gpc') != 1) {
            $ret = $this->conn->quote($str, $parameter_type);
            if($ret === FALSE) $ret = '';
        }
        return $ret;
    }

    /**
    * MySQL Query + read
    *
    * @param string Query
    * @returns array
    */
    public function select($query, $fetchType = PDO::FETCH_ASSOC) {
        $this->sql = $query;
        $this->lastStmt = $this->conn->query($query, $fetchType);
        $this->fillError();
        if ($this->lastStmt === FALSE) {
            return false;
        }
        $result = array();
        foreach($this->lastStmt as $rez) {
            $result[]=$rez;
        }
        $this->lastStmt = null;
        return $result;
    }

    public function select_row($query, $fetchType = PDO::FETCH_ASSOC) {
        $rows = $this->select($query, $fetchType);
        if($rows === FALSE) return FALSE;
        if(isset($rows[0])) return $rows[0];
        return [];
    }

    public function select_scalar($query) {
        $rows = $this->select($query, PDO::FETCH_NUM);
        if($rows === FALSE) return FALSE;
        if(isset($rows[0][0])) return $rows[0][0];
        return false;
    }

    public function select_dict($query, $key_type = PDO::PARAM_INT) {
        $ret = [];
        $this->sql = $query;
        $this->lastStmt = $this->conn->query($query, PDO::FETCH_NUM);
        if($this->lastStmt === FALSE) {
            $this->fillError();
            return false;
        }
        $is_arr = $this->lastStmt->columnCount() > 2;
        foreach($this->lastStmt as $row) {
            $key = array_shift($row);
            if($key_type == PDO::PARAM_INT) $key = intval($key);
            $val = $is_arr ? $row : $row[0];
            $ret[$key] = $val;
        }
        return $ret;
    }

    public static function uuid() {
        global $DB;
        return $DB->select_scalar("SELECT uuid()");
    }

    /**
    * Affected by last query rows count
    *
    * @returns int
    */
    public function affectedRows() {
        return $this->lastStmt ? $this->lastStmt->rowCount() : 0;
    }

    public function inTransaction() {
        return $this->bTransaction;
    }
    public function beginTransaction() {
        $this->bTransaction = $this->conn->beginTransaction();
        return $this->bTransaction;
    }
    public function commit() {
        $this->bTransaction = false;
        return $this->conn->commit();
    }
    public function rollBack() {
        $this->bTransaction = false;
        return $this->conn->rollBack();
    }

    /**
    * PDO prepare query
    *
    * @param string Query
    * @param array Driver options
    * @return PDOStatement
    */
    public function prepare($sql) {
        $this->lastStmt = $this->conn->prepare($sql);
        $this->fillError();
        $this->sql = $sql;
        $this->binds = [];
        return $this;
    }

    public function bindValue($par, $val, $type = PDO::PARAM_STR) {
        $this->lastStmt->bindValue($par, $val, $type);
        $this->binds[] = "$par=$val";
        return $this;
    }

    public function bind($par, $val) {
        $type = PDO::PARAM_STR;
        if(is_integer($val)) $type = PDO::PARAM_INT;
        $this->lastStmt->bindValue($par, $val, $type);
        $this->binds[] = "$par=$val";
        return $this;
    }

    public function execute() {
        $ret = $this->lastStmt->execute();
        $this->fillStmtError();
        return $ret;
    }

    public function execute_row($type = PDO::FETCH_ASSOC, $default = []) {
        $ok = $this->lastStmt->execute();
        $this->fillStmtError();
        if($ok) return $this->lastStmt->fetch($type);
        return $default;
    }

    public function execute_scalar($default = FALSE) {
        $ok = $this->lastStmt->execute();
        $this->fillStmtError();
        if($ok) return $this->fetchScalar($default);
        return $default;
    }

    public function execute_all($type = PDO::FETCH_ASSOC, $default = []) {
        $ok = $this->lastStmt->execute();
        $this->fillStmtError();
        if($ok) return $this->lastStmt->fetchAll($type);
        return $default;

    }

    public function fetchScalar($default = false) {
        $row = $this->lastStmt->fetch(PDO::FETCH_NUM);
        return $row ? $row[0] : $default;
    }

    public function fetchRow($type = PDO::FETCH_ASSOC) {
        return $this->lastStmt->fetch($type);
    }
}
