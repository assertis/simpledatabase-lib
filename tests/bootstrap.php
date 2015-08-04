<?php

namespace Test;

date_default_timezone_set('GMT');
error_reporting(E_ALL);

require_once __DIR__.'/../vendor/autoload.php';

class PDOMock extends \PDO {

    public function __construct() {}

}

class PDOStatementMock extends \PDOStatement {

    public function __construct() {}

}

class PDOLogger extends \PDO {

    private $queries = [];
    private $lastInsertId = 0;
    private $rowCount = 0;
    private $results = [];

    public function __construct() {}

    public function prepare($sql) {
        $statement = new PDOLoggerStatement();
        $statement->setPdo($this);
        $statement->setSql($sql);
        $statement->setRowCount($this->rowCount);
        $statement->setResults($this->results);
        return $statement;
    }

    /**
     * @param int $rowCount
     */
    public function setRowCount($rowCount) {
        $this->rowCount = $rowCount;
    }

    /**
     * @param array $results
     */
    public function setResults($results) {
        $this->results = $results;
    }

    public function addQuery($sql, $data) {
        return $this->queries[] = compact('sql', 'data');
    }

    public function resetQueries() {
        $this->queries = [];
        $this->lastInsertId = 0;
    }

    public function lastInsertId() {
        $this->lastInsertId++;
        return $this->lastInsertId;
    }

    /**
     * @param string $prefix
     * @return array
     */
    public function getQueries($prefix=null) {
        if (empty($prefix)) {
            return $this->queries;
        }

        $out = [];
        foreach ($this->queries as $query) {
            if (strpos($query['sql'], $prefix) === 0) {
                $out[] = $query;
            }
        }
        return $out;
    }

    /**
     * @param string $table
     * @return array
     */
    public function getInsertQueries($table) {
        return $this->getQueries("INSERT INTO `{$table}`");
    }

    /**
     * @param string $table
     * @param int $index
     * @return array
     */
    public function getInsertQuery($table, $index=0) {
        $queries = $this->getInsertQueries($table);
        return $queries[$index];
    }

    /**
     * @param string $table
     * @return array
     */
    public function getUpdateQueries($table) {
        return $this->getQueries("UPDATE `{$table}`");
    }

    /**
     * @param string $table
     * @param int $index
     * @return array
     */
    public function getUpdateQuery($table, $index=0) {
        $queries = $this->getUpdateQueries($table);
        return $queries[$index];
    }

    /**
     * @param string $table
     * @return array
     */
    public function getDeleteQueries($table) {
        return $this->getQueries("DELETE FROM `{$table}`");
    }

    /**
     * @param string $table
     * @param int $index
     * @return array
     */
    public function getDeleteQuery($table, $index=0) {
        $queries = $this->getDeleteQueries($table);
        return $queries[$index];
    }

    /**
     * @return array
     */
    public function getSelectQueries() {
        return $this->getQueries("SELECT");
    }

    /**
     * @param int $index
     * @return array
     */
    public function getSelectQuery($index=0) {
        $queries = $this->getSelectQueries();
        return $queries[$index];
    }

}

class PDOLoggerStatement extends \PDOStatement {

    private $pdo;
    private $sql;
    private $rowCount = 0;
    private $results = [];

    public function __construct() {}

    public function setPdo(PDOLogger $pdo) {
        $this->pdo = $pdo;
    }

    public function setSql($sql) {
        $this->sql = $sql;
    }

    public function execute($data) {
        $this->pdo->addQuery(trim($this->sql), $data);
        return true;
    }

    public function rowCount() {
        return $this->rowCount;
    }

    public function fetchColumn() {
        return $this->results[0][0];
    }

    public function fetch() {
        return $this->results[0];
    }

    public function fetchAll() {
        return $this->results;
    }

    /**
     * @param int $rowCount
     */
    public function setRowCount($rowCount) {
        $this->rowCount = $rowCount;
    }

    /**
     * @param array $results
     */
    public function setResults($results) {
        $this->results = $results;
    }

}
