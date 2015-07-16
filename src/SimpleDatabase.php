<?php

namespace Assertis\SimpleDatabase;

use Exception;
use PDO;
use PDOStatement;
use Psr\Log\LoggerInterface;

/**
 * Class SimpleDb
 *
 * @package Assertis\Util
 *
 * @author  Michał Tatarynowicz <michal@assertis.co.uk>
 */
class SimpleDatabase {

    /**
     * @var PDO
     */
    private $pdo;
    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @param PDO             $pdo
     * @param LoggerInterface $logger
     */
    public function __construct(PDO $pdo, LoggerInterface $logger) {
        $this->pdo = $pdo;
        $this->logger = $logger;
    }

    /**
     * @param string $sql
     * @param array  $params
     *
     * @return string
     */
    public static function resolveQuery($sql, $params) {
        $keys = array_map(function ($key) {
            return ':' . $key;
        }, array_keys($params));
        $values = array_map(function ($value) {
            return "'{$value}'";
        }, array_values($params));

        return str_replace($keys, $values, $sql);
    }

    /**
     * @param string $sql
     * @param array  $params
     *
     * @return PDOStatement
     * @throws SimpleDatabaseExecuteException
     */
    public function executeQuery($sql, $params = []) {
        $query = $this->getPdo()->prepare($sql);
        $this->getLogger()->debug('Executing query ' . self::resolveQuery($sql, $params));

        if (!$query->execute($params)) {
            $errorInfo = $query->errorInfo();
            $this->getLogger()->error("Could not execute query {$sql} with parameters " . json_encode($params) . ": {$errorInfo['0']}/{$errorInfo['1']} - {$errorInfo[2]}.");
            throw new SimpleDatabaseExecuteException($errorInfo, $sql, $params);
        }

        return $query;
    }

    /**
     * @param string $sql
     * @param array  $params
     * @param int    $columnId
     * @param bool   $optional
     *
     * @return string
     * @throws NoRecordsFoundException
     * @throws SimpleDatabaseExecuteException
     */
    public function getColumn($sql, array $params = [], $columnId = 0, $optional = false) {
        $query = $this->executeQuery($sql, $params);

        if ($query->rowCount() < 1) {
            if ($optional) {
                return null;
            }
            else {
                throw new NoRecordsFoundException($sql, $params);
            }
        }

        return $query->fetchColumn($columnId);
    }

    /**
     * @param string $sql
     * @param array $params
     * @param bool $optional
     * @param int $fetchMode
     * @return array|null
     * @throws NoRecordsFoundException
     * @throws SimpleDatabaseExecuteException
     */
    public function getRow($sql, array $params = [], $optional = false, $fetchMode = PDO::FETCH_ASSOC) {
        $query = $this->executeQuery($sql, $params);

        if ($query->rowCount() < 1) {
            if ($optional) {
                return null;
            }
            else {
                throw new NoRecordsFoundException($sql, $params);
            }
        }

        return $query->fetch($fetchMode);
    }

    /**
     * @param string $sql
     * @param array $params
     * @param int $fetchMode
     * @return array[]
     * @throws SimpleDatabaseExecuteException
     */
    public function getAll($sql, array $params = [], $fetchMode = PDO::FETCH_ASSOC) {
        $query = $this->executeQuery($sql, $params);

        return $query->fetchAll($fetchMode);
    }

    /**
     * @param string $sql
     * @param array $params
     * @param int $columnId
     * @return array
     */
    public function getColumnFromAllRows($sql, array $params = [], $columnId = 0) {
        $out = [];
        foreach ($this->getAll($sql, $params, PDO::FETCH_NUM) as $row) {
            $out[] = $row[$columnId];
        }

        return $out;
    }

    /**
     * @return PDOStatement
     * @throws SimpleDatabaseExecuteException
     */
    public function startTransaction() {
        return $this->executeQuery("START TRANSACTION");
    }

    /**
     * @return PDOStatement
     * @throws SimpleDatabaseExecuteException
     */
    public function commitTransaction() {
        return $this->executeQuery("COMMIT");
    }

    /**
     * @return PDOStatement
     * @throws SimpleDatabaseExecuteException
     */
    public function rollbackTransaction() {
        return $this->executeQuery("ROLLBACK");
    }

    /**
     * @return string
     */
    public function getLastInsertId() {
        return $this->getPdo()->lastInsertId();
    }

    /**
     * @param callable $callback
     * @param array    $params
     *
     * @return mixed
     *
     * @throws Exception
     */
    public function transactional(callable $callback, array $params) {
        try {
            $this->startTransaction();
            $result = call_user_func_array($callback, $params);
            $this->commitTransaction();
        } catch (Exception $exception) {
            $this->rollbackTransaction();
            throw $exception;
        }

        return $result;
    }

    /**
     * @return PDO
     */
    protected function getPdo() {
        return $this->pdo;
    }

    /**
     * @return LoggerInterface
     */
    protected function getLogger() {
        return $this->logger;
    }

    /**
     * @param array $data
     * @return string
     */
    private function getInsertKeys(array $data) {
        return join(', ', array_keys($data));
    }

    /**
     * @param array $data
     * @return string
     */
    private function getInsertPlaceholders(array $data) {
        return join(', ', array_map(function($key){
            return ':'.$key;
        }, array_keys($data)));
    }

    /**
     * @param array $data
     * @return string
     */
    private function getInsertData(array $data) {
        return join(', ', array_map(function($item){
            return $this->getPdo()->quote($item);
        }, $data));
    }

    /**
     * @param string $table
     * @param array $data
     * @return string
     */
    public function insert($table, array $data) {
        $keys = $this->getInsertKeys($data);
        $placeholders = $this->getInsertPlaceholders($data);
        $sql = "INSERT INTO {$table} ({$keys}) VALUES ({$placeholders});";
        $this->executeQuery($sql, $data);

        return $this->getLastInsertId();
    }

    /**
     * @param string $table
     * @param array $data
     * @return bool
     * @throws SimpleDatabaseExecuteException
     */
    public function replace($table, array $data) {
        $keys = $this->getInsertKeys($data);
        $placeholders = $this->getInsertPlaceholders($data);
        $sql = "REPLACE INTO {$table} ({$keys}) VALUES ({$placeholders});";
        $this->executeQuery($sql, $data);

        return true;
    }

    public function replaceMultiple($table, $datasets) {
        $keys = $this->getInsertKeys($datasets[0]);

        $placeholders = [];
        foreach ($datasets as $dataset) {
            $placeholders[] = $this->getInsertData($dataset);
        }

        $placeholders = join('), (', $placeholders);

        //$placeholders = $this->getInsertPlaceholders($data);
        $sql = "REPLACE INTO {$table} ({$keys}) VALUES ({$placeholders});";
        $this->executeQuery($sql);

        return true;
    }

    /**
     * @param string $table
     * @return PDOStatement
     * @throws SimpleDatabaseExecuteException
     */
    public function truncateTable($table) {
        return $this->executeQuery("TRUNCATE {$table};");
    }

}
