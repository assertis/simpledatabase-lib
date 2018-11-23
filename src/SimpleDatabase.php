<?php

namespace Assertis\SimpleDatabase;

use Exception;
use PDO;
use PDOStatement;
use Psr\Log\LoggerInterface;

/**
 * @author  Michał Tatarynowicz <michal@assertis.co.uk>
 */
class SimpleDatabase
{

    /**
     * @var SimpleDatabasePdo
     */
    private $pdo;
    /**
     * @var SimpleDatabasePdo
     */
    private $readOnlyPdo;
    /**
     * @var LoggerInterface
     */
    private $logger;
    /**
     * @var LoggerInterface
     */
    private $queryLogger;

    /**
     * @param SimpleDatabasePdo $pdo
     * @param LoggerInterface $logger
     * @param bool $logQueries
     * @param SimpleDatabasePdo|null $readOnlyPDO
     *  If this parameter is passed, then read/write separation will be enabled. $pdo parameter will be the one
     *  responsible for write operations, and $readOnlyPDO will be connection responsible for reading data.
     */
    public function __construct($pdo, $logger, $logQueries = false, $readOnlyPDO = null)
    {
        $this->pdo = $pdo;
        $this->readOnlyPdo = $readOnlyPDO;
        $this->logger = $logger;

        if ($logQueries) {
            $this->queryLogger = $logger;
        }
    }

    /**
     * @param LoggerInterface $queryLogger
     */
    public function setQueryLogger(LoggerInterface $queryLogger)
    {
        $this->queryLogger = $queryLogger;
    }

    /**
     * @param string $sql
     * @param array $params
     *
     * @return string|array
     */
    public static function resolveQuery($sql, $params)
    {
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
     * @param array $params
     *
     * @return PDOStatement
     * @throws SimpleDatabaseExecuteException
     */
    public function executeQuery($sql, $params = []): PDOStatement
    {
        $query = $this->getPdoBasedOnQueryType($sql)->prepare($sql);

        if ($this->queryLogger) {
            $this->queryLogger->debug(sprintf(
                'Executing "%s" with params %s',
                $sql,
                json_encode($params)
            ));
            $this->queryLogger->info(sprintf(
                self::resolveQuery($sql, $params)
            ));
        }

        if (!$query->execute($params)) {
            $errorInfo = $query->errorInfo();
            $this->getLogger()->error("Could not execute query {$sql} with parameters " . json_encode($params) .
                ": {$errorInfo['0']}/{$errorInfo['1']} - {$errorInfo[2]}.");
            throw new SimpleDatabaseExecuteException($errorInfo, $sql, $params);
        }

        return $query;
    }

    /**
     * @param string $sql
     * @param array $params
     * @param int $columnId
     * @param bool $optional
     *
     * @return string
     * @throws NoRecordsFoundException
     * @throws SimpleDatabaseExecuteException
     */
    public function getColumn($sql, array $params = [], $columnId = 0, $optional = false): ?string
    {
        $query = $this->executeQuery($sql, $params);

        if ($query->rowCount() < 1) {
            if ($optional) {
                return null;
            } else {
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
     *
     * @return array|null
     * @throws NoRecordsFoundException
     * @throws SimpleDatabaseExecuteException
     */
    public function getRow($sql, array $params = [], $optional = false, $fetchMode = PDO::FETCH_ASSOC)
    {
        $query = $this->executeQuery($sql, $params);

        if ($query->rowCount() < 1) {
            if ($optional) {
                return null;
            } else {
                throw new NoRecordsFoundException($sql, $params);
            }
        }

        return $query->fetch($fetchMode);
    }

    /**
     * @param string $sql
     * @param array $params
     * @param int $fetchMode
     *
     * @return array[]
     * @throws SimpleDatabaseExecuteException
     */
    public function getAll($sql, array $params = [], $fetchMode = PDO::FETCH_ASSOC): array
    {
        $query = $this->executeQuery($sql, $params);

        return $query->fetchAll($fetchMode);
    }

    /**
     * @param string $sql
     * @param array $params
     * @param int $columnId
     *
     * @return array
     */
    public function getColumnFromAllRows($sql, array $params = [], $columnId = 0): array
    {
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
    public function startTransaction(): PDOStatement
    {
        return $this->executeQuery("START TRANSACTION");
    }

    /**
     * @return PDOStatement
     * @throws SimpleDatabaseExecuteException
     */
    public function commitTransaction(): PDOStatement
    {
        return $this->executeQuery("COMMIT");
    }

    /**
     * @return PDOStatement
     * @throws SimpleDatabaseExecuteException
     */
    public function rollbackTransaction(): PDOStatement
    {
        return $this->executeQuery("ROLLBACK");
    }

    /**
     * @return string
     */
    public function getLastInsertId(): string
    {
        return $this->getPdo()->lastInsertId();
    }

    /**
     * @param callable $callback
     * @param array $params
     *
     * @return mixed
     *
     * @throws Exception
     */
    public function transactional(callable $callback, array $params)
    {
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
     * @return bool
     */
    public function inTransaction(): bool
    {
        return $this->pdo->inTransaction();
    }

    /**
     * @return PDO
     */
    protected function getPdo(): PDO
    {
        return $this->pdo;
    }

    /**
     * @return null|PDO
     */
    protected function getReadPdo()
    {
        return $this->readOnlyPdo;
    }

    /**
     * @return PDO
     */
    protected function getWritePdo(): PDO
    {
        return $this->getPdo();
    }

    /**
     *  Return read or write PDO object based on sql query passed as parameter.
     *
     * @param string $sql
     * @return PDO
     * @throws SimpleDatabaseException
     * This method will work fine only if self::isReadWriteSeparationEnabled method will return true.
     * If this object do not have readOnlyPdo parameter provided - then it will throw an exception!
     */
    public function getPdoBasedOnQueryType(string $sql): PDO
    {
        // if we have read/write separation enabled and it is read query
        if ($this->readOnlyPdo !== null && strpos(strtoupper(trim($sql)), 'SELECT') === 0) {
            return $this->getReadPdo();
        } else {
            return $this->getWritePdo();
        }
    }

    /**
     * @return LoggerInterface
     */
    protected function getLogger(): LoggerInterface
    {
        return $this->logger;
    }

    /**
     * @param array $data
     *
     * @return string
     */
    private function getInsertKeys(array $data): string
    {
        return join(',', array_map(function ($identifier) {
            return '`' . str_replace('.', '`.`', $identifier) . '`';
        }, array_keys($data)));
    }

    /**
     * @param array $data
     *
     * @return string
     */
    private function getInsertPlaceholders(array $data): string
    {
        return join(',', array_map(function ($key) {
            return ':' . $key;
        }, array_keys($data)));
    }

    /**
     * @param array $data
     *
     * @return string
     */
    private function getInsertData(array $data): string
    {
        return join(',', array_map([$this, 'quote'], $data));
    }

    /**
     * @param mixed $item
     * @return string
     */
    public function quote($item): string
    {
        if (is_array($item)) {
            return $this->pdo->quote(join(',', $item));
        } elseif (is_null($item)) {
            return 'NULL';
        } else {
            return $this->pdo->quote($item);
        }
    }

    /**
     * @param string $table
     * @param array $data
     *
     * @return string
     */
    public function insert($table, array $data): string
    {
        $keys = $this->getInsertKeys($data);
        $placeholders = $this->getInsertPlaceholders($data);
        $sql = "INSERT INTO `{$table}` ({$keys}) VALUES ({$placeholders});";
        $this->executeQuery($sql, $data);

        return $this->getLastInsertId();
    }

    /**
     * @param string $table
     * @param array $entities
     *
     * @return bool
     * @throws SimpleDatabaseExecuteException
     */
    public function insertMultiple($table, array $entities): bool
    {
        $keys = $this->getInsertKeys($entities[0]);

        $entityValues = [];
        foreach ($entities as $entity) {
            $entityValues[] = $this->getInsertData($entity);
        }

        $values = join("), \n(", $entityValues);

        $sql = "INSERT INTO `{$table}` ({$keys}) VALUES ({$values});";
        $this->executeQuery($sql);

        return true;
    }

    /**
     * @param string $table
     * @param array $data
     *
     * @return bool
     * @throws SimpleDatabaseExecuteException
     */
    public function replace($table, array $data): bool
    {
        $keys = $this->getInsertKeys($data);
        $placeholders = $this->getInsertPlaceholders($data);
        $sql = "REPLACE INTO `{$table}` ({$keys}) VALUES ({$placeholders});";
        $this->executeQuery($sql, $data);

        return true;
    }

    /**
     * @param string $table
     * @param array $entities
     *
     * @return bool
     * @throws SimpleDatabaseExecuteException
     */
    public function replaceMultiple($table, array $entities): bool
    {
        $keys = $this->getInsertKeys($entities[0]);

        $entityValues = [];
        foreach ($entities as $entity) {
            $entityValues[] = $this->getInsertData($entity);
        }

        $values = join("), \n(", $entityValues);

        $sql = "REPLACE INTO `{$table}` ({$keys}) VALUES ({$values});";
        $this->executeQuery($sql);

        return true;
    }

    /**
     * @param string $table
     * @param array $entity
     * @return bool
     */
    public function delete($table, array $entity): bool
    {
        return $this->deleteMultiple($table, [$entity]);
    }

    /**
     * @param string $table
     * @param array $entities
     * @return bool
     * @throws SimpleDatabaseExecuteException
     */
    public function deleteMultiple($table, array $entities): bool
    {
        $keys = $this->getInsertKeys($entities[0]);

        $entityValues = [];
        foreach ($entities as $entity) {
            $entityValues[] = $this->getInsertData($entity);
        }

        $values = '(' . join("), \n(", $entityValues) . ')';

        $sql = "DELETE FROM `{$table}` WHERE ({$keys}) IN ({$values});";

        $this->executeQuery($sql);

        return true;
    }

    /**
     * @param string $table
     *
     * @return PDOStatement
     * @throws SimpleDatabaseExecuteException
     */
    public function truncateTable($table): PDOStatement
    {
        return $this->executeQuery("TRUNCATE `{$table}`;");
    }

    /**
     * @return PDOStatement
     */
    public function disableForeignKeyChecks()
    {
        $this->executeQuery("SET foreign_key_checks = 0;");
    }

    /**
     * @return PDOStatement
     */
    public function enableForeignKeyChecks()
    {
        $this->executeQuery("SET foreign_key_checks = 1;");
    }

    /**
     * @param string $tableName
     * @param string $newTableName
     * @param bool $withData
     *
     * @throws SimpleDatabaseExecuteException
     */
    public function duplicateTable($tableName, $newTableName, $withData = false)
    {
        $this->executeQuery("CREATE TABLE IF NOT EXISTS `{$newTableName}` LIKE `{$tableName}`;");
        if ($withData) {
            $tmpTableName = '_old_'.$newTableName;
            $this->renameTable($newTableName, $tmpTableName);
            $this->renameTable($tableName, $newTableName);
            $this->dropTable($tmpTableName);
        }
    }

    public function renameTable($currentName, $newName)
    {
        $this->executeQuery("RENAME TABLE `{$currentName}` TO `{$newName}`");
    }

    /**
     * @param string $prefix
     *
     * @return array[]
     * @throws SimpleDatabaseExecuteException
     */
    public function listTablesStartsWith($prefix)
    {
        return $this->getAll("SHOW TABLES LIKE '{$prefix}%';", [], PDO::FETCH_COLUMN);
    }

    /**
     * @param string $prefix
     * @return array[]
     * @throws SimpleDatabaseExecuteException
     */
    public function listTablesNotStartingWith($prefix)
    {
        return array_filter($this->listAllTables(), function ($tableName) use ($prefix) {
            return strpos($tableName, $prefix) !== 0;
        });
    }

    /**
     * @return array[]
     * @throws SimpleDatabaseExecuteException
     */
    public function listAllTables()
    {
        return $this->listTablesStartsWith('');
    }

    /**
     * @param string $tableName
     *
     * @throws SimpleDatabaseExecuteException
     */
    public function dropTable($tableName)
    {
        $this->executeQuery("DROP TABLE `{$tableName}`;");
    }

    /**
     * This method define if we are able to use separate read/write PDO object.
     * @return bool
     */
    public function isReadWriteSeparationEnabled()
    {
        return $this->readOnlyPdo !== null;
    }

    /**
     * @return bool
     */
    public function resetConnection(): bool
    {
        if ($this->pdo instanceof SimpleDatabasePdo) {
            $this->pdo = new PDO(
                $this->pdo->getDsn(),
                $this->pdo->getUsername(),
                $this->pdo->getPassword(),
                $this->pdo->getOptions()
            );
            if ($this->readOnlyPdo instanceof SimpleDatabasePdo) {
                $this->readOnlyPdo = new PDO(
                    $this->readOnlyPdo->getDsn(),
                    $this->readOnlyPdo->getUsername(),
                    $this->readOnlyPdo->getPassword(),
                    $this->readOnlyPdo->getOptions()
                );
            }
            return true;
        }
        return false;
    }
}
