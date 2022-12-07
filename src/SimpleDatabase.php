<?php

declare(strict_types=1);

namespace Assertis\SimpleDatabase;

use Exception;
use PDO;
use PDOException;
use PDOStatement;
use Psr\Log\LoggerInterface;

/**
 * @author  Michał Tatarynowicz <michal@assertis.co.uk>
 */
class SimpleDatabase
{
    /**
     * @var LoggerInterface
     */
    private $logger;
    /**
     * @var LoggerInterface|null
     */
    private $queryLogger;
    /**
     * @var SimplePdoFactory
     */
    private $simplePdo;

    public function __construct(
        SimplePdoFactory $simplePdo,
        LoggerInterface $logger,
        LoggerInterface $queryLogger = null
    ) {
        $this->simplePdo = $simplePdo;
        $this->logger = $logger;
        $this->queryLogger = $queryLogger;
    }

    public function setQueryLogger(LoggerInterface $queryLogger): void
    {
        $this->queryLogger = $queryLogger;
    }

    public static function resolveQuery(string $sql, array $params): string
    {
        $keys = array_map(
            static function ($key) {
                return ':' . $key;
            },
            array_keys($params)
        );
        $values = array_map(
            static function ($value) {
                return "'{$value}'";
            },
            array_values($params)
        );

        return str_replace($keys, $values, $sql);
    }

    /**
     * @param string $sql
     * @param array $params
     * @param int $retries
     * @return PDOStatement
     * @throws SimpleDatabaseExecuteException
     */
    public function executeQuery(string $sql, array $params = [], int $retries = 3): PDOStatement
    {
        $pdo = $this->simplePdo->getPdo($sql);
        $query = $pdo->prepare($sql);

        $this->logQuery($sql, $params);

        $errorInfo = null;

        try {
            if (!$query->execute($params)) {
                $errorInfo = $query->errorInfo();
            }
        } catch (PDOException $ex) {
            if ($retries > 0) {
                sleep(1);
                if ($this->simplePdo->reconnect($pdo)) {
                    return $this->executeQuery($sql, $params, $retries - 1);
                }
            }

            $errorInfo = $ex->errorInfo;
        }

        if ($errorInfo) {
            $this->logQueryError($sql, $params, $errorInfo);

            throw new SimpleDatabaseExecuteException($errorInfo, $sql, $params);
        }

        return $query;
    }

    /**
     * @param string $sql
     * @param array $params
     * @param int $retries
     * @throws SimpleDatabaseExecuteException
     */
    public function exec(string $sql, array $params = [], int $retries = 3): void
    {
        $pdo = $this->simplePdo->getPdo($sql);

        $this->logQuery($sql, $params);

        $errorInfo = null;

        try {
            if ($pdo->exec(self::resolveQuery($sql, $params)) === false) {
                $errorInfo = $pdo->errorInfo();
            }
        } catch (PDOException $ex) {
            if ($retries > 0) {
                sleep(1);
                if ($this->simplePdo->reconnect($pdo)) {
                    $this->exec($sql, $params, $retries - 1);

                    return;
                }
            }

            $errorInfo = $ex->errorInfo;
        }

        if (!$errorInfo) {
            return;
        }

        $this->logQueryError($sql, $params, $errorInfo);

        throw new SimpleDatabaseExecuteException($errorInfo, $sql, $params);
    }

    private function logQuery(string $sql, array $params): void
    {
        if (!isset($this->queryLogger)) {
            return;
        }

        $this->queryLogger->info(
            sprintf(
                self::resolveQuery($sql, $params)
            )
        );
    }

    private function logQueryError(string $sql, array $params, array $errorInfo): void
    {
        $message = sprintf(
            'Could not execute query %s with parameters %s: %s/%d - %s.',
            $sql,
            json_encode($params, JSON_THROW_ON_ERROR),
            $errorInfo[0],
            $errorInfo[1],
            $errorInfo[2] ?? '(blank)',
        );

        $this->getLogger()->error($message);
    }

    /**
     * @param string $sql
     * @param array $params
     * @param int $columnId
     * @param bool $optional
     *
     * @return string|int|null
     * @throws NoRecordsFoundException
     * @throws SimpleDatabaseExecuteException
     */
    public function getColumn(string $sql, array $params = [], int $columnId = 0, bool $optional = false)
    {
        $query = $this->executeQuery($sql, $params);

        if ($query->rowCount() < 1) {
            if ($optional) {
                return null;
            }

            throw new NoRecordsFoundException($sql, $params);
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
    public function getRow(
        string $sql,
        array $params = [],
        bool $optional = false,
        int $fetchMode = PDO::FETCH_ASSOC
    ): ?array {
        $query = $this->executeQuery($sql, $params);

        if ($query->rowCount() < 1) {
            if ($optional) {
                return null;
            }

            throw new NoRecordsFoundException($sql, $params);
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
     * @return array
     * @throws SimpleDatabaseExecuteException
     */
    public function getColumnFromAllRows(string $sql, array $params = [], int $columnId = 0): array
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
        return $this->executeQuery('START TRANSACTION');
    }

    /**
     * @return PDOStatement
     * @throws SimpleDatabaseExecuteException
     */
    public function commitTransaction(): PDOStatement
    {
        return $this->executeQuery('COMMIT');
    }

    /**
     * @return PDOStatement
     * @throws SimpleDatabaseExecuteException
     */
    public function rollbackTransaction(): PDOStatement
    {
        return $this->executeQuery('ROLLBACK');
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

    public function inTransaction(): bool
    {
        return $this->getPdo()->inTransaction();
    }

    protected function getPdo(): PDO
    {
        return $this->simplePdo->getPdo();
    }

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
        return implode(
            ',',
            array_map(
                static function ($identifier) {
                    return '`' . str_replace('.', '`.`', $identifier) . '`';
                },
                array_keys($data)
            )
        );
    }

    /**
     * @param array $data
     *
     * @return string
     */
    private function getInsertPlaceholders(array $data): string
    {
        return implode(
            ',',
            array_map(
                static function ($key) {
                    return ':' . $key;
                },
                array_keys($data)
            )
        );
    }

    /**
     * @param array $data
     *
     * @return string
     */
    private function getInsertData(array $data): string
    {
        return implode(',', array_map([$this, 'quote'], $data));
    }

    /**
     * @param mixed $item
     * @return string
     */
    public function quote($item): string
    {
        if (is_array($item)) {
            return $this->simplePdo->getPdo()->quote(implode(',', $item));
        } elseif ($item === null) {
            return 'NULL';
        } else {
            return $this->simplePdo->getPdo()->quote((string)$item);
        }
    }

    /**
     * @param string $table
     * @param array $data
     * @return string
     * @throws SimpleDatabaseExecuteException
     */
    public function insert(string $table, array $data): string
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
    public function insertMultiple(string $table, array $entities): bool
    {
        $keys = $this->getInsertKeys($entities[0]);

        $entityValues = [];
        foreach ($entities as $entity) {
            $entityValues[] = $this->getInsertData($entity);
        }

        $values = implode("), \n(", $entityValues);

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
    public function replace(string $table, array $data): bool
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
     * @return bool
     * @throws SimpleDatabaseExecuteException
     */
    public function replaceMultiple(string $table, array $entities): bool
    {
        $keys = $this->getInsertKeys($entities[0]);

        $entityValues = [];
        foreach ($entities as $entity) {
            $entityValues[] = $this->getInsertData($entity);
        }

        $values = implode("), \n(", $entityValues);

        $sql = "REPLACE INTO `{$table}` ({$keys}) VALUES ({$values});";
        $this->executeQuery($sql);

        return true;
    }

    /**
     * @param string $table
     * @param array $entity
     * @return bool
     * @throws SimpleDatabaseExecuteException
     */
    public function delete(string $table, array $entity): bool
    {
        return $this->deleteMultiple($table, [$entity]);
    }

    /**
     * @param string $table
     * @param array $entities
     * @return bool
     * @throws SimpleDatabaseExecuteException
     */
    public function deleteMultiple(string $table, array $entities): bool
    {
        $keys = $this->getInsertKeys($entities[0]);

        $entityValues = [];
        foreach ($entities as $entity) {
            $entityValues[] = $this->getInsertData($entity);
        }

        $values = '(' . implode("), \n(", $entityValues) . ')';

        $sql = "DELETE FROM `{$table}` WHERE ({$keys}) IN ({$values});";

        $this->executeQuery($sql);

        return true;
    }

    /**
     * @param string $table
     * @return PDOStatement
     * @throws SimpleDatabaseExecuteException
     */
    public function truncateTable(string $table): PDOStatement
    {
        return $this->executeQuery("TRUNCATE `{$table}`;");
    }

    /**
     * @throws SimpleDatabaseExecuteException
     */
    public function disableForeignKeyChecks(): void
    {
        $this->executeQuery('SET foreign_key_checks = 0;');
    }

    /**
     * @throws SimpleDatabaseExecuteException
     */
    public function enableForeignKeyChecks(): void
    {
        $this->executeQuery('SET foreign_key_checks = 1;');
    }

    /**
     * @param string $tableName
     * @param string $newTableName
     * @param bool $withData
     * @throws SimpleDatabaseExecuteException
     */
    public function duplicateTable(string $tableName, string $newTableName, bool $withData = false): void
    {
        $this->executeQuery("CREATE TABLE IF NOT EXISTS `{$newTableName}` LIKE `{$tableName}`;");
        if ($withData) {
            $this->truncateTable($newTableName);
            $this->executeQuery("INSERT INTO `{$newTableName}` SELECT * FROM `{$tableName}`;");
        }
    }

    /**
     * @param string $currentName
     * @param string $newName
     * @throws SimpleDatabaseExecuteException
     */
    public function renameTable(string $currentName, string $newName): void
    {
        $this->executeQuery("RENAME TABLE `{$currentName}` TO `{$newName}`");
    }

    /**
     * @param string $prefix
     * @return string[]
     * @throws SimpleDatabaseExecuteException
     */
    public function listTablesStartsWith(string $prefix): array
    {
        return $this->getAll("SHOW TABLES LIKE '{$prefix}%';", [], PDO::FETCH_COLUMN);
    }

    /**
     * @param string $prefix
     * @return array[]
     * @throws SimpleDatabaseExecuteException
     */
    public function listTablesNotStartingWith(string $prefix): array
    {
        return array_filter(
            $this->listAllTables(),
            static function ($tableName) use ($prefix) {
                return strpos($tableName, $prefix) !== 0;
            }
        );
    }

    /**
     * @return array[]
     * @throws SimpleDatabaseExecuteException
     */
    public function listAllTables(): array
    {
        return $this->listTablesStartsWith('');
    }

    /**
     * @param string $tableName
     *
     * @throws SimpleDatabaseExecuteException
     */
    public function dropTable(string $tableName): void
    {
        $this->executeQuery("DROP TABLE `{$tableName}`;");
    }
}
