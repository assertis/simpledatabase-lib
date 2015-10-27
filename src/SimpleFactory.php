<?php

namespace Assertis\SimpleDatabase;

/**
 * @author Michał Tatarynowicz <michal@assertis.co.uk>
 */
abstract class SimpleFactory
{

    /**
     * @var SimpleDatabase
     */
    private $db;

    /**
     * @param SimpleDatabase $db
     */
    public function __construct(SimpleDatabase $db)
    {
        $this->db = $db;
    }

    /**
     * @return SimpleDatabase
     */
    protected function getDb()
    {
        return $this->db;
    }

    /**
     * @param array $data
     * @return mixed
     */
    abstract protected function fromArray(array $data);

    /**
     * @param string $sql
     * @param array $parameters
     * @param bool $optional
     * @return mixed
     * @throws NoRecordsFoundException
     */
    protected function getByParameters($sql, array $parameters, $optional = false)
    {
        $data = $this->getDb()->getRow($sql, $parameters, $optional);
        
		// Can only happen if is optional
        if (null === $data) {
            return null;
        }

        return $this->fromArray($data);
    }

    /**
     * @param string $sql
     * @param array $parameters
     * @return mixed[]
     */
    protected function getAllByParameters($sql, array $parameters)
    {
        $data = $this->getDb()->getAll($sql, $parameters);

        return array_map([$this, 'fromArray'], $data);
    }

    /**
     * @param array $parameters
     * @return string
     */
    private function parametersToQuery(array $parameters)
    {
        if (empty($parameters)) {
            return '1=1';
        }

        $out = [];
        foreach ($parameters as $key => $value) {
            $out[] = "({$key} = :{$key})";
        }
        return join(' AND ', $out);
    }

    /**
     * @param string $table
     * @param array $parameters
     * @param string $order
     * @param int $page
     * @param int $limit
     * @return mixed[]
     */
    protected function getSearchResultsByParameters($table, array $parameters, $order, $page, $limit)
    {
        $offset = ($page - 1) * $limit;
        $query = $this->parametersToQuery($parameters);
        $sql = "SELECT * FROM `{$table}` WHERE {$query} ORDER BY {$order} LIMIT {$offset},{$limit};";
        
        return $this->getAllByParameters($sql, $parameters);
    }

    /**
     * @param string $table
     * @param array $parameters
     * @return int
     */
    public function getSearchResultsCountByParameters($table, array $parameters)
    {
        $query = $this->parametersToQuery($parameters);
        $sql = "SELECT COUNT(*) FROM `{$table}` WHERE {$query};";

        return (int)$this->getDb()->getColumn($sql, $parameters);
    }
}
