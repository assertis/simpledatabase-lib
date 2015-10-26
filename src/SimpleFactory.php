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

}
