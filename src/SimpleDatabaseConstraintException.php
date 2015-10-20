<?php

namespace Assertis\SimpleDatabase;

/**
 * @author Michał Tatarynowicz <michal@assertis.co.uk>
 */
class SimpleDatabaseConstraintException extends SimpleDatabaseException
{

    /**
     * @var string
     */
    private $sql;

    /**
     * @var array
     */
    private $params;

    /**
     * @param string $message
     * @param string $sql
     * @param array $params
     */
    public function __construct($message, $sql, $params)
    {
        parent::__construct($message, 0, null); // TODO: Change the auto-generated stub
        $this->sql = $sql;
        $this->params = $params;
    }

    /**
     * @return string
     */
    public function getSql()
    {
        return $this->sql;
    }

    /**
     * @param string $sql
     */
    public function setSql($sql)
    {
        $this->sql = $sql;
    }

    /**
     * @return array
     */
    public function getParams()
    {
        return $this->params;
    }

    /**
     * @param array $params
     */
    public function setParams($params)
    {
        $this->params = $params;
    }

    /**
     * @return string
     */
    public function getResolvedQuery()
    {
        return SimpleDatabase::resolveQuery($this->sql, $this->params);
    }

}
