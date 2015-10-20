<?php

namespace Assertis\SimpleDatabase;

/**
 * Class SimpleDatabaseExecuteException
 * @package Assertis\Util
 */
class SimpleDatabaseExecuteException extends SimpleDatabaseException
{

    const MESSAGE = "Could not execute an SQL query.";
    const CODE = 500;

    /**
     * @var string
     */
    private $sql;
    /**
     * @var array
     */
    private $params;
    /**
     * @var array
     */
    private $errorInfo;

    /**
     * @param array $errorInfo
     * @param string $sql
     * @param array $params
     */
    public function __construct($errorInfo, $sql, $params)
    {
        parent::__construct(self::MESSAGE, self::CODE);
        $this->errorInfo = $errorInfo;
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
     * @return array
     */
    public function getParams()
    {
        return $this->params;
    }

    /**
     * @return array
     */
    public function getErrorInfo()
    {
        return $this->errorInfo;
    }

}
