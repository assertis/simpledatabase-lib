<?php
declare(strict_types=1);

namespace Assertis\SimpleDatabase;

class SimpleDatabaseExecuteException extends SimpleDatabaseException
{
    private const MESSAGE = 'Could not execute an SQL query.';
    private const CODE = 500;

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

    public function __construct(array $errorInfo, string $sql, array $params)
    {
        parent::__construct(self::MESSAGE, self::CODE);

        $this->errorInfo = $errorInfo;
        $this->sql = $sql;
        $this->params = $params;
    }

    public function getSql(): string
    {
        return $this->sql;
    }

    public function getParams(): array
    {
        return $this->params;
    }

    public function getErrorInfo(): array
    {
        return $this->errorInfo;
    }
}
