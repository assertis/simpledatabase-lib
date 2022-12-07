<?php

declare(strict_types=1);

namespace Assertis\SimpleDatabase;

use JsonException;

class SimpleDatabaseExecuteException extends SimpleDatabaseException
{
    private const MESSAGE = 'Could not execute query %s with parameters %s: %s/%s - %s';
    private const CODE = 500;

    private string $sql;
    private array $params;
    private array $errorInfo;

    /**
     * @throws JsonException
     */
    public function __construct(array $errorInfo, string $sql, array $params)
    {
        $message = sprintf(
            self::MESSAGE,
            $sql,
            json_encode($params, JSON_THROW_ON_ERROR),
            $errorInfo['0'],
            $errorInfo['1'],
            $errorInfo['2'] ?? '(blank)'
        );

        parent::__construct($message, self::CODE);

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
