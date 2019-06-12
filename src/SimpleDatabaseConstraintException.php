<?php
declare(strict_types=1);

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

    public function __construct(string $message, string $sql, array $params)
    {
        parent::__construct($message);

        $this->sql = $sql;
        $this->params = $params;
    }

    public function getSql(): string
    {
        return $this->sql;
    }

    public function setSql(string $sql): void
    {
        $this->sql = $sql;
    }

    public function getParams(): array
    {
        return $this->params;
    }

    public function setParams(array $params): void
    {
        $this->params = $params;
    }

    public function getResolvedQuery(): string
    {
        return SimpleDatabase::resolveQuery($this->sql, $this->params);
    }
}
