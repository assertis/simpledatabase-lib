<?php
declare(strict_types=1);

namespace Assertis\SimpleDatabase;

/**
 * @author Michał Tatarynowicz <michal@assertis.co.uk>
 */
class NoRecordsFoundException extends SimpleDatabaseConstraintException
{
    private const MESSAGE = 'No records were found using SQL: %s';

    /**
     * @param string $sql
     * @param array $params
     */
    public function __construct(string $sql, array $params)
    {
        $message = sprintf(self::MESSAGE, SimpleDatabase::resolveQuery($sql, $params));
        parent::__construct($message, $sql, $params);
    }
}
