<?php

namespace Assertis\SimpleDatabase;

/**
 * @author Michał Tatarynowicz <michal@assertis.co.uk>
 */
class NoRecordsFoundException extends SimpleDatabaseConstraintException
{

    const MESSAGE = "No records were found using SQL: %s";

    /**
     * @param string $sql
     * @param array $params
     */
    public function __construct($sql, $params)
    {
        $message = sprintf(self::MESSAGE, SimpleDatabase::resolveQuery($sql, $params));
        parent::__construct($message, $sql, $params);
    }
}
