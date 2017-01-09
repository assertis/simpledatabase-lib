<?php

namespace Assertis\SimpleDatabase;

/**
 * @author Rafał Orłowski <rafal.orlowski@assertis.co.uk>
 */
class SimpleQueryUtils
{

    const QUERY_TYPE_SELECT = "SELECT";
    const QUERY_TYPE_INSERT = "INSERT";
    const QUERY_TYPE_UPDATE = "UPDATE";
    const QUERY_TYPE_DELETE = "DELETE";
    const QUERY_TYPE_REPLACE = "REPLACE";
    const QUERY_TYPE_OTHER = "OTHER";

    const QUERY_TYPE_READ = "READ";
    const QUERY_TYPE_WRITE = "WRITE";

    const QUERY_TYPES_DEFINED_AS_READ = [
        self::QUERY_TYPE_SELECT
    ];
    const QUERY_TYPES_DEFINED_AS_WRITE = [
        self::QUERY_TYPE_INSERT,
        self::QUERY_TYPE_UPDATE,
        self::QUERY_TYPE_REPLACE,
        self::QUERY_TYPE_DELETE,
        self::QUERY_TYPE_OTHER
    ];

    /**
     * @param string $query
     * @return string
     * @throws UnknownQueryTypeException
     */
    public static function defineQueryAsReadOrWrite(string $query): string
    {
        $queryType = self::getQueryType($query);
        if(in_array($queryType, self::QUERY_TYPES_DEFINED_AS_READ)){
            return self::QUERY_TYPE_READ;
        } elseif (in_array($queryType, self::QUERY_TYPES_DEFINED_AS_WRITE)){
            return self::QUERY_TYPE_WRITE;
        } else {
            throw new UnknownQueryTypeException();
        }
    }

    /**
     * @param string $query
     * @return string
     */
    public static function getQueryType(string $query): string
    {
        $firstWord = strtoupper(self::getFirstWord($query));

        switch ($firstWord){
            case self::QUERY_TYPE_SELECT:
                return self::QUERY_TYPE_SELECT;
            case self::QUERY_TYPE_INSERT:
                return self::QUERY_TYPE_INSERT;
            case self::QUERY_TYPE_REPLACE:
                return self::QUERY_TYPE_REPLACE;
            case self::QUERY_TYPE_DELETE:
                return self::QUERY_TYPE_DELETE;
            case self::QUERY_TYPE_UPDATE:
                return self::QUERY_TYPE_UPDATE;
            default:
                return self::QUERY_TYPE_OTHER;
        }
    }

    /**
     * @param string $query
     * @return string|null
     */
    public static function getFirstWord(string $query)
    {
        $splitByWhiteSpaces = preg_split("/\\s/", $query);
        $firstWord = reset($splitByWhiteSpaces);

        while(empty($firstWord) && next($splitByWhiteSpaces) !== FALSE){
            $firstWord = current($splitByWhiteSpaces);
        }
        return $firstWord;
    }

}