<?php

namespace Test;
use Assertis\SimpleDatabase\SimpleQueryUtils;
use Assertis\SimpleDatabase\UnknownQueryTypeException;

/**
 * @author Rafał Orłowski <rafal.orlowski@assertis.co.uk>
 */
class SimpleQueryUtilsTest extends \PHPUnit_Framework_TestCase
{

    public function testGetQueryType()
    {
        $insert = "INSERT INTO `users` VALUES (1, 'admin', 'password')";
        $update = "UPDATE `users` SET `password`='admin1' WHERE `id`=1";
        $delete = "DELETE FROM `users` WHERE `id`=1";
        $select = "SELECT * FROM `users`";
        $replace = "REPLACE INTO `users` VALUES (1, 'admin', 'password')";
        $other1 = "START TRANSACTION";
        $other2 = "SET foreign_key_checks=0";
        $multiLineQuery = "
            SELECT 
                *
            FROM `users`
        ";
        $lowercase = "select * from `users`";

        $this->assertEquals("SELECT",SimpleQueryUtils::getQueryType($select));
        $this->assertEquals("INSERT",SimpleQueryUtils::getQueryType($insert));
        $this->assertEquals("UPDATE",SimpleQueryUtils::getQueryType($update));
        $this->assertEquals("DELETE",SimpleQueryUtils::getQueryType($delete));
        $this->assertEquals("REPLACE",SimpleQueryUtils::getQueryType($replace));
        $this->assertEquals("OTHER",SimpleQueryUtils::getQueryType($other1));
        $this->assertEquals("OTHER",SimpleQueryUtils::getQueryType($other2));
        $this->assertEquals("SELECT",SimpleQueryUtils::getQueryType($multiLineQuery));
        $this->assertEquals("SELECT",SimpleQueryUtils::getQueryType($lowercase));
    }

    public function testGetFirstWord()
    {
        $this->assertEquals("AAA", SimpleQueryUtils::getFirstWord("AAA BBB CCC "));
        $this->assertEquals("AAA", SimpleQueryUtils::getFirstWord("         AAA BBB CCC "));
        $this->assertEquals("AAA", SimpleQueryUtils::getFirstWord("
                 AAA 
                 BBB 
                 CCC 
        "));

        $this->assertEquals(null, SimpleQueryUtils::getFirstWord("
                  
        "));
    }

    public function testDefineQueryAsReadOrWrite()
    {
        $insert = "INSERT INTO `users` VALUES (1, 'admin', 'password')";
        $update = "UPDATE `users` SET `password`='admin1' WHERE `id`=1";
        $delete = "DELETE FROM `users` WHERE `id`=1";
        $select = "SELECT * FROM `users`";
        $replace = "REPLACE INTO `users` VALUES (1, 'admin', 'password')";
        $other1 = "START TRANSACTION";
        $other2 = "SET foreign_key_checks=0";

        $this->assertEquals("READ", SimpleQueryUtils::defineQueryAsReadOrWrite($select));
        $this->assertEquals("WRITE", SimpleQueryUtils::defineQueryAsReadOrWrite($insert));
        $this->assertEquals("WRITE", SimpleQueryUtils::defineQueryAsReadOrWrite($update));
        $this->assertEquals("WRITE", SimpleQueryUtils::defineQueryAsReadOrWrite($delete));
        $this->assertEquals("WRITE", SimpleQueryUtils::defineQueryAsReadOrWrite($replace));
        $this->assertEquals("WRITE", SimpleQueryUtils::defineQueryAsReadOrWrite($other1));
        $this->assertEquals("WRITE", SimpleQueryUtils::defineQueryAsReadOrWrite($other2));
    }

}