<?php
declare(strict_types=1);

namespace Assertis\SimpleDatabase;

use PDO;
use PHPUnit_Framework_MockObject_MockObject;
use PHPUnit_Framework_TestCase;
use Test\PDOMock;

/**
 * @author Michał Tatarynowicz <michal@assertis.co.uk>
 */
class SimplePdoFactoryTest extends PHPUnit_Framework_TestCase
{
    /**
     * @var PDO|PHPUnit_Framework_MockObject_MockObject
     */
    private $masterPdo;
    /**
     * @var callable
     */
    private $masterPdoFactory;
    /**
     * @var PDO|PHPUnit_Framework_MockObject_MockObject
     */
    private $slavePdo;
    /**
     * @var callable
     */
    private $slavePdoFactory;

    public function setUp()
    {
        $this->masterPdoFactory = function () {
            return $this->masterPdo = $this->createMock(PDOMock::class);
        };

        $this->slavePdoFactory = function () {
            return $this->slavePdo = $this->createMock(PDOMock::class);
        };
    }

    public function testGetPdo()
    {
        $db = new SimplePdoFactory($this->masterPdoFactory, $this->slavePdoFactory);

        $master = $db->getMasterPdo();
        $slave = $db->getSlavePdo();

        static::assertNotSame($master, $slave);

        static::assertSame($master, $db->getPdo());
        static::assertSame($master, $db->getMasterPdo());
        static::assertSame($slave, $db->getSlavePdo());

        static::assertSame($slave, $db->getPdo('SELECT * FROM `users`;'));
        static::assertSame($slave, $db->getPdo('  select * from `users` '));
        static::assertSame($slave, $db->getPdo('
            SELECT 
                *
            FROM `users`
        '));
        static::assertSame($master, $db->getPdo('INSERT INTO `users` VALUES (1,2)'));
        static::assertSame($master, $db->getPdo("UPDATE `users` SET `password`='admin1' WHERE `id`=1"));
        static::assertSame($master, $db->getPdo('DELETE FROM `users` WHERE `id`=1'));
        static::assertSame($master, $db->getPdo("REPLACE INTO `users` VALUES (1, 'admin', 'password')"));
        static::assertSame($master, $db->getPdo('SET foreign_key_checks=0'));
    }

    public function testReconnectWillFailIfNotDisconnected()
    {
        $db = new SimplePdoFactory($this->masterPdoFactory, $this->slavePdoFactory);
        $db->getMasterPdo();
        $db->getSlavePdo();
        self::assertFalse($db->reconnect($this->masterPdo));
    }

    public function testReconnectWillFailIfDifferentError()
    {
        $db = new SimplePdoFactory($this->masterPdoFactory, $this->slavePdoFactory);
        $db->getMasterPdo();
        $db->getSlavePdo();

        $this->masterPdo->method('errorCode')->willReturn(1234);

        self::assertFalse($db->reconnect($this->masterPdo));
    }

    public function testReconnectWillCreateNewPdoInstance()
    {
        $db = new SimplePdoFactory($this->masterPdoFactory, $this->slavePdoFactory);
        $master = $db->getMasterPdo();
        $slave = $db->getSlavePdo();

        $this->masterPdo->method('errorCode')->willReturn(2006);

        self::assertTrue($db->reconnect($this->masterPdo));
        self::assertNotSame($master, $db->getMasterPdo());
        self::assertSame($slave, $db->getSlavePdo());
    }
}
