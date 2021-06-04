<?php

declare(strict_types=1);

namespace Assertis\SimpleDatabase;

use PDO;
use PDOException;
use PDOStatement;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Test\PDOMock;

/**
 * @author Michał Tatarynowicz <michal@assertis.co.uk>
 */
class SimplePdoFactoryTest extends TestCase
{
    /**
     * @var PDO|MockObject
     */
    private $writePdo;
    /**
     * @var callable
     */
    private $writePdoFactory;
    /**
     * @var bool
     */
    private $writePdoConnected = true;
    /**
     * @var PDO|MockObject
     */
    private $readPdo;
    /**
     * @var callable
     */
    private $readPdoFactory;
    /**
     * @var bool
     */
    private $readPdoConnected = true;

    public function setUp(): void
    {
        parent::setUp();

        $this->writePdoFactory = function () {
            return $this->writePdo = $this->getMockPdo($this->writePdoConnected);
        };

        $this->readPdoFactory = function () {
            return $this->readPdo = $this->getMockPdo($this->readPdoConnected);
        };
    }

    private function getMockPdo(bool $connected): MockObject
    {
        $pdo = $this->createMock(PDOMock::class);

        if ($connected) {
            $statement = $this->createMock(PDOStatement::class);
            $statement->method('fetchAll')->willReturn([]);
            $pdo->method('query')->willReturn($statement);
        } else {
            $pdo->method('query')->willThrowException(new PDOException());
        }

        return $pdo;
    }

    public function testGetPdo()
    {
        $db = new SimplePdoFactory($this->writePdoFactory, $this->readPdoFactory);

        $writeReplica = $db->getWritePdo();
        $readReplica = $db->getReadPdo();

        static::assertNotSame($writeReplica, $readReplica);

        static::assertSame($writeReplica, $db->getPdo());
        static::assertSame($writeReplica, $db->getWritePdo());
        static::assertSame($readReplica, $db->getReadPdo());

        static::assertSame($readReplica, $db->getPdo('SELECT * FROM `users`;'));
        static::assertSame($readReplica, $db->getPdo('  select * from `users` '));
        static::assertSame(
            $readReplica,
            $db->getPdo(
                '
            SELECT 
                *
            FROM `users`
        '
            )
        );
        static::assertSame($writeReplica, $db->getPdo('INSERT INTO `users` VALUES (1,2)'));
        static::assertSame($writeReplica, $db->getPdo("UPDATE `users` SET `password`='admin1' WHERE `id`=1"));
        static::assertSame($writeReplica, $db->getPdo('DELETE FROM `users` WHERE `id`=1'));
        static::assertSame($writeReplica, $db->getPdo("REPLACE INTO `users` VALUES (1, 'admin', 'password')"));
        static::assertSame($writeReplica, $db->getPdo('SET foreign_key_checks=0'));
    }

    public function testReconnectWillFailIfNotDisconnected()
    {
        $this->writePdoConnected = true;

        $db = new SimplePdoFactory($this->writePdoFactory, $this->readPdoFactory);
        $writeDb = $db->getWritePdo();
        $readDb = $db->getReadPdo();

        self::assertFalse($db->reconnect($this->writePdo));
        self::assertSame($writeDb, $db->getWritePdo());
        self::assertSame($readDb, $db->getReadPdo());
    }

    public function testReconnectWillCreateNewPdoInstance()
    {
        $this->writePdoConnected = false;

        $db = new SimplePdoFactory($this->writePdoFactory, $this->readPdoFactory);
        $writePdo = $db->getWritePdo();
        $readPdo = $db->getReadPdo();

        self::assertTrue($db->reconnect($this->writePdo));
        self::assertNotSame($writePdo, $db->getWritePdo());
        self::assertSame($readPdo, $db->getReadPdo());
    }

    public function testLegacyNamesWork()
    {
        $db = new SimplePdoFactory($this->writePdoFactory, $this->readPdoFactory);
        $writePdo = $db->getWritePdo();
        $readPdo = $db->getReadPdo();

        self::assertSame($writePdo, $db->getMasterPdo());
        self::assertSame($readPdo, $db->getSlavePdo());
    }
}
