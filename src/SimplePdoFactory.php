<?php

declare(strict_types=1);

namespace Assertis\SimpleDatabase;

use PDO;
use PDOException;

/**
 * @author  Michał Tatarynowicz <michal@assertis.co.uk>
 */
class SimplePdoFactory
{
    /**
     * @var callable
     */
    private $writePdoFactory;
    /**
     * @var PDO|null
     */
    private $writePdo;
    /**
     * @var callable
     */
    private $readPdoFactory;
    /**
     * @var PDO|null
     */
    private $readPdo;

    public function __construct(callable $writePdoFactory, ?callable $readPdoFactory)
    {
        $this->writePdoFactory = $writePdoFactory;
        $this->readPdoFactory = $readPdoFactory;
    }

    public function getPdo(string $sql = null): PDO
    {
        $isSelect = $sql && stripos(trim($sql), 'SELECT') === 0;

        return $isSelect ? $this->getReadPdo() : $this->getWritePdo();
    }

    public function getWritePdo(): PDO
    {
        if (!isset($this->writePdo)) {
            $this->writePdo = ($this->writePdoFactory)();
        }

        return $this->writePdo;
    }

    public function getReadPdo()
    {
        if (!isset($this->readPdoFactory)) {
            return $this->getWritePdo();
        }

        if (!isset($this->readPdo)) {
            $this->readPdo = ($this->readPdoFactory)();
        }

        return $this->readPdo;
    }

    public function isDisconnected(PDO $pdo): bool
    {
        try {
            $pdo->query('SELECT 1')->fetchAll();

            return false;
        } catch (PDOException $exception) {
            return true;
        }
    }

    public function reconnect(PDO $pdo): bool
    {
        if (!$this->isDisconnected($pdo)) {
            return false;
        }

        if ($this->writePdo && $this->writePdo === $pdo) {
            $this->writePdo = ($this->writePdoFactory)();
        }

        if ($this->readPdo && $this->readPdo === $pdo) {
            $this->readPdo = ($this->readPdoFactory)();
        }

        return true;
    }

    /**
     * @return PDO
     * @deprecated Please use SimplePdoFactory::getWritePdo
     */
    public function getMasterPdo(): PDO
    {
        return $this->getWritePdo();
    }

    /**
     * @return PDO
     * @deprecated Please use SimplePdoFactory::getReadPdo
     */
    public function getSlavePdo(): PDO
    {
        return $this->getReadPdo();
    }
}
