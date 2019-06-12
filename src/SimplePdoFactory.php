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
    private const MYSQL_ERROR_DISCONNECTED = 2006;

    /**
     * @var callable
     */
    private $masterPdoFactory;
    /**
     * @var PDO|null
     */
    private $masterPdo;
    /**
     * @var callable|null
     */
    private $slavePdoFactory;
    /**
     * @var PDO|null
     */
    private $slavePdo;

    public function __construct(callable $masterPdoFactory, ?callable $slavePdoFactory)
    {
        $this->masterPdoFactory = $masterPdoFactory;
        $this->slavePdoFactory = $slavePdoFactory;
    }

    public function getPdo(string $sql = null): PDO
    {
        $isSelect = $sql && stripos(trim($sql), 'SELECT') === 0;

        return $isSelect ? $this->getSlavePdo() : $this->getMasterPdo();
    }

    public function getMasterPdo(): PDO
    {
        if (!$this->masterPdo) {
            $this->masterPdo = ($this->masterPdoFactory)();
        }

        return $this->masterPdo;
    }

    public function getSlavePdo(): PDO
    {
        if (!$this->slavePdoFactory) {
            return $this->getMasterPdo();
        }

        if (!$this->slavePdo) {
            $this->slavePdo = ($this->slavePdoFactory)();
        }

        return $this->slavePdo;
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

        if ($this->masterPdo && $this->masterPdo === $pdo) {
            $this->masterPdo = ($this->masterPdoFactory)();
        }

        if ($this->slavePdo && $this->slavePdo === $pdo) {
            $this->slavePdo = ($this->slavePdoFactory)();
        }

        return true;
    }
}
