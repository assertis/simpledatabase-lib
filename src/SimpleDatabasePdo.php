<?php
declare(strict_types=1);

namespace Assertis\SimpleDatabase;

use PDO;

/**
 * @author Łukasz Nowak.
 */
class SimpleDatabasePdo extends PDO
{
    /**
     * @var string
     */
    private $dsn;
    /**
     * @var string
     */
    private $username;
    /**
     * @var string
     */
    private $password;
    /**
     * @var array
     */
    private $options;

    public function __construct(
        string $dsn,
        string $username,
        string $password,
        array $options
    ) {
        $this->dsn = $dsn;
        $this->username = $username;
        $this->password = $password;
        $this->options = $options;
        parent::__construct($this->dsn, $this->username, $this->password, $this->options);
    }

    /**
     * @return string
     */
    public function getDsn(): string
    {
        return $this->dsn;
    }

    /**
     * @return string
     */
    public function getUsername(): string
    {
        return $this->username;
    }

    /**
     * @return string
     */
    public function getPassword(): string
    {
        return $this->password;
    }

    /**
     * @return array
     */
    public function getOptions(): array
    {
        return $this->options;
    }
}