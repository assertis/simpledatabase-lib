<?php

declare(strict_types=1);

namespace Tests\Assertis\SimpleDatabase;

use Assertis\SimpleDatabase\NoRecordsFoundException;
use Assertis\SimpleDatabase\SimpleDatabase;
use Assertis\SimpleDatabase\SimpleDatabaseConstraintException;
use Assertis\SimpleDatabase\SimpleDatabaseExecuteException;
use Assertis\SimpleDatabase\SimplePdoFactory;
use PDO;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

/**
 * @author Michał Tatarynowicz <michal@assertis.co.uk>
 */
class SimpleDatabaseTest extends TestCase
{
    /**
     * @var PDO|MockObject
     */
    private $pdo;
    /**
     * @var SimplePdoFactory|MockObject
     */
    private $pdoFactory;
    /**
     * @var LoggerInterface|MockObject
     */
    private $logger;
    /**
     * @var PDOStatementMock|MockObject
     */
    private $statement;

    public function setUp(): void
    {
        $this->pdo = $this->createMock(PDOMock::class);
        $this->pdoFactory = $this->createMock(SimplePdoFactory::class);
        $this->statement = $this->createMock(PDOStatementMock::class);
        $this->logger = $this->createMock(LoggerInterface::class);
    }

    /**
     * @throws SimpleDatabaseExecuteException
     */
    public function testExecuteQuery(): void
    {
        $sql = 'SQL';
        $params = ['foo' => 'bar'];

        $this->pdoFactory->expects(self::once())->method('getPdo')->with($sql)->willReturn($this->pdo);
        $this->pdo->expects(static::once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects(static::once())->method('execute')->with($params)->willReturn(true);

        $db = new SimpleDatabase($this->pdoFactory, $this->logger);

        static::assertSame($this->statement, $db->executeQuery($sql, $params));
    }

    public function testExecuteQueryThrowsExceptionOnExecuteError(): void
    {
        $sql = 'SQL';
        $params = ['foo' => 'bar'];

        $this->pdoFactory->expects(self::once())->method('getPdo')->with($sql)->willReturn($this->pdo);
        $this->pdo->expects(static::once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects(static::once())->method('execute')->with($params)->willReturn(false);
        $this->statement->expects(static::once())->method('errorInfo')->willReturn(['error', 1, 'error']);
        $this->logger->expects(static::once())->method('error');

        $this->expectException(SimpleDatabaseExecuteException::class);

        $db = new SimpleDatabase($this->pdoFactory, $this->logger);
        $db->executeQuery($sql, $params);
    }

    public function testExecThrowsExceptionOnError(): void
    {
        $sql = 'SELECT field FROM atable WHERE foo > :foo;';
        $effectiveSql = 'SELECT field FROM atable WHERE foo > \'bar\';';
        $params = ['foo' => 'bar'];

        $this->pdoFactory->expects(self::once())->method('getPdo')->with($sql)->willReturn($this->pdo);
        $this->pdo->expects(static::once())->method('exec')->with($effectiveSql)->willReturn(false);
        $this->pdo->expects(static::once())->method('errorInfo')->willReturn(['error', 1, 'error']);
        $this->logger->expects(static::once())->method('error');

        $this->expectException(SimpleDatabaseExecuteException::class);

        $db = new SimpleDatabase($this->pdoFactory, $this->logger);
        $db->exec($sql, $params);
    }

    /**
     * @throws NoRecordsFoundException
     * @throws SimpleDatabaseExecuteException
     */
    public function testGetColumn(): void
    {
        $sql = 'SQL';
        $params = ['foo' => 'bar'];
        $data = ['baz', 'boo'];

        $this->pdoFactory->expects(self::once())->method('getPdo')->with($sql)->willReturn($this->pdo);
        $this->pdo->expects(static::once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects(static::once())->method('execute')->with($params)->willReturn(true);
        $this->statement->expects(static::once())->method('rowCount')->willReturn(1);
        $this->statement->expects(static::once())->method('fetchColumn')->with(0)->willReturn($data[0]);

        $db = new SimpleDatabase($this->pdoFactory, $this->logger);

        static::assertSame($data[0], $db->getColumn($sql, $params));
    }

    /**
     * @throws NoRecordsFoundException
     * @throws SimpleDatabaseExecuteException
     */
    public function testGetColumnWithOptional(): void
    {
        $sql = 'SQL';
        $data = [null];

        $this->pdoFactory->expects(self::once())->method('getPdo')->with($sql)->willReturn($this->pdo);
        $this->pdo->expects(static::once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects(static::once())->method('execute')->willReturn(true);
        $this->statement->expects(static::once())->method('rowCount')->willReturn(1);
        $this->statement->expects(static::once())->method('fetchColumn')->with(0)->willReturn($data[0]);

        $db = new SimpleDatabase($this->pdoFactory, $this->logger);

        static::assertSame($data[0], $db->getColumn($sql, [], 0, true));
    }

    /**
     * @throws NoRecordsFoundException
     * @throws SimpleDatabaseExecuteException
     */
    public function testGetColumnThrowsExceptionOnNoResults(): void
    {
        $sql = 'SQL';
        $params = ['foo' => 'bar'];

        $this->pdoFactory->expects(self::once())->method('getPdo')->with($sql)->willReturn($this->pdo);
        $this->pdo->expects(static::once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects(static::once())->method('execute')->with($params)->willReturn(true);
        $this->statement->expects(static::once())->method('rowCount')->willReturn(0);

        $this->expectException(SimpleDatabaseConstraintException::class);

        $db = new SimpleDatabase($this->pdoFactory, $this->logger);
        $db->getColumn($sql, $params);
    }

    /**
     * @throws NoRecordsFoundException
     * @throws SimpleDatabaseExecuteException
     */
    public function testGetRow(): void
    {
        $sql = 'SQL';
        $params = ['foo' => 'bar'];
        $data = ['baz', 'boo'];

        $this->pdoFactory->expects(self::once())->method('getPdo')->with($sql)->willReturn($this->pdo);
        $this->pdo->expects(static::once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects(static::once())->method('execute')->with($params)->willReturn(true);
        $this->statement->expects(static::once())->method('rowCount')->willReturn(1);
        $this->statement->expects(static::once())->method('fetch')->willReturn($data);

        $db = new SimpleDatabase($this->pdoFactory, $this->logger);

        static::assertSame($data, $db->getRow($sql, $params));
    }

    /**
     * @throws NoRecordsFoundException
     * @throws SimpleDatabaseExecuteException
     */
    public function testGetRowThrowsExceptionOnNoResults(): void
    {
        $sql = 'SQL';
        $params = ['foo' => 'bar'];

        $this->pdoFactory->expects(self::once())->method('getPdo')->with($sql)->willReturn($this->pdo);
        $this->pdo->expects(static::once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects(static::once())->method('execute')->with($params)->willReturn(true);
        $this->statement->expects(static::once())->method('rowCount')->willReturn(0);

        $this->expectException(SimpleDatabaseConstraintException::class);

        $db = new SimpleDatabase($this->pdoFactory, $this->logger);
        $db->getRow($sql, $params);
    }

    /**
     * @throws SimpleDatabaseExecuteException
     */
    public function testGetAll(): void
    {
        $sql = 'SELECT * FROM `table`';
        $params = ['foo' => 'bar'];
        $data = [['baz', 'boo']];
        $fetchMode = 1234;

        $this->pdoFactory->expects(self::once())->method('getPdo')->with($sql)->willReturn($this->pdo);
        $this->pdo->expects(static::once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects(static::once())->method('execute')->with($params)->willReturn(true);
        $this->statement->expects(static::once())->method('fetchAll')->with($fetchMode)->willReturn($data);

        $db = new SimpleDatabase($this->pdoFactory, $this->logger);

        static::assertSame($data, $db->getAll($sql, $params, $fetchMode));
    }

    /**
     * @throws SimpleDatabaseExecuteException
     */
    public function testListTablesStartsWith(): void
    {
        $prefix = 'pref';
        $sql = "SHOW TABLES LIKE '$prefix%';";
        $tables = ['prefactor', 'predator'];

        $this->pdoFactory->expects(self::once())->method('getPdo')->with($sql)->willReturn($this->pdo);
        $this->pdo->expects(static::once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects(static::once())->method('execute')->willReturn(true);
        $this->statement->expects(static::once())->method('fetchAll')->with(PDO::FETCH_COLUMN)->willReturn($tables);

        $db = new SimpleDatabase($this->pdoFactory, $this->logger);

        static::assertSame($tables, $db->listTablesStartsWith($prefix));
    }

    /**
     * @throws SimpleDatabaseExecuteException
     */
    public function testListAllTables(): void
    {
        $sql = "SHOW TABLES LIKE '%';";
        $tables = ['prefactor', 'predator', 'test'];

        $this->pdoFactory->expects(self::once())->method('getPdo')->with($sql)->willReturn($this->pdo);
        $this->pdo->expects(static::once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects(static::once())->method('execute')->willReturn(true);
        $this->statement->expects(static::once())->method('fetchAll')->with(PDO::FETCH_COLUMN)->willReturn($tables);

        $db = new SimpleDatabase($this->pdoFactory, $this->logger);

        static::assertSame($tables, $db->listAllTables());
    }

    /**
     * @throws SimpleDatabaseExecuteException
     */
    public function testDuplicateTableWithData(): void
    {
        $create = 'CREATE TABLE IF NOT EXISTS `new_table` LIKE `table`;';
        $truncate = 'TRUNCATE `new_table`;';
        $insert = 'INSERT INTO `new_table` SELECT * FROM `table`;';

        $statements = [
            $this->createMock(PDOStatementMock::class),
            $this->createMock(PDOStatementMock::class),
            $this->createMock(PDOStatementMock::class),
        ];

        $this->pdoFactory->expects(static::exactly(3))
            ->method('getPdo')
            ->withConsecutive([$create], [$truncate], [$insert])
            ->willReturn($this->pdo);

        $this->pdo->expects(static::exactly(3))
            ->method('prepare')
            ->withConsecutive([$create], [$truncate], [$insert])
            ->willReturnOnConsecutiveCalls($statements[0], $statements[1], $statements[2]);

        $statements[0]->expects(static::once())->method('execute')->willReturn(true);
        $statements[1]->expects(static::once())->method('execute')->willReturn(true);
        $statements[2]->expects(static::once())->method('execute')->willReturn(true);

        $db = new SimpleDatabase($this->pdoFactory, $this->logger);
        $db->duplicateTable('table', 'new_table', true);
    }

    /**
     * @throws SimpleDatabaseExecuteException
     */
    public function testDuplicateTableWithoutData(): void
    {
        $sql = 'CREATE TABLE IF NOT EXISTS `new_table` LIKE `table`;';

        $this->pdoFactory->expects(self::once())->method('getPdo')->with($sql)->willReturn($this->pdo);
        $this->pdo->expects(static::once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects(static::once())->method('execute')->willReturn(true);

        $db = new SimpleDatabase($this->pdoFactory, $this->logger);
        $db->duplicateTable('table', 'new_table');
    }

    /**
     * @throws SimpleDatabaseExecuteException
     */
    public function testDropTable(): void
    {
        $sql = 'DROP TABLE `new_table`;';

        $this->pdoFactory->expects(self::once())->method('getPdo')->with($sql)->willReturn($this->pdo);
        $this->pdo->expects(static::once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects(static::once())->method('execute')->willReturn(true);

        $db = new SimpleDatabase($this->pdoFactory, $this->logger);
        $db->dropTable('new_table');
    }

    /**
     * @throws SimpleDatabaseExecuteException
     */
    public function testGetColumnFromAllRows(): void
    {
        $sql = 'SQL';
        $params = ['foo' => 'bar'];
        $data = [['baz', 'boo'], ['bing', 'bang']];
        $column = 1;

        $this->pdoFactory->expects(self::once())->method('getPdo')->with($sql)->willReturn($this->pdo);
        $this->pdo->expects(static::once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects(static::once())->method('execute')->with($params)->willReturn(true);
        $this->statement->expects(static::once())->method('fetchAll')->willReturn($data);

        $db = new SimpleDatabase($this->pdoFactory, $this->logger);

        static::assertSame(
            [$data[0][$column], $data[1][$column]],
            $db->getColumnFromAllRows($sql, $params, $column)
        );
    }

    public function testGetLastInsertId(): void
    {
        $id = '1234';

        $this->pdoFactory->expects(self::once())->method('getPdo')->with()->willReturn($this->pdo);
        $this->pdo->expects(static::once())->method('lastInsertId')->willReturn($id);

        $db = new SimpleDatabase($this->pdoFactory, $this->logger);

        static::assertSame($id, $db->getLastInsertId());
    }

    /**
     * @throws SimpleDatabaseExecuteException
     */
    public function testTransaction(): void
    {
        $this->pdoFactory->expects(static::exactly(3))->method('getPdo')->willReturn($this->pdo);
        $this->pdo->expects(static::exactly(3))->method('prepare')->willReturn($this->statement);
        $this->statement->expects(static::exactly(3))->method('execute')->willReturn(true);

        $db = new SimpleDatabase($this->pdoFactory, $this->logger);
        $db->startTransaction();
        $db->commitTransaction();
        $db->rollbackTransaction();
    }
}
