<?php

namespace Assertis\SimpleDatabase;

use PDO;
use PHPUnit_Framework_MockObject_MockObject;
use PHPUnit_Framework_TestCase;
use Psr\Log\LoggerInterface;
use Test\PDOMock;
use Test\PDOStatementMock;

/**
 * @author Michał Tatarynowicz <michal@assertis.co.uk>
 */
class SimpleDatabaseTest extends PHPUnit_Framework_TestCase
{

    /**
     * @var PDO|PHPUnit_Framework_MockObject_MockObject
     */
    private $pdo;
    /**
     * @var LoggerInterface|PHPUnit_Framework_MockObject_MockObject
     */
    private $logger;
    /**
     * @var PDOStatementMock|PHPUnit_Framework_MockObject_MockObject
     */
    private $statement;

    public function setUp()
    {
        $this->pdo = $this->createMock(PDOMock::class);
        $this->statement = $this->createMock(PDOStatementMock::class);
        $this->logger = $this->createMock(LoggerInterface::class);
    }

    public function testExecuteQuery()
    {
        $sql = "SQL";
        $params = ['foo' => 'bar'];

        $this->pdo->expects($this->once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects($this->once())->method('execute')->with($params)->willReturn(true);

        $db = new SimpleDatabase($this->pdo, $this->logger);

        $this->assertSame($this->statement, $db->executeQuery($sql, $params));
    }

    /**
     * @expectedException \Assertis\SimpleDatabase\SimpleDatabaseExecuteException
     */
    public function testExecuteQueryThrowsExceptionOnExecuteError()
    {
        $sql = "SQL";
        $params = ['foo' => 'bar'];

        $this->pdo->expects($this->once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects($this->once())->method('execute')->with($params)->willReturn(false);
        $this->logger->expects($this->once())->method('error');

        $db = new SimpleDatabase($this->pdo, $this->logger);
        $db->executeQuery($sql, $params);
    }

    public function testExecuteQueryWithReadPdoUsage()
    {
        $sql = "SELECT * FROM `table`";
        $this->statement->expects($this->once())->method('execute')->willReturn(true);

        $readPDO = $this->createMock(PDOMock::class);
        $readPDO->expects($this->once())->method('prepare')->willReturn($this->statement);

        $this->pdo->expects($this->exactly(0))->method('prepare')->with($sql)->willReturn($this->statement);
        $db = new SimpleDatabase($this->pdo, $this->logger, false, $readPDO);
        $db->executeQuery($sql);
    }

    public function testExecuteQueryWithReadWriteModeEnabled()
    {
        $sql = "INSERT INTO `table` VALUES (1,2,3)";
        $this->statement->expects($this->once())->method('execute')->willReturn(true);

        $readPDO = $this->createMock(PDOMock::class);
        $readPDO->expects($this->exactly(0))->method('prepare')->willReturn($this->statement);

        $this->pdo->expects($this->once())->method('prepare')->with($sql)->willReturn($this->statement);
        $db = new SimpleDatabase($this->pdo, $this->logger, false, $readPDO);
        $db->executeQuery($sql);
    }

    public function testIsReadWriteSeparationEnabled()
    {
        $readPDO = $this->createMock(PDOMock::class);
        $db = new SimpleDatabase($this->pdo, $this->logger, false, $readPDO);

        $this->assertTrue($db->isReadWriteSeparationEnabled());

        $db = new SimpleDatabase($this->pdo, $this->logger);
        $this->assertFalse($db->isReadWriteSeparationEnabled());
    }

    public function testGetPdoBasedOnQueryType()
    {
        $readPDO = $this->createMock(PDOMock::class);
        $db = new SimpleDatabase($this->pdo, $this->logger, false, $readPDO);

        $this->assertSame($readPDO, $db->getPdoBasedOnQueryType("SELECT * FROM `users`"));
        $this->assertSame($this->pdo, $db->getPdoBasedOnQueryType("INSERT INTO `users` VALUES (1,2)"));
        $this->assertSame($this->pdo, $db->getPdoBasedOnQueryType("UPDATE `users` SET `password`='admin1' WHERE `id`=1"));
        $this->assertSame($this->pdo, $db->getPdoBasedOnQueryType("DELETE FROM `users` WHERE `id`=1"));
        $this->assertSame($this->pdo, $db->getPdoBasedOnQueryType("REPLACE INTO `users` VALUES (1, 'admin', 'password')"));
        $this->assertSame($this->pdo, $db->getPdoBasedOnQueryType("SET foreign_key_checks=0"));
    }

    public function testGetColumn()
    {
        $sql = "SQL";
        $params = ['foo' => 'bar'];
        $data = ['baz', 'boo'];

        $this->pdo->expects($this->once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects($this->once())->method('execute')->with($params)->willReturn(true);
        $this->statement->expects($this->once())->method('rowCount')->willReturn(1);
        $this->statement->expects($this->once())->method('fetchColumn')->with(0)->willReturn($data[0]);

        $db = new SimpleDatabase($this->pdo, $this->logger);

        $this->assertSame($data[0], $db->getColumn($sql, $params));
    }

    public function testGetColumnWithReadPdo()
    {
        $sql = "SELECT `column` FROM `table`";
        $params = ['foo' => 'bar'];
        $data = ['baz', 'boo'];

        $this->statement->expects($this->once())->method('execute')->with($params)->willReturn(true);
        $this->statement->expects($this->once())->method('rowCount')->willReturn(1);
        $this->statement->expects($this->once())->method('fetchColumn')->with(0)->willReturn($data[0]);

        $readPdo = $this->createMock(PDOMock::class);
        $readPdo->expects($this->once())->method('prepare')->with($sql)->willReturn($this->statement);
        $db = new SimpleDatabase($this->pdo, $this->logger, false, $readPdo);

        $this->assertSame($data[0], $db->getColumn($sql, $params));
    }

    /**
     * @expectedException \Assertis\SimpleDatabase\SimpleDatabaseConstraintException
     */
    public function testGetColumnThrowsExceptionOnNoResults()
    {
        $sql = "SQL";
        $params = ['foo' => 'bar'];

        $this->pdo->expects($this->once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects($this->once())->method('execute')->with($params)->willReturn(true);
        $this->statement->expects($this->once())->method('rowCount')->willReturn(0);

        $db = new SimpleDatabase($this->pdo, $this->logger);
        $db->getColumn($sql, $params);
    }

    public function testGetRow()
    {
        $sql = "SQL";
        $params = ['foo' => 'bar'];
        $data = ['baz', 'boo'];

        $this->pdo->expects($this->once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects($this->once())->method('execute')->with($params)->willReturn(true);
        $this->statement->expects($this->once())->method('rowCount')->willReturn(1);
        $this->statement->expects($this->once())->method('fetch')->willReturn($data);

        $db = new SimpleDatabase($this->pdo, $this->logger);

        $this->assertSame($data, $db->getRow($sql, $params));
    }

    public function testGetRowWithReadPdo()
    {
        $sql = "SELECT * FROM `table`";
        $params = ['foo' => 'bar'];
        $data = ['baz', 'boo'];

        $this->statement->expects($this->once())->method('execute')->with($params)->willReturn(true);
        $this->statement->expects($this->once())->method('rowCount')->willReturn(1);
        $this->statement->expects($this->once())->method('fetch')->willReturn($data);

        $readPdo = $this->createMock(PDOMock::class);
        $readPdo->expects($this->once())->method('prepare')->with($sql)->willReturn($this->statement);
        $db = new SimpleDatabase($this->pdo, $this->logger, false, $readPdo);

        $this->assertSame($data, $db->getRow($sql, $params));
    }

    /**
     * @expectedException \Assertis\SimpleDatabase\SimpleDatabaseConstraintException
     */
    public function testGetRowThrowsExceptionOnNoResults()
    {
        $sql = "SQL";
        $params = ['foo' => 'bar'];

        $this->pdo->expects($this->once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects($this->once())->method('execute')->with($params)->willReturn(true);
        $this->statement->expects($this->once())->method('rowCount')->willReturn(0);

        $db = new SimpleDatabase($this->pdo, $this->logger);
        $db->getRow($sql, $params);
    }

    public function testGetAll()
    {
        $sql = "SELECT * FROM `table`";
        $params = ['foo' => 'bar'];
        $data = [['baz', 'boo']];
        $fetchMode = 1234;

        $this->statement->expects($this->once())->method('execute')->with($params)->willReturn(true);
        $this->statement->expects($this->once())->method('fetchAll')->with($fetchMode)->willReturn($data);

        $readPdo = $this->createMock(PDOMock::class);
        $readPdo->expects($this->once())->method('prepare')->with($sql)->willReturn($this->statement);
        $db = new SimpleDatabase($this->pdo, $this->logger, false, $readPdo);

        $this->assertSame($data, $db->getAll($sql, $params, $fetchMode));
    }

    public function testListTablesStartsWith()
    {
        $prefix = 'pref';
        $sql = "SHOW TABLES LIKE '$prefix%';";
        $tables = ['prefactor', 'predator'];

        $this->pdo->expects($this->once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects($this->once())->method('execute')->willReturn(true);
        $this->statement->expects($this->once())->method('fetchAll')->with(PDO::FETCH_COLUMN)->willReturn($tables);

        $db = new SimpleDatabase($this->pdo, $this->logger);

        $this->assertSame($tables, $db->listTablesStartsWith($prefix));
    }

    public function testListAllTables()
    {
        $sql = "SHOW TABLES LIKE '%';";
        $tables = ['prefactor', 'predator', 'test'];

        $this->pdo->expects($this->once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects($this->once())->method('execute')->willReturn(true);
        $this->statement->expects($this->once())->method('fetchAll')->with(PDO::FETCH_COLUMN)->willReturn($tables);

        $db = new SimpleDatabase($this->pdo, $this->logger);

        $this->assertSame($tables, $db->listAllTables());
    }

    public function testDuplicateTableWithData()
    {
        $createSql = "CREATE TABLE IF NOT EXISTS `new_table` LIKE `table`;";
        $clearSql = "TRUNCATE `new_table`;";
        $insertSql = "INSERT INTO `new_table` SELECT * FROM `table`;";
        
        $statements = [
            $this->createMock(PDOStatementMock::class),
            $this->createMock(PDOStatementMock::class),
            $this->createMock(PDOStatementMock::class),
        ];

        $this->pdo->expects($this->exactly(3))
            ->method('prepare')
            ->withConsecutive([$createSql], [$clearSql], [$insertSql])
            ->willReturnOnConsecutiveCalls($statements[0], $statements[1], $statements[2]);

        $statements[0]->expects($this->once())->method('execute')->willReturn(true);
        $statements[1]->expects($this->once())->method('execute')->willReturn(true);
        $statements[2]->expects($this->once())->method('execute')->willReturn(true);

        $db = new SimpleDatabase($this->pdo, $this->logger);

        $db->duplicateTable('table', 'new_table', true);
    }

    public function testDuplicateTableWithoutData()
    {
        $createSql = "CREATE TABLE IF NOT EXISTS `new_table` LIKE `table`;";

        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with($createSql)
            ->willReturn($this->statement);

        $this->statement->expects($this->once())->method('execute')->willReturn(true);

        $db = new SimpleDatabase($this->pdo, $this->logger);

        $db->duplicateTable('table', 'new_table');
    }

    public function testDropTable()
    {
        $createSql = "DROP TABLE `new_table`;";

        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with($createSql)
            ->willReturn($this->statement);

        $this->statement->expects($this->once())
            ->method('execute')
            ->willReturn(true);

        $db = new SimpleDatabase($this->pdo, $this->logger);

        $db->dropTable('new_table');
    }

    public function testGetColumnFromAllRows()
    {
        $sql = "SQL";
        $params = ['foo' => 'bar'];
        $data = [['baz', 'boo'], ['bing', 'bang']];
        $column = 1;

        $this->pdo->expects($this->once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects($this->once())->method('execute')->with($params)->willReturn(true);
        $this->statement->expects($this->once())->method('fetchAll')->willReturn($data);

        $db = new SimpleDatabase($this->pdo, $this->logger);

        $this->assertSame(
            [$data[0][$column], $data[1][$column]],
            $db->getColumnFromAllRows($sql, $params, $column)
        );
    }

    public function testGetColumnFromAllRowsWithReadPdo()
    {
        $sql = "SELECT `column` FROM `table`";
        $params = ['foo' => 'bar'];
        $data = [['baz', 'boo'], ['bing', 'bang']];
        $column = 1;

        $this->statement->expects($this->once())->method('execute')->with($params)->willReturn(true);
        $this->statement->expects($this->once())->method('fetchAll')->willReturn($data);

        $readPdo = $this->createMock(PDOMock::class);
        $readPdo->expects($this->once())->method('prepare')->with($sql)->willReturn($this->statement);
        $db = new SimpleDatabase($this->pdo, $this->logger, false, $readPdo);

        $this->assertSame(
            [$data[0][$column], $data[1][$column]],
            $db->getColumnFromAllRows($sql, $params, $column)
        );
    }

    public function testGetLastInsertId()
    {
        $id = '1234';
        $this->pdo->expects($this->once())->method('lastInsertId')->willReturn($id);
        $db = new SimpleDatabase($this->pdo, $this->logger);
        $this->assertSame($id, $db->getLastInsertId());
    }

    public function testGetLastInsertIdWithReadPdo()
    {
        $id = '1234';
        $this->pdo->expects($this->once())->method('lastInsertId')->willReturn($id);

        $readPdo = $this->createMock(PDO::class);
        $db = new SimpleDatabase($this->pdo, $this->logger, false, $readPdo);

        $this->assertSame($id, $db->getLastInsertId());
    }

    public function testTransaction()
    {
        $this->pdo->expects($this->exactly(3))->method('prepare')->willReturn($this->statement);
        $this->statement->expects($this->exactly(3))->method('execute')->willReturn(true);

        $db = new SimpleDatabase($this->pdo, $this->logger);
        $db->startTransaction();
        $db->commitTransaction();
        $db->rollbackTransaction();
    }

    public function testTransactionWithReadPdo()
    {
        $this->pdo->expects($this->exactly(3))->method('prepare')->willReturn($this->statement);
        $this->statement->expects($this->exactly(3))->method('execute')->willReturn(true);

        $readPdo = $this->createMock(PDO::class);
        $db = new SimpleDatabase($this->pdo, $this->logger, false, $readPdo);
        $db->startTransaction();
        $db->commitTransaction();
        $db->rollbackTransaction();
    }
}
