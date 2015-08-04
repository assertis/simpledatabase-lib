<?php

namespace Assertis\SimpleDatabase;

use PDO;
use Test\PDOMock;
use Test\PDOStatementMock;
use PHPUnit_Framework_MockObject_MockObject;
use PHPUnit_Framework_TestCase;
use Psr\Log\LoggerInterface;

/**
 * @author Michał Tatarynowicz <michal@assertis.co.uk>
 */
class SimpleDatabaseTest extends PHPUnit_Framework_TestCase {

    /**
     * @var PHPUnit_Framework_MockObject_MockObject
     */
    private $pdo;
    /**
     * @var PHPUnit_Framework_MockObject_MockObject
     */
    private $logger;
    /**
     * @var PHPUnit_Framework_MockObject_MockObject
     */
    private $statement;

    public function setUp() {
        $this->pdo = $this->getMock(PDOMock::class);
        $this->statement = $this->getMock(PDOStatementMock::class);
        $this->logger = $this->getMock(LoggerInterface::class);
    }

    public function testExecuteQuery() {
        $sql = "SQL";
        $params = [ 'foo' => 'bar' ];

        $this->pdo->expects($this->once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects($this->once())->method('execute')->with($params)->willReturn(true);

        $db = new SimpleDatabase($this->pdo, $this->logger);

        $this->assertSame($this->statement, $db->executeQuery($sql, $params));
    }

    /**
     * @expectedException \Assertis\SimpleDatabase\SimpleDatabaseExecuteException
     */
    public function testExecuteQueryThrowsExceptionOnExecuteError() {
        $sql = "SQL";
        $params = [ 'foo' => 'bar' ];

        $this->pdo->expects($this->once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects($this->once())->method('execute')->with($params)->willReturn(false);
        $this->logger->expects($this->once())->method('error');

        $db = new SimpleDatabase($this->pdo, $this->logger);
        $db->executeQuery($sql, $params);
    }

    public function testGetColumn() {
        $sql = "SQL";
        $params = [ 'foo' => 'bar' ];
        $data = [ 'baz', 'boo' ];

        $this->pdo->expects($this->once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects($this->once())->method('execute')->with($params)->willReturn(true);
        $this->statement->expects($this->once())->method('rowCount')->willReturn(1);
        $this->statement->expects($this->once())->method('fetchColumn')->with(0)->willReturn($data[0]);

        $db = new SimpleDatabase($this->pdo, $this->logger);

        $this->assertSame($data[0], $db->getColumn($sql, $params));
    }

    /**
     * @expectedException \Assertis\SimpleDatabase\SimpleDatabaseConstraintException
     */
    public function testGetColumnThrowsExceptionOnNoResults() {
        $sql = "SQL";
        $params = [ 'foo' => 'bar' ];

        $this->pdo->expects($this->once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects($this->once())->method('execute')->with($params)->willReturn(true);
        $this->statement->expects($this->once())->method('rowCount')->willReturn(0);

        $db = new SimpleDatabase($this->pdo, $this->logger);
        $db->getColumn($sql, $params);
    }

    public function testGetRow() {
        $sql = "SQL";
        $params = [ 'foo' => 'bar' ];
        $data = [ 'baz', 'boo' ];

        $this->pdo->expects($this->once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects($this->once())->method('execute')->with($params)->willReturn(true);
        $this->statement->expects($this->once())->method('rowCount')->willReturn(1);
        $this->statement->expects($this->once())->method('fetch')->willReturn($data);

        $db = new SimpleDatabase($this->pdo, $this->logger);

        $this->assertSame($data, $db->getRow($sql, $params));
    }

    /**
     * @expectedException \Assertis\SimpleDatabase\SimpleDatabaseConstraintException
     */
    public function testGetRowThrowsExceptionOnNoResults() {
        $sql = "SQL";
        $params = [ 'foo' => 'bar' ];

        $this->pdo->expects($this->once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects($this->once())->method('execute')->with($params)->willReturn(true);
        $this->statement->expects($this->once())->method('rowCount')->willReturn(0);

        $db = new SimpleDatabase($this->pdo, $this->logger);
        $db->getRow($sql, $params);
    }

    public function testGetAll() {
        $sql = "SQL";
        $params = [ 'foo' => 'bar' ];
        $data = [ [ 'baz', 'boo' ] ];
        $fetchMode = 1234;

        $this->pdo->expects($this->once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects($this->once())->method('execute')->with($params)->willReturn(true);
        $this->statement->expects($this->once())->method('fetchAll')->with($fetchMode)->willReturn($data);

        $db = new SimpleDatabase($this->pdo, $this->logger);

        $this->assertSame($data, $db->getAll($sql, $params, $fetchMode));
    }

    public function testListTablesStartsWith() {
        $prefix = 'pref';
        $sql = "SHOW TABLES LIKE '$prefix%';";
        $tables = [ 'prefactor', 'predator' ];

        $this->pdo->expects($this->once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects($this->once())->method('execute')->willReturn(true);
        $this->statement->expects($this->once())->method('fetchAll')->with(PDO::FETCH_COLUMN)->willReturn($tables);

        $db = new SimpleDatabase($this->pdo, $this->logger);

        $this->assertSame($tables, $db->listTablesStartsWith($prefix));
    }

    public function testListAllTables() {
        $sql = "SHOW TABLES LIKE '%';";
        $tables = [ 'prefactor', 'predator', 'test' ];

        $this->pdo->expects($this->once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects($this->once())->method('execute')->willReturn(true);
        $this->statement->expects($this->once())->method('fetchAll')->with(PDO::FETCH_COLUMN)->willReturn($tables);

        $db = new SimpleDatabase($this->pdo, $this->logger);

        $this->assertSame($tables, $db->listAllTables());
    }

    public function testDuplicateTableWithData() {
        $createSql = "CREATE TABLE IF NOT EXISTS `newtable` LIKE `table`;";
        $clearSql = "TRUNCATE `newtable`;";
        $insertSql = "INSERT INTO `newtable` SELECT * FROM `table`;";
        $statements = [ $this->getMock(PDOStatementMock::class), $this->getMock(PDOStatementMock::class),
                        $this->getMock(PDOStatementMock::class) ];

        $this->pdo->expects($this->exactly(3))
                  ->method('prepare')
                  ->withConsecutive([ $createSql ], [ $clearSql ], [ $insertSql ])
                  ->willReturnOnConsecutiveCalls($statements[0], $statements[1], $statements[2]);

        $statements[0]->expects($this->once())->method('execute')->willReturn(true);
        $statements[1]->expects($this->once())->method('execute')->willReturn(true);
        $statements[2]->expects($this->once())->method('execute')->willReturn(true);

        $db = new SimpleDatabase($this->pdo, $this->logger);

        $db->duplicateTable('table', 'newtable', true);
    }

    public function testDuplicateTableWithoutData() {
        $createSql = "CREATE TABLE IF NOT EXISTS `newtable` LIKE `table`;";

        $this->pdo->expects($this->once())
                  ->method('prepare')
                  ->with($createSql)
                  ->willReturn($this->statement);

        $this->statement->expects($this->once())->method('execute')->willReturn(true);

        $db = new SimpleDatabase($this->pdo, $this->logger);

        $db->duplicateTable('table', 'newtable');
    }

    public function testDropTable() {
        $createSql = "DROP TABLE `newtable`;";

        $this->pdo->expects($this->once())
                  ->method('prepare')
                  ->with($createSql)
                  ->willReturn($this->statement);

        $this->statement->expects($this->once())
                        ->method('execute')
                        ->willReturn(true);

        $db = new SimpleDatabase($this->pdo, $this->logger);

        $db->dropTable('newtable');
    }

    public function testGetColumnFromAllRows() {
        $sql = "SQL";
        $params = [ 'foo' => 'bar' ];
        $data = [ [ 'baz', 'boo' ], [ 'bing', 'bang' ] ];
        $column = 1;

        $this->pdo->expects($this->once())->method('prepare')->with($sql)->willReturn($this->statement);
        $this->statement->expects($this->once())->method('execute')->with($params)->willReturn(true);
        $this->statement->expects($this->once())->method('fetchAll')->willReturn($data);

        $db = new SimpleDatabase($this->pdo, $this->logger);

        $this->assertSame([ $data[0][ $column ], $data[1][ $column ] ],
            $db->getColumnFromAllRows($sql, $params, $column));
    }

    public function testGetLastInsertId() {
        $id = '1234';
        $this->pdo->expects($this->once())->method('lastInsertId')->willReturn($id);
        $db = new SimpleDatabase($this->pdo, $this->logger);
        $this->assertSame($id, $db->getLastInsertId());
    }

    public function testTransaction() {
        $this->pdo->expects($this->exactly(3))->method('prepare')->willReturn($this->statement);
        $this->statement->expects($this->exactly(3))->method('execute')->willReturn(true);

        $db = new SimpleDatabase($this->pdo, $this->logger);
        $db->startTransaction();
        $db->commitTransaction();
        $db->rollbackTransaction();
    }
}
