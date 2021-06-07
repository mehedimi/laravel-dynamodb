<?php
namespace Mehedi\LaravelDynamoDB\Tests;

use Mehedi\LaravelDynamoDB\DynamoDBConnection;
use PHPUnit\Framework\TestCase;

class BuilderTest extends TestCase
{
    protected $connection;

    public function setUp(): void
    {
        $this->connection = new DynamoDBConnection([]);
    }

    /**
     * @test
     */
    public function it_can_query_from_a_table()
    {
        $query = $this
            ->connection
            ->from('Users')
            ->toArray();

        $expected = [
            'Table' => 'Users'
        ];

        $this->assertEquals($expected, $query);
    }

    /**
     * @test
     */
    public function it_can_query_by_key_expressions()
    {
        $query = $this->connection
            ->from('Users')
            ->keyCondition('PK', 'USERS')
            ->toArray();

        $expected = [
            'Table' => 'Users',
            'KeyConditionExpression' => '#1 = :1',
            'ExpressionAttributeNames' => [
                '#1' => 'PK'
            ],
            'ExpressionAttributeValues' => [
                ':1' => [
                    'S' => 'USERS'
                ]
            ]
        ];
        // Test equal expression
        $this->assertEquals($expected, $query);
    }

    /**
     * @test
     */
    public function it_can_query_by_begins_with_key_expression()
    {
        $query = $this->connection
            ->from('Users')
            ->keyCondition('PK', 'USERS')
            ->keyConditionBeginsWith('SK', 'Me')
            ->toArray();

        $expected = [
            'Table' => 'Users',
            'KeyConditionExpression' => '#1 = :1 and begins_with(#2, :2)',
            'ExpressionAttributeNames' => [
                '#1' => 'PK',
                '#2' => 'SK'
            ],
            'ExpressionAttributeValues' => [
                ':1' => [
                    'S' => 'USERS'
                ],
                ':2' => [
                    'S' => 'Me'
                ]
            ]
        ];

        $this->assertEquals($expected, $query);
    }

    /**
     * @test
     */
    public function it_can_query_by_between_key_expression()
    {
        $query = $this->connection
            ->from('Users')
            ->keyCondition('PK', 'USERS')
            ->keyConditionBetween('SK', '2', '4')
            ->toArray();

        $expected = [
            'Table' => 'Users',
            'KeyConditionExpression' => '#1 = :1 and #2 BETWEEN :2 AND :3',
            'ExpressionAttributeNames' => [
                '#1' => 'PK',
                '#2' => 'SK'
            ],
            'ExpressionAttributeValues' => [
                ':1' => [
                    'S' => 'USERS'
                ],
                ':2' => [
                    'S' => '2'
                ],
                ':3' => [
                    'S' => '4'
                ]
            ]
        ];

        $this->assertEquals($expected, $query);
    }

    /**
     * @test
     */
    public function it_can_query_by_filter_expression()
    {
        $query = $this->connection
            ->from('Users')
            ->keyCondition('PK', 'USERS')
            ->filter('is_active', true)
            ->orFilter('is_active', false)
            ->toArray();

        $expected = [
            'Table' => 'Users',
            'KeyConditionExpression' => '#1 = :1',
            'ExpressionAttributeNames' => [
                '#1' => 'PK',
                '#2' => 'is_active'
            ],
            'ExpressionAttributeValues' => [
                ':1' => [
                    'S' => 'USERS'
                ],
                ':2' => [
                    'BOOL' => true
                ],
                ':3' => [
                    'BOOL' => false
                ]
            ],
            'FilterExpression' => '#2 = :2 or #2 = :3'
        ];

        $this->assertEquals($expected, $query);
    }

    /**
     * @test
     */
    public function it_can_query_using_consistent_read()
    {
        $query = $this->connection
            ->from('Users')
            ->consistentRead(true)
            ->toArray();

        $expected = [
            'Table' => 'Users',
            'ConsistentRead' => true
        ];

        $this->assertEquals($expected, $query);
    }

    /**
     * @test
     */
    public function it_can_get_result_by_backward()
    {
        $query = $this->connection
            ->from('Users')
            ->scanFromBackward()
            ->toArray();

        $expected = [
            'Table' => 'Users',
            'ScanIndexForward' => false
        ];

        $this->assertEquals($expected, $query);
    }

    /**
     * @test
     */
    public function it_can_limit_query_result()
    {
        $query = $this->connection
            ->from('Users')
            ->limit(10)
            ->toArray();

        $expected = [
            'Table' => 'Users',
            'Limit' => 10
        ];

        $this->assertEquals($expected, $query);

        $query = $this->connection
            ->from('Users')
            ->limit(-1)
            ->toArray();

        $expected = [
            'Table' => 'Users',
        ];

        $this->assertEquals($expected, $query);
    }

    /**
     * @test
     */
    public function it_can_support_table_prefix()
    {
        $query = (new DynamoDBConnection([
            'prefix' => 'Staging-'
        ]))->from('Users')
            ->toArray();

        $expected = [
            'Table' => 'Staging-Users',
        ];

        $this->assertEquals($expected, $query);
    }

    /**
     * @test
     */
    public function it_can_process_raw_expression()
    {
        $query = $this->connection->query()->raw($this->connection->raw([
            'Table' => 'Users'
        ]))->toArray();

        $expected = [
            'Table' => 'Users',
        ];

        $this->assertEquals($expected, $query);
    }

}