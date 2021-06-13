<?php
namespace Mehedi\LaravelDynamoDB\Tests;

use Illuminate\Support\Arr;
use Mehedi\LaravelDynamoDB\DynamoDBConnection;
use PHPUnit\Framework\TestCase;
use function Couchbase\defaultDecoder;

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
            'TableName' => 'Users'
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
            'TableName' => 'Users',
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
            'TableName' => 'Users',
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
            'TableName' => 'Users',
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
            'TableName' => 'Users',
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
            'TableName' => 'Users',
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
            'TableName' => 'Users',
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
            'TableName' => 'Users',
            'Limit' => 10
        ];

        $this->assertEquals($expected, $query);

        $query = $this->connection
            ->from('Users')
            ->limit(-1)
            ->toArray();

        $expected = [
            'TableName' => 'Users',
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
            'TableName' => 'Staging-Users',
        ];

        $this->assertEquals($expected, $query);
    }

    /**
     * @test
     */
    public function it_can_process_raw_expression()
    {
        $query = $this->connection->query()->raw($this->connection->raw([
            'TableName' => 'Users'
        ]))->toArray();

        $expected = [
            'TableName' => 'Users',
        ];

        $this->assertEquals($expected, $query);
    }

    /**
     * @test
     */
    public function it_can_update_a_item()
    {
        $query = $this->connection
            ->table('Users')
            ->key(['PK' => 'User-1'])
            ->inTesting()
            ->update([
                'name' => 'Name Here'
            ])->toArrayForUpdate();

        $expected = [
            'TableName' => 'Users',
            'Key' => [
                'PK' => [
                    'S' => 'User-1'
                ]
            ],
            'ExpressionAttributeNames' => [
                '#1' => 'name'
            ],
            'ExpressionAttributeValues' => [
                ':1' => [
                    'S' => 'Name Here'
                ]
            ],
            'UpdateExpression' => 'set #1 = :1',
            'ReturnValues' => 'NONE'
        ];

        $this->assertEquals($expected, $query);
    }

    /**
     * @test
     */
    public function it_can_remove_attribute_from_a_item()
    {
        $query = $this->connection->table('Users')->key(['PK' => 'User-1'])
            ->inTesting()->update([
                'name' => 'Name Here',
                'age' => null
            ])->toArrayForUpdate();

        $expected = [
            'TableName' => 'Users',
            'Key' => [
                'PK' => [
                    'S' => 'User-1'
                ]
            ],
            'ExpressionAttributeNames' => [
                '#1' => 'name',
                '#2' => 'age',
            ],
            'ExpressionAttributeValues' => [
                ':1' => [
                    'S' => 'Name Here'
                ]
            ],
            'UpdateExpression' => 'set #1 = :1 remove #2',
            'ReturnValues' => 'NONE'
        ];

        $this->assertEquals($expected, $query);
    }

    /**
     * @test
     */
    public function it_can_add_attribute_on_a_item()
    {
        $query = $this->connection->table('Users')->key(['PK' => 'User-1'])
            ->inTesting()->update([
                'name' => 'Name Here',
                'add:age' => 18,
                'add:salary' => 70000,
                'delete:meta' => 'dob'
            ])->toArrayForUpdate();

        $expected = [
            'TableName' => 'Users',
            'Key' => [
                'PK' => [
                    'S' => 'User-1'
                ]
            ],
            'ExpressionAttributeNames' => [
                '#1' => 'name',
                '#2' => 'age',
                '#3' => 'salary',
                '#4' => 'meta',
            ],
            'ExpressionAttributeValues' => [
                ':1' => [
                    'S' => 'Name Here'
                ],
                ':2' => [
                    'N' => 18
                ],
                ':3' => [
                    'N' => 70000
                ],
                ':4' => [
                    'S' => 'dob'
                ]
            ],
            'UpdateExpression' => 'set #1 = :1 add #2 :2, #3 :3 delete #4 :4',
            'ReturnValues' => 'NONE'
        ];

        $this->assertEquals($expected, $query);
    }

    /**
     * @test
     */
    public function it_can_put_item()
    {
        $query = $this->connection
            ->table('Users')
            ->key(['PK' => 'User-1', 'SK' => 'Profile'])
            ->inTesting()->insert([
                'name' => 'Name Here',
                'age' => null
            ])->toArrayForInsert();

        $expected = [
            'TableName' => 'Users',
            'Item' => [
                'name' => ['S' => 'Name Here'],
                'age' => ['NULL' => true],
                'PK' => ['S' => 'User-1'],
                'SK' => ['S' => 'Profile']
            ],
            'ConditionExpression' => '#1 <> :1 and #2 <> :2',
            'ReturnValues' => 'NONE',
            'ExpressionAttributeNames' => [
                '#1' => 'PK',
                '#2' => 'SK',
            ],
            'ExpressionAttributeValues' => [
                ':1' => ['S' => 'User-1'],
                ':2' => ['S' => 'Profile'],
            ],
        ];

        $this->assertEquals($expected, $query);
    }

    /**
     * @test
     */
    public function it_can_delete_an_item()
    {
        $query = $this->connection
            ->table('Users')
            ->key(['PK' => 'User-1', 'SK' => 'Profile'])
            ->inTesting()->delete()->toArrayForDelete();

        $expected = [
            'TableName' => 'Users',
            'ReturnValues' => 'NONE',
            'Key' => [
                'PK' => ['S' => 'User-1'],
                'SK' => ['S' => 'Profile'],
            ]
        ];

        $this->assertEquals($expected, $query);
    }

    /**
     * @test
     */
    public function it_can_increment_a_value_of_attribute()
    {
        $query = $this->connection
            ->table('Users')
            ->key(['PK' => 'User-1', 'SK' => 'Profile'])
            ->inTesting()->increment('visits')->toArrayForUpdate();

        $expected = [
            'TableName' => 'Users',
            'ReturnValues' => 'NONE',
            'Key' => [
                'PK' => ['S' => 'User-1'],
                'SK' => ['S' => 'Profile'],
            ],
            'ExpressionAttributeNames' => [
                '#1' => 'visits'
            ],
            'ExpressionAttributeValues' => [
                ':1' => [
                    'N' => 1
                ]
            ],
            'UpdateExpression' => 'set #1 = #1 + :1'
        ];

        $this->assertEquals($expected, $query);

        $query = $this->connection
            ->table('Users')
            ->key(['PK' => 'User-1', 'SK' => 'Profile'])
            ->inTesting()->increment('add:visits', 2)->toArrayForUpdate();

        $expected['UpdateExpression'] = 'add #1 :1';
        Arr::set($expected, 'ExpressionAttributeValues.:1.N', 2);

        $this->assertEquals($expected, $query);
    }

    /**
     * @test
     */
    public function it_can_decrement_a_value_of_attribute()
    {
        $query = $this->connection
            ->table('Users')
            ->key(['PK' => 'User-1', 'SK' => 'Profile'])
            ->inTesting()
            ->decrement('visits', 1, ['updated_at' => 'now'])
            ->toArrayForUpdate();

        $expected = [
            'TableName' => 'Users',
            'ReturnValues' => 'NONE',
            'Key' => [
                'PK' => ['S' => 'User-1'],
                'SK' => ['S' => 'Profile'],
            ],
            'ExpressionAttributeNames' => [
                '#1' => 'visits',
                '#2' => 'updated_at'
            ],
            'ExpressionAttributeValues' => [
                ':1' => [
                    'N' => 1
                ],
                ':2' => [
                    'S' => 'now'
                ]
            ],
            'UpdateExpression' => 'set #1 = #1 - :1, #2 = :2'
        ];

        $this->assertEquals($expected, $query);
    }

}