<?php
namespace Mehedi\LaravelDynamoDB\Tests;

use Illuminate\Support\Arr;
use Mehedi\LaravelDynamoDB\DynamoDBConnection;
use Mehedi\LaravelDynamoDB\Query\RawExpression;
use Mehedi\LaravelDynamoDB\Query\ReturnValues;
use PHPUnit\Framework\TestCase;
use Mockery as m;

class BuilderTest extends TestCase
{
    use MockHelpers;

    protected $connection;

    protected function tearDown(): void
    {
        m::close();
    }

    public function setUp(): void
    {
        $this->connection = new DynamoDBConnection([]);

        $this->connection->setClient(new FakeClient());

        $this->connection->setPostProcessor(new FakeProcessor());
    }

    /**
     * @test
     */
    public function it_can_query_from_a_table()
    {
        $query = $this
            ->getBuilder()
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
        $query = $this->getBuilder()
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
        $query = $this->getBuilder()
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
        $query = $this->getBuilder()
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
        $query = $this->getBuilder()
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
        $query = $this->getBuilder()
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
        $query = $this->getBuilder()
            ->from('Users')
            ->scanIndexBackward()
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
        $query = $this->getBuilder()
            ->from('Users')
            ->limit(10)
            ->toArray();

        $expected = [
            'TableName' => 'Users',
            'Limit' => 10
        ];

        $this->assertEquals($expected, $query);

        $query = $this->getBuilder()
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
        $query = $this->getBuilder()
            ->raw(new RawExpression(['TableName' => 'Users']))
            ->toArray();

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
            'UpdateExpression' => 'set #1 = :1'
        ];

        $query = $this->connection->from('Users')
            ->key(['PK' => 'User-1'])
            ->update([
                'name' => 'Name Here'
            ]);

        $this->assertEquals($expected, $query);
    }

    /**
     * @test
     */
    public function it_can_remove_attribute_from_a_item()
    {
        $query = $this->connection->table('Users')->key(['PK' => 'User-1'])
            ->update([
                'name' => 'Name Here',
                'age' => null
            ]);

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
            'UpdateExpression' => 'set #1 = :1 remove #2'
        ];

        $this->assertEquals($expected, $query);
    }

    /**
     * @test
     */
    public function it_can_add_attribute_on_a_item()
    {
        $query = $this->connection->table('Users')->key(['PK' => 'User-1'])
            ->update([
                'name' => 'Name Here',
                'add:age' => 18,
                'add:salary' => 70000,
                'delete:meta' => 'dob'
            ]);

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
            'UpdateExpression' => 'set #1 = :1 add #2 :2, #3 :3 delete #4 :4'
        ];

        $this->assertEquals($expected, $query);
    }

    /**
     * @test
     */
    public function it_can_put_item()
    {
        $query = $this->connection->table('Users')
            ->key(['PK' => 'User-1', 'SK' => 'Profile'])->insert([
                'name' => 'Name Here',
                'age' => null
            ]);

        $expected = [
            'TableName' => 'Users',
            'Item' => [
                'name' => ['S' => 'Name Here'],
                'age' => ['NULL' => true],
                'PK' => ['S' => 'User-1'],
                'SK' => ['S' => 'Profile']
            ],
            'ConditionExpression' => '#1 <> :1 and #2 <> :2',
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
            ->key(['PK' => 'User-1', 'SK' => 'Profile'])->delete();

        $expected = [
            'TableName' => 'Users',
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
            ->increment('visits');

        $expected = [
            'TableName' => 'Users',
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
            ->increment('add:visits', 2);

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
            ->decrement('visits', 1, ['updated_at' => 'now']);

        $expected = [
            'TableName' => 'Users',
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

    /**
    * @test
    **/
    function it_can_set_return_value()
    {
        $query = $this->connection
            ->table('Users')
            ->returnValues(ReturnValues::ALL_NEW)
            ->key(['PK' => 'User-1', 'SK' => 'Profile'])
            ->decrement('visits', 1, ['updated_at' => 'now']);

        $expected = [
            'TableName' => 'Users',
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
            'UpdateExpression' => 'set #1 = #1 - :1, #2 = :2',
            'ReturnValues' => 'ALL_NEW'
        ];

        $this->assertEquals($expected, $query);
    }

    /**
    * @test
    **/
    function it_can_add_condition_expression()
    {
        $query = $this->getBuilder()->from('users')
            ->condition('PK', '=', 'demo')
            ->orCondition('SK', 'or value')
            ->putItem(['hello' => 'word']);

        $this->assertEquals([
            '#1' => 'PK',
            '#2' => 'SK'
        ], $query['ExpressionAttributeNames']);

        $this->assertEquals([
            ':1' => [
                'S' => 'demo'
            ],
            ':2' => [
                'S' => 'or value'
            ]
        ], $query['ExpressionAttributeValues']);

        $this->assertEquals('#1 = :1 or #2 = :2', $query['ConditionExpression']);
    }

    /**
    * @test
    **/
    function it_add_attribute_exists_condition()
    {
        $query = $this->getBuilder()->from('users')
            ->conditionAttributeExists('PK')
            ->conditionAttributeExists('LK')
            ->orConditionAttributeExists('SK')
            ->putItem(['hello' => 'word']);

        $this->assertEquals('attribute_exists(#1) and attribute_exists(#2) or attribute_exists(#3)', $query['ConditionExpression']);
    }

    /**
    * @test
    **/
    function it_add_attribute_not_exists_condition()
    {
        $query = $this->getBuilder()->from('users')
            ->conditionAttributeNotExists('PK')
            ->conditionAttributeNotExists('LK')
            ->orConditionAttributeNotExists('SK')
            ->putItem(['hello' => 'word']);

        $this->assertEquals('attribute_not_exists(#1) and attribute_not_exists(#2) or attribute_not_exists(#3)', $query['ConditionExpression']);
    }

    /**
    * @test
    **/
    function it_add_attribute_type_condition()
    {
        $query = $this->getBuilder()->from('users')
            ->conditionAttributeType('PK', 'S')
            ->orConditionAttributeType('LK', 'S')
            ->conditionAttributeType('SK', 'S')
            ->putItem(['hello' => 'word']);

        $this->assertEquals('attribute_type(#1, :1) or attribute_type(#2, :1) and attribute_type(#3, :1)', $query['ConditionExpression']);
    }

    /**
    * @test
    **/
    function it_add_begins_with_condition()
    {
        $query = $this->getBuilder()->from('users')
            ->conditionBeginsWith('PK', 'S')
            ->orConditionBeginsWith('LK', 'S')
            ->conditionBeginsWith('SK', 'S')
            ->putItem(['hello' => 'word']);

        $this->assertEquals('begins_with(#1, :1) or begins_with(#2, :1) and begins_with(#3, :1)', $query['ConditionExpression']);
    }

    /**
    * @test
    **/
    function it_add_contains_condition()
    {
        $query = $this->getBuilder()->from('users')
            ->conditionContains('PK', 'S')
            ->orConditionContains('LK', 'S')
            ->conditionContains('SK', 'S')
            ->putItem(['hello' => 'word']);

        $this->assertEquals('contains(#1, :1) or contains(#2, :1) and contains(#3, :1)', $query['ConditionExpression']);
    }

    /**
    * @test
    **/
    function it_add_size_condition()
    {
        $query = $this->getBuilder()->from('users')
            ->conditionSize('PK', 64000)
            ->orConditionSize('LK', '>', 2)
            ->conditionSize('SK', 3)
            ->putItem(['hello' => 'word']);

        $this->assertEquals('size(#1) = :1 or size(#2) > :2 and size(#3) = :3', $query['ConditionExpression']);
    }
}
