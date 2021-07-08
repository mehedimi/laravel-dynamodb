<?php


namespace Mehedi\LaravelDynamoDB\Tests\Eloquent;


use Illuminate\Database\ConnectionResolver;
use Mehedi\LaravelDynamoDB\DynamoDBConnection;
use Mehedi\LaravelDynamoDB\Eloquent\Builder;
use PHPUnit\Framework\TestCase;

class BuilderTest extends TestCase
{
    protected $model;

    protected function setUp(): void
    {
        $connection = new DynamoDBConnection([]);

        $connectionResolver = new ConnectionResolver();
        $connectionResolver->addConnection('dynamodb', $connection);
        $connectionResolver->setDefaultConnection('dynamodb');
        User::setConnectionResolver($connectionResolver);

        $this->model = User::query();
    }

    /** @test */
    function it_is_instance_of_eloquent_builder_instance()
    {
        $this->assertInstanceOf(Builder::class, $this->model);
    }

    /**
     * @test
     */
    function it_can_extract_key()
    {
        $user = new User(['primary' => 'p', 'sort' => 's']);

        $key = [
            'primary' => 'p',
            'sort' => 's'
        ];

        $this->assertEquals($key, $user->getKey());
    }

    /** @test */
    function it_can_passthru_query_method()
    {
        $query = User::whereKey('is_active', true)->toArray();

        $expected = [
            'KeyConditionExpression' => '#1 = :1',
                'ExpressionAttributeNames' => [
                    '#1' => 'is_active'
                ],
            'ExpressionAttributeValues' => [
                ':1' => [
                    'S' => '1'
                ]
            ]
        ];

        $this->assertEquals($expected, $query);
    }

    /**
    * @test
    **/
    function it_can_add_where_on_key()
    {
        $query = User::whereKey('is_active', true)->toArray();

        $expected = [
            'KeyConditionExpression' => '#1 = :1',
            'ExpressionAttributeNames' => [
                '#1' => 'is_active'
            ],
            'ExpressionAttributeValues' => [
                ':1' => [
                    'S' => '1'
                ]
            ]
        ];

        $this->assertEquals($expected, $query);
    }

    /**
    * @test
    **/
    function it_can_add_where_multiple_key()
    {
        $query = User::whereKey('is_active', true)
            ->whereKey('name', 'demo')->toArray();

        $expected = [
            'KeyConditionExpression' => '#1 = :1 and #2 = :2',
            'ExpressionAttributeNames' => [
                '#1' => 'is_active',
                '#2' => 'name'
            ],
            'ExpressionAttributeValues' => [
                ':1' => [
                    'S' => '1'
                ],
                ':2' => [
                    'S' => 'demo'
                ]
            ]
        ];

        $this->assertEquals($expected, $query);
    }

    /**
    * @test
    **/
    function it_can_set_model_key()
    {
        $query = User::key('name', 'hello')->inTesting()->find();
        $expected = [
            'TableName' => 'users',
            'Key' => [
                'primary' => [
                    'S' => 'name'
                ],
                'sort' => [
                    'S' => 'hello'
                ]
            ]
        ];
        $this->assertEquals($expected, $query);
        $query = User::inTesting()->find(['name', 'hello']);
        $this->assertEquals($expected, $query);
    }

}