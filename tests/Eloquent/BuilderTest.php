<?php


namespace Mehedi\LaravelDynamoDB\Tests\Eloquent;


use Illuminate\Database\ConnectionResolver;
use Mehedi\LaravelDynamoDB\DynamoDBConnection;
use Mehedi\LaravelDynamoDB\Eloquent\Builder;
use PHPUnit\Framework\TestCase;
use Mockery as m;

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

    protected function tearDown(): void
    {
        m::close();
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
    function it_can_set_model_table_name()
    {
        $query = User::query();

        $this->assertEquals('users', $query->getQuery()->from);
    }

    /**
    * @test
    **/
    function it_can_set_key_conditions()
    {
        $query= User::keyCondition('pk', 'pk-1')->getQuery();

        $this->assertEquals(['#1 = :1'], $query->keyConditionExpressions);
        $this->assertEquals(['#1' => 'pk'], $query->expression->getNames());
        $this->assertEquals([':1' => ['S' => 'pk-1']], $query->expression->getValues());

        $query= User::keyConditionBetween('pk', 'pk-from', 'pk-to')->getQuery();

        $this->assertEquals(['#1 BETWEEN :1 AND :2'], $query->keyConditionExpressions);
        $this->assertEquals(['#1' => 'pk'], $query->expression->getNames());
        $this->assertEquals([':1' => ['S' => 'pk-from'], ':2' => ['S' => 'pk-to']], $query->expression->getValues());
    }

    /**
    * @test
    **/
    function it_can_add_multiple_key_conditions()
    {
        $query = User::keyCondition('name', 'demo')
            ->keyCondition('is_active', false)->getQuery();

        $this->assertEquals(['#1 = :1', '#2 = :2'], $query->keyConditionExpressions);
        $this->assertEquals(['#1' => 'name', '#2' => 'is_active'], $query->expression->getNames());
        $this->assertEquals([':1' => ['S' => 'demo'], ':2' => ['BOOL' => false]], $query->expression->getValues());

        $query = User::keyCondition('name', 'from')
            ->keyConditionBetween('date', 'from', 'to')
            ->keyConditionBeginsWith('name', 'foo')
            ->getQuery();

        $this->assertEquals(['#1 = :1', '#2 BETWEEN :1 AND :2', 'begins_with(#1, :3)'], $query->keyConditionExpressions);
        $this->assertEquals(['#1' => 'name', '#2' => 'date'], $query->expression->getNames());
        $this->assertEquals([':1' => ['S' => 'from'], ':2' => ['S' => 'to'], ':3' => ['S' => 'foo']], $query->expression->getValues());
    }

    /**
    * @test
    **/
    function it_set_an_item_key()
    {
        $query = User::key('foo', 'hello')->getQuery();

        $this->assertEquals(["primary" => "foo","sort" => "hello"], $query->key);
    }

    /**
    * @test
    **/
    function it_set_select_columns()
    {
        $query = User::select('name', 'id')->getQuery();

        $this->assertEquals(['#1', '#2'], $query->projectionExpression);
        $this->assertEquals(['#1' => 'name', '#2' => 'id'], $query->expression->getNames());
    }


}