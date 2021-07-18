<?php


namespace Mehedi\LaravelDynamoDB\Tests;

use Mockery as m;
use Mehedi\LaravelDynamoDB\DynamoDBConnection;
use Mehedi\LaravelDynamoDB\Query\Builder;
use Mehedi\LaravelDynamoDB\Query\DynamoDBGrammar;
use Mehedi\LaravelDynamoDB\Query\Processor;

trait MockHelpers
{
    protected function getConnection()
    {
        $connection = m::mock(DynamoDBConnection::class);
        $connection->shouldReceive('getDatabaseName')
            ->andReturn('database');
        $connection->shouldReceive('getClient')
            ->andReturn(new FakeClient());

        return $connection;
    }

    protected function getBuilder()
    {
        $grammar = new DynamoDBGrammar;
        $processor = m::mock(Processor::class);

        return new Builder($this->getConnection(), $grammar, $processor);
    }

    protected function getDynamoDBBuilder()
    {
        $grammar = new DynamoDBGrammar();
        $processor = m::mock(Processor::class);

        return new Builder(m::mock(ConnectionInterface::class), $grammar, $processor);
    }
}