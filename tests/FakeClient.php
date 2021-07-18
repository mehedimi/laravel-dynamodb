<?php


namespace Mehedi\LaravelDynamoDB\Tests;


use Aws\DynamoDb\DynamoDbClient;

class FakeClient
{
    public function updateItem($query)
    {
        return $query;
    }

    public function putItem($query)
    {
        return $query;
    }

    public function deleteItem($query)
    {
        return $query;
    }
}