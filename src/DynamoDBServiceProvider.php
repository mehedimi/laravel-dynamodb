<?php

namespace Mehedi\LaravelDynamoDB;


use Illuminate\Support\ServiceProvider;

class DynamoDBServiceProvider extends ServiceProvider
{
    /**
     * Register dynamodb database connection
     */
    public function register()
    {
        $this->app->resolving('db', function ($db) {
            $db->extend('dynamodb', function ($config) {
                return new DynamoDBConnection($config);
            });
        });
    }
}