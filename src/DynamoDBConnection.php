<?php
namespace Mehedi\LaravelDynamoDB;

use Aws\DynamoDb\DynamoDbClient;
use Closure;
use Illuminate\Database\Connection;
use Illuminate\Support\Traits\ForwardsCalls;
use Mehedi\LaravelDynamoDB\Query\Builder;
use Mehedi\LaravelDynamoDB\Query\Grammar;
use Mehedi\LaravelDynamoDB\Query\Processor;
use Mehedi\LaravelDynamoDB\Query\RawExpression;

class DynamoDBConnection extends Connection
{
    use ForwardsCalls;

    /**
     * DynamoDB Client
     *
     * @var DynamoDbClient $client
     */
    protected $client;

    /**
     * @var string $tablePrefix
     */
    public $tablePrefix;

    /**
     * Query grammar
     *
     * @var Grammar $grammar
     */
    public $queryGrammar;

    /**
     * Result processor
     *
     * @var Processor $processor
     */
    public $postProcessor;


    public function __construct($config)
    {
        $this->config = $config;
        $this->client = $this->makeClient($config);

        $this->tablePrefix = $config['prefix'] ?? null;

        $this->useDefaultPostProcessor();
        $this->useDefaultQueryGrammar();
    }


    /**
     * Make dynamodb client
     *
     * @param $config
     * @return DynamoDbClient
     */
    public function makeClient($config)
    {
        return new DynamoDbClient([
            'region' => $config['region'] ?? 'us-east-1',
            'version' => $config['version'] ?? 'latest',
            'credentials' => [
                'key' => $config['access_key'] ?? '',
                'secret' => $config['secret_key'] ?? ''
            ],
            'endpoint' => $config['endpoint'] ?? null
        ]);
    }

    /**
     * Query builder
     *
     * @return Builder
     */
    public function query()
    {
        return new Builder($this);
    }

    /**
     * Set the table
     *
     * @param $table
     * @return Builder
     */
    public function from($table)
    {
        return $this->query()->from($table);
    }

    /**
     * Get default query grammar
     *
     * @return Grammar
     */
    public function getDefaultQueryGrammar()
    {
        return $this->withTablePrefix(new Grammar());
    }

    /**
     * Get default post processor
     *
     * @return Processor
     */
    public function getDefaultPostProcessor()
    {
        return new Processor();
    }

    /**
     * Get the dynamodb client
     *
     * @return DynamoDbClient
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * Call dynamodb client methods
     *
     * @param $name
     * @param $arguments
     */
    public function __call($name, $arguments)
    {
        $this->forwardCallTo($this->client, $name, $arguments);
    }

    /**
     * Set the table name
     *
     * @param Closure|\Illuminate\Database\Query\Builder|string $table
     * @param null $as
     * @return Builder
     */
    public function table($table, $as = null)
    {
        return $this->from($table);
    }

    /**
     * @param mixed $value
     * @return RawExpression
     */
    public function raw($value)
    {
        return new RawExpression($value);
    }
}