<?php
namespace Mehedi\LaravelDynamoDB;

use Aws\DynamoDb\DynamoDbClient;
use Illuminate\Database\Connection;
use Illuminate\Support\Traits\ForwardsCalls;
use Mehedi\LaravelDynamoDB\Query\Builder;
use Mehedi\LaravelDynamoDB\Query\DynamoDBGrammar;
use Mehedi\LaravelDynamoDB\Query\DynamoDBProcessor;
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
     * @var DynamoDBGrammar $grammar
     */
    public $queryGrammar;

    /**
     * Result processor
     *
     * @var DynamoDBProcessor $processor
     */
    public $postProcessor;


    public function __construct($config)
    {
        $this->config = $config;
        $this->makeClient($config);

        $this->tablePrefix = $config['prefix'] ?? null;

        $this->useDefaultPostProcessor();
        $this->useDefaultQueryGrammar();
    }


    /**
     * Make dynamodb client
     *
     * @param $config
     * @return DynamoDBConnection
     */
    public function makeClient($config)
    {
        $client = new DynamoDbClient([
            'region' => $config['region'] ?? 'us-east-1',
            'version' => $config['version'] ?? 'latest',
            'credentials' => [
                'key' => $config['access_key'] ?? '',
                'secret' => $config['secret_key'] ?? ''
            ],
            'endpoint' => $config['endpoint'] ?? null
        ]);

        return $this->setClient($client);
    }

    /**
     * Query builder
     *
     * @return Builder
     */
    public function query(): Builder
    {
        return new Builder($this);
    }

    /**
     * Set the table
     *
     * @param $table
     * @return Builder
     */
    public function from($table): Builder
    {
        return $this->query()->from($table);
    }

    /**
     * Get default query grammar
     *
     */
    public function getDefaultQueryGrammar()
    {
        return $this->withTablePrefix(new DynamoDBGrammar());
    }

    /**
     * Get default post processor
     *
     * @return DynamoDBProcessor
     */
    public function getDefaultPostProcessor(): DynamoDBProcessor
    {
        return new DynamoDBProcessor();
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
     * Set the dynamodb client
     *
     * @param $client
     * @return $this
     */
    public function setClient($client)
    {
        $this->client = $client;

        return $this;
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
     * @param string $table
     * @param null $as
     * @return Builder
     */
    public function table($table, $as = null): Builder
    {
        return $this->from($table);
    }

    /**
     * @param mixed $value
     * @return RawExpression
     */
    public function raw($value): RawExpression
    {
        return new RawExpression($value);
    }
}
