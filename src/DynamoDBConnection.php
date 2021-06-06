<?php
namespace Mehedi\LaravelDynamoDB;

use Aws\DynamoDb\DynamoDbClient;
use Illuminate\Support\Traits\ForwardsCalls;
use Mehedi\LaravelDynamoDB\Query\Builder;
use Mehedi\LaravelDynamoDB\Query\Grammar;
use Mehedi\LaravelDynamoDB\Query\Processor;

class DynamoDBConnection
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
    public $grammar;

    /**
     * Result processor
     *
     * @var Processor $processor
     */
    public $processor;


    public function __construct($config)
    {
        $this->config = $config;
        $this->client = $this->makeClient($config);

        $this->useDefaultQueryGrammar();
        $this->useDefaultPostProcessor();
    }


    /**
     * Make dynamodb client
     *
     * @param $config
     * @return DynamoDbClient
     */
    public function makeClient($config)
    {
        $this->tablePrefix = $config['prefix'] ?? null;

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
     * Use default query grammar
     */
    public function useDefaultQueryGrammar()
    {
        $this->grammar = $this->getDefaultQueryGrammar();
    }

    /**
     * Use default post processor
     */
    public function useDefaultPostProcessor()
    {
        $this->processor = $this->getDefaultPostProsessor();
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
    public function getDefaultPostProsessor()
    {
        return new Processor();
    }

    /**
     * With table prefix
     *
     * @param Grammar $grammar
     * @return Grammar
     */
    public function withTablePrefix(Grammar $grammar)
    {
        $grammar->setTablePrefix($this->tablePrefix);
        return $grammar;
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
}