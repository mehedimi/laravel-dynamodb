<?php
namespace Mehedi\LaravelDynamoDB;

use Aws\DynamoDb\DynamoDbClient;
use Closure;
use Illuminate\Database\ConnectionInterface;
use Illuminate\Support\Traits\ForwardsCalls;
use Mehedi\LaravelDynamoDB\Query\Builder;
use Mehedi\LaravelDynamoDB\Query\Grammar;
use Mehedi\LaravelDynamoDB\Query\Processor;
use Mehedi\LaravelDynamoDB\Query\RawExpression;

class DynamoDBConnection implements ConnectionInterface
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

    /**
     * @inheritdoc
     */
    public function selectOne($query, $bindings = [], $useReadPdo = true)
    {
        //
    }

    /**
     * @inheritdoc
     */
    public function select($query, $bindings = [], $useReadPdo = true)
    {
        //
    }

    /**
     * @inheritdoc
     */
    public function cursor($query, $bindings = [], $useReadPdo = true)
    {
        //
    }

    /**
     * @inheritdoc
     */
    public function insert($query, $bindings = [])
    {
        //
    }

    /**
     * @inheritdoc
     */
    public function update($query, $bindings = [])
    {
        //
    }

    /**
     * @inheritdoc
     */
    public function delete($query, $bindings = [])
    {
        //
    }

    /**
     * @inheritdoc
     */
    public function statement($query, $bindings = [])
    {
        //
    }

    /**
     * @inheritdoc
     */
    public function affectingStatement($query, $bindings = [])
    {
        //
    }

    /**
     * @inheritdoc
     */
    public function unprepared($query)
    {
        //
    }

    /**
     * @inheritdoc
     */
    public function prepareBindings(array $bindings)
    {
        //
    }

    /**
     * @inheritdoc
     */
    public function transaction(Closure $callback, $attempts = 1)
    {
        //
    }

    /**
     * @inheritdoc
     */
    public function beginTransaction()
    {
        //
    }

    /**
     * @inheritdoc
     */
    public function commit()
    {
        //
    }

    /**
     * @inheritdoc
     */
    public function rollBack()
    {
        //
    }

    /**
     * @inheritdoc
     */
    public function transactionLevel()
    {
        //
    }

    /**
     * @inheritdoc
     */
    public function pretend(Closure $callback)
    {
        //
    }

    /**
     * Get database name
     *
     * @return string|null
     */
    public function getDatabaseName()
    {
        return null;
    }
}