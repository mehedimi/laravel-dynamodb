<?php

namespace Mehedi\LaravelDynamoDB\Query;

use BadMethodCallException;
use Illuminate\Database\ConnectionInterface;
use Illuminate\Database\Query\Processors\Processor;
use Illuminate\Pagination\Cursor;
use Illuminate\Support\Str;
use InvalidArgumentException;
use Mehedi\LaravelDynamoDB\Collections\ItemCollection;
use Mehedi\LaravelDynamoDB\Concerns\BuildQueries;
use Mehedi\LaravelDynamoDB\Pagination\CursorPaginator;
use Mehedi\LaravelDynamoDB\Pagination\CursorStorage;
use Mehedi\LaravelDynamoDB\Query\Batch\Get;
use Mehedi\LaravelDynamoDB\Query\Batch\Write;
use Mehedi\LaravelDynamoDB\Query\Batch\DeleteRequest;
use Mehedi\LaravelDynamoDB\Query\Batch\PutRequest;

/**
 * Class Builder
 *
 * @method Builder conditionAttributeExists($path)
 * @method Builder orConditionAttributeExists($path)
 * @method Builder conditionAttributeNotExists($path)
 * @method Builder orConditionAttributeNotExists($path)
 * @method Builder conditionAttributeType($path, $type)
 * @method Builder orConditionAttributeType($path, $type)
 * @method Builder conditionBeginsWith($path, $substr)
 * @method Builder orConditionBeginsWith($path, $substr)
 * @method Builder conditionContains($path, $operand)
 * @method Builder orConditionContains($path, $operand)
 * @property DynamoDBGrammar $grammar
 * @property DynamoDBProcessor $processor
 *
 * @package Mehedi\LaravelDynamoDB\Query
 */
class Builder extends \Illuminate\Database\Query\Builder
{
    use BuildQueries;

    /**
     * @var array $batchRequests
     */
    public $batchRequests;

    /**
     * Batch read chunk site
     *
     * @var int $readChunkSize
     */
    protected $batchReadChunkSize = 100;

    /**
     * Batch write chunk size
     *
     * @var int $writeChunkSize
     */
    protected $batchWriteChunkSize = 25;

    /**
     * Determines the read consistency model
     *
     * @var bool $consistentRead
     */
    public $consistentRead = false;

    /**
     * A condition expression to determine which items should be modified
     *
     * @var array $conditionExpressions
     */
    public $conditionExpressions = [];

    /**
     * List of conditional functions
     *
     * @var string[]
     */
    protected $conditionalFunctions = [
        'attribute_exists',
        'attribute_not_exists',
        'attribute_type',
        'begins_with',
        'contains'
    ];

    /**
     * The primary key of the first item that this operation will evaluate.
     *
     * @var null|array $excludsiveStartKey
     */
    public $exclusiveStartKey;

    /**
     * Expression
     *
     * @var Expression $expression
     */
    public $expression;

    /**
     * Fetching mode query|scan
     *
     * @var string
     */
    public $fetchMode = FetchMode::QUERY;

    /**
     * Filter expressions
     *
     * @var array $filterExpressions
     */
    public $filterExpressions = [];

    /**
     * The name of an index to query.
     *
     * @var string $indexName
     */
    public $indexName;

    /**
     * Insert item
     *
     * @var array $item
     */
    public $item;

    /**
     * Key attribute of an item
     *
     * @var array $key
     */
    public $key;

    /**
     * The condition that specifies the key values for items to be retrieved by the Query action.
     *
     * @var array $keyConditionExpressions
     */
    public $keyConditionExpressions = [];

    /**
     * Projection expression
     *
     * @var array $projectionExpression
     */
    public $projectionExpression = [];

    /**
     * Raw query
     *
     * @var RawExpression $raw
     */
    public $raw;

    /**
     * Return type of operation response
     *
     * @var string $returnType
     */
    public $returnValues;

    /**
     * Specifies the order for index traversal.
     *
     * @var boolean $scanIndexForward
     */
    public $scanIndexForward;

    /**
     * Update expressions
     *
     * @var array[] $updates
     */
    public $updates = [
        'set' => [],
        'remove' => [],
        'add' => [],
        'delete' => []
    ];


    public function __construct(ConnectionInterface $connection,
                                DynamoDBGrammar $grammar = null,
                                Processor $processor = null)
    {
        $this->connection = $connection;
        $this->expression = new Expression();
        $this->grammar = $grammar ?: $connection->getQueryGrammar();
        $this->processor = $processor ?: $connection->getPostProcessor();
    }

    /**
     * Add between filter
     *
     * @param string $column
     * @param $from
     * @param $to
     * @param string $type
     * @return $this
     */
    protected function addBetweenFilter(string $column, $from, $to, string $type = 'and'): Builder
    {
        $column = $this->expression->addName($column);
        $from = $this->expression->addValue($from);
        $to = $this->expression->addValue($to);

        $this->filterExpressions[] = [sprintf('%s BETWEEN %s AND %s', $column, $from, $to), $type];

        return $this;
    }

    /**
     * Add begins with filter
     *
     * @param string $column
     * @param $substr
     * @param string $type
     * @return $this
     */
    protected function addBeginsWithFilter(string $column, $substr, string $type = 'and'): Builder
    {
        $column = $this->expression->addName($column);
        $substr = $this->expression->addValue($substr);

        $this->filterExpressions[] = [sprintf('begins_with(%s, %s)', $column, $substr), $type];

        return $this;
    }

    /**
     * Add filter
     *
     * @param $column
     * @param $operator
     * @param null $value
     * @param string $type
     * @return $this
     */
    protected function addFilter($column, $operator, $value = null, string $type = 'and'): Builder
    {
        $column = $this->expression->addName($column);
        $value = $this->expression->addValue($value);

        $this->filterExpressions[] = [sprintf('%s %s %s', $column, $operator, $value), $type];

        return $this;
    }

    /**
     * Add condition expression
     *
     * @param $column
     * @param $operator
     * @param null $value
     * @param string $type
     * @return $this
     */
    protected function addCondition($column, $operator, $value = null, string $type = 'and'): Builder
    {
        $column = $this->expression->addName($column);
        $value = $this->expression->addValue($value);

        $this->conditionExpressions[] = [sprintf('%s %s %s', $column, $operator, $value), $type];

        return $this;
    }

    /**
     * Add function condition
     *
     * @param $functionName
     * @param $name
     * @param null $value
     * @param string $type
     * @return $this
     */
    protected function addConditionFunction($functionName, $name, $value = null, string $type = 'and')
    {
        $name = $this->expression->addName($name);
        $arguments = [$name];

        if (! is_null($value)) {
            $arguments[] = $this->expression->addValue($value);
        }

        $this->conditionExpressions[] = [
            sprintf('%s(%s)', $functionName, implode(', ', $arguments)), $type
        ];

        return $this;
    }

    /**
     * Alias of exclusive start key
     *
     * @alias exclusiveStartKey($key)
     * @param $key
     * @return $this
     */
    public function afterKey($key)
    {
        return $this->exclusiveStartKey($key);
    }

    /**
     * Check key is exists to the instance
     *
     * @throws InvalidArgumentException
     */
    protected function checkKeyExists()
    {
        if (empty($this->key)) {
            throw new InvalidArgumentException('Please set the primary key using key() method.');
        }
    }

    /**
     *  Determines the read consistency model
     *
     * @param bool $mode
     * @return $this
     */
    public function consistentRead(bool $mode): Builder
    {
        $this->consistentRead = $mode;

        return $this;
    }

    /**
     * Add condition
     *
     * @param $column
     * @param $operator
     * @param null $value
     * @return $this
     */
    public function condition($column, $operator, $value = null): Builder
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );

        return $this->addCondition($column, $operator, $value);
    }

    /**
     * Add size condition
     *
     * @param $column
     * @param $operator
     * @param null $value
     * @param string $type
     * @return $this
     */
    public function conditionSize($column, $operator, $value = null, string $type = 'and'): Builder
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );

        $column = $this->expression->addName($column);
        $value = $this->expression->addValue($value);

        $this->conditionExpressions[] = [sprintf('size(%s) %s %s', $column, $operator, $value), $type];

        return $this;
    }

    /**
     * Get a paginator only supporting simple next link.
     *
     * @param int $perPage
     * @param array $columns
     * @param string $cursorName
     * @param null $cursor
     * @return CursorPaginator
     */
    public function cursorPaginate($perPage = 15, $columns = [], $cursorName = 'cursor', $cursor = null)
    {
        /** @var Cursor|null $cursor */
        $cursor = $cursor ?: \Illuminate\Pagination\CursorPaginator::resolveCurrentCursor($cursorName);

        $cursor = CursorStorage::make($cursor);

        if (! empty($columns)) {
            $this->select(...$columns);
        }

        if ($cursor->hasNextCursor()) {
            $this->exclusiveStartKey($cursor->nextCursor());
        }

        $items = $this->limit((int) $perPage)->fetch();

        return new CursorPaginator($items, $perPage, $cursor->cursor());
    }

    /**
     * Decrement a column's value by a given amount.
     *
     * @param $column
     * @param int|float $amount
     * @param array $extra
     * @return array
     *
     * @throws InvalidArgumentException
     */
    public function decrement($column, $amount = 1, array $extra = [])
    {
        $this->checkKeyExists();

        if (! is_numeric($amount)) {
            throw new InvalidArgumentException('Non-numeric value passed to decrement method.');
        }

        $column = $this->expression->addName($column);
        $amount = $this->expression->addValue($amount);

        $this->updates['set'][] = sprintf('%s = %s - %s', $column, $column, $amount);

        return $this->update($extra);
    }

    /**
     * Delete an item
     *
     * @param $key
     * @return array
     */
    public function delete($key = null): array
    {
        if (! is_null($key)) {
            $this->key($key);
        }

        $this->checkKeyExists();

        $query = $this->grammar->compileDeleteQuery($this);

        $response = $this->connection->getClient()->deleteItem($query);

        return $this->processor->processAffectedOperation($response);
    }

    /**
     * Delete items using batch request
     *
     * @param $keys
     * @return array
     */
    public function deleteItemBatch($keys)
    {
        $this->batchRequests = [];
        $response = [];

        foreach (array_chunk($keys, $this->batchWriteChunkSize) as $keyChunk) {
            $this->batchRequests[] = Write::make()
                ->addMany(
                    $this->from,
                    array_map(function ($key) {
                        return DeleteRequest::make($key);
                    }, $keyChunk)
                );
        }

        $queries = $this->grammar->compileBatchWriteItem($this);

        foreach ($queries as $query) {
            $response[] = $this->connection->getClient()->batchWriteItem($query);
        }

        return $this->processor->processBatchWriteItems($response);
    }

    /**
     * Add exclusive start
     *
     * @param $key
     * @return $this
     */
    public function exclusiveStartKey($key)
    {
        $this->exclusiveStartKey = $key;

        return $this;
    }

    /**
     * Fetch data from dynamodb
     *
     * @return ItemCollection
     */
    public function fetch(): ItemCollection
    {
        $result = $this->connection->getClient()->{$this->fetchMode}($this->toArray());

        return $this->processor->processItems($result);
    }

    /**
     * Select fetch mode
     *
     * @param $mode
     * @return $this
     */
    public function fetchMode($mode)
    {
        $this->fetchMode = $mode;

        return $this;
    }

    /**
     *  Set the table which the query is targeting.
     *
     * @param string $table
     * @return $this
     */
    public function from($table, $as = null): Builder
    {
        $this->from = $table;

        return $this;
    }

    /**
     * Add filter expression
     *
     * @param $column
     * @param $operator
     * @param null $value
     * @return $this
     */
    public function filter($column, $operator, $value = null): Builder
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );

        return $this->addFilter($column, $operator, $value);
    }

    /**
     * Add between filter
     *
     * @param $column
     * @param $from
     * @param $to
     * @return $this
     */
    public function filterBetween($column, $from, $to): Builder
    {
        return $this->addBetweenFilter($column, $from, $to);
    }

    /**
     * Add begins with filter
     *
     * @param $column
     * @param $substr
     * @return $this
     */
    public function filterBeginsWith($column, $substr): Builder
    {
        return $this->addBeginsWithFilter($column, $substr);
    }

    /**
     * Get one item from database by primary key
     *
     * @param array $columns
     * @return object|null
     */
    public function first($columns = [])
    {
        return $this->limit(1)->{$this->fetchMode}($columns)->first();
    }

    /**
     * Get an item from Database
     *
     * @return array
     */
    public function find($key, $columns = [])
    {
        $this->key((array)$key)->select($columns);

        return $this->getItem();
    }

    /**
     * Find multiple item using many keys
     *
     * @param array $keys
     * @param int $chunkSize
     * @return \Illuminate\Support\Collection
     */
    public function findMany(array $keys, $columns = [])
    {
        return $this->select($columns)->getItemBatch($keys);
    }


    /**
     * Get items collection
     *
     * @param array $columns
     * @return ItemCollection
     */
    public function get($columns = [])
    {
        if (! empty($columns)) {
            $this->select($columns);
        }

        return $this->fetch();
    }

    /**
     * Get an item from the database
     *
     * @return array
     * @alias find()
     */
    public function getItem()
    {
        $query = $this->grammar->compileGetItem($this);

        $result = $this->connection->getClient()->getItem($query);

        return $this->processor->processItem($result);
    }

    /**
     * Get item batch
     *
     * @param $keys
     * @return \Illuminate\Support\Collection
     */
    public function getItemBatch($keys)
    {
        $responses = [];
        $this->batchRequests = [];

        foreach (array_chunk($keys, $this->batchReadChunkSize) as $keyChunk) {
            $this->batchRequests[] = Get::make()
                ->addMany($this->from, $keyChunk);
        }

        $queries = $this->grammar->compileBatchGetItem($this);

        foreach ($queries as $query) {
            $responses[] = $this->connection->getClient()->batchGetItem($query);
        }

        return $this->processor->processBatchGetItems($responses, $this->from, $this->connection->getTablePrefix());
    }

    /**
     * Increment a column's value by a given amount.
     *
     * @param string $column
     * @param int|float $amount
     * @param array $extra
     * @return array
     *
     */
    public function increment($column, $amount = 1, array $extra = []): array
    {
        $this->checkKeyExists();

        if (! is_numeric($amount)) {
            throw new InvalidArgumentException('Non-numeric value passed to increment method.');
        }

        $amount = $this->expression->addValue($amount);
        if (Str::startsWith($column, 'add:')) {
            $this->updates['add'][] = sprintf('%s %s', $this->expression->addName(explode(':', $column)[1]), $amount);
        } else {
            $column = $this->expression->addName($column);
            $this->updates['set'][] = sprintf('%s = %s + %s', $column, $column, $amount);
        }

        return $this->update($extra);
    }

    /**
     * Insert item
     *
     * @param array $values
     * @return array|false
     */
    public function insert(array $values)
    {
        foreach ($this->key ?? [] as $keyColumn => $keyValue) {
            $this->condition($keyColumn, '<>', $keyValue);
        }

        return $this->putItem($values);
    }

    /**
     * Set different index
     *
     * @param $indexName
     * @return $this
     */
    public function index($indexName)
    {
        $this->indexName = $indexName;

        return $this;
    }

    /**
     * Insert or replace an item
     *
     * @param array $item
     * @return array|false
     */
    public function insertOrReplace(array $item)
    {
        return $this->putItem($item);
    }

    /**
     * Add condition expression on key
     *
     * @param string $column
     * @param $operator
     * @param null $value
     * @return $this
     */
    public function keyCondition(string $column, $operator, $value = null): Builder
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );

        $column = $this->expression->addName($column);
        $value = $this->expression->addValue($value);

        $this->keyConditionExpressions[] = sprintf('%s %s %s', $column, $operator, $value);

        return $this;
    }

    /**
     * Add between condition on key expression
     *
     * @param string $column
     * @param string $from
     * @param string $to
     * @return $this
     */
    public function keyConditionBetween(string $column, string $from, string $to): Builder
    {
        $column = $this->expression->addName($column);
        $from = $this->expression->addValue($from);
        $to = $this->expression->addValue($to);

        $this->keyConditionExpressions[] = sprintf('%s BETWEEN %s AND %s', $column, $from, $to);
        return $this;
    }

    /**
     * Add begins with key condition
     *
     * @param string $column
     * @param string $value
     * @return $this
     */
    public function keyConditionBeginsWith(string $column, string $value): Builder
    {
        $column = $this->expression->addName($column);
        $value = $this->expression->addValue($value);

        $this->keyConditionExpressions[] = sprintf('begins_with(%s, %s)', $column, $value);
        return $this;
    }

    /**
     * Set the item key
     *
     * @param array $key
     * @return $this
     */
    public function key(array $key): Builder
    {
        $this->key = $key;

        return $this;
    }

    /**
     * Limit query result
     *
     * @param int $value
     * @return $this
     */
    public function limit($value)
    {
        $this->limit = (int) $value;

        return $this;
    }

    /**
     * Put item
     *
     * @param array $item
     * @return array|false
     */
    public function putItem(array $item)
    {
        if (empty($item)) {
            return false;
        }

        $this->item = $item;

        if (! empty($this->key)) {
            $this->item += $this->key;
        }

        $query = $this->grammar->compileInsertQuery($this);

        $response = $this->connection->getClient()->putItem($query);

        return $this->processor->processAffectedOperation($response);
    }

    /**
     * Put item in a batch request
     *
     * @param array $items
     * @return array
     */
    public function putItemBatch(array $items)
    {
        $this->batchRequests = [];

        foreach (array_chunk($items, $this->batchWriteChunkSize) as $itemChunk) {
            $this->batchRequests[] = Write::make()
                ->addMany(
                    $this->from,
                    array_map(function ($item){
                        return PutRequest::make($item);
                    }, $itemChunk)
                );
        }

        $response = [];

        $requests = $this->grammar->compileBatchWriteItem($this);

        foreach ($requests as $request) {
            $response[] = $this->connection->getClient()->batchWriteItem($request);
        }

        return $this->processor->processBatchWriteItems($response);
    }

    /**
     * Select item attributes
     *
     * @return $this
     */
    public function select($columns = []): Builder
    {
        $columns = is_array($columns) ? $columns : func_get_args();

        foreach ($columns as $column) {
            $name = $this->expression->addName($column);
            if (! in_array($name, $this->projectionExpression)) {
                $this->projectionExpression[] = $name;
            }
        }

        return $this;
    }

    /**
     * Scan index forward
     *
     * @param bool $type
     * @return $this
     */
    public function scanIndexBackward(bool $type = true): Builder
    {
        $this->scanIndexForward = ! $type;

        return $this;
    }

    /**
     * Add or condition
     *
     * @param $column
     * @param $operator
     * @param null $value
     * @return $this
     */
    public function orCondition($column, $operator, $value = null): Builder
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );

        return $this->addCondition($column, $operator, $value, 'or');
    }

    /**
     * Add size condition
     *
     * @param $column
     * @param $operator
     * @param null $value
     * @return $this
     */
    public function orConditionSize($column, $operator, $value = null): Builder
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );

        return $this->conditionSize($column, $operator, $value, 'or');
    }

    /**
     * Add or filter expression
     *
     * @param $column
     * @param mixed $operator
     * @param null $value
     * @return $this
     */
    public function orFilter($column, $operator = '=', $value = null): Builder
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );

        return $this->addFilter($column, $operator, $value, 'or');
    }

    /**
     * Add or between filter
     *
     * @param $column
     * @param $from
     * @param $to
     * @return $this
     */
    public function orFilterBetween($column, $from, $to): Builder
    {
        return $this->addBetweenFilter($column, $from, $to, 'or');
    }

    /**
     * Add or begins with filter
     *
     * @param $column
     * @param $substr
     * @return $this
     */
    public function orFilterBeginsWith($column, $substr): Builder
    {
        return $this->addBeginsWithFilter($column, $substr, 'or');
    }


    /**
     * Get dynamodb query from builder
     *
     * @return array
     */
    public function toArray(): array
    {
        return $this->grammar->compileQuery($this);
    }

    /**
     * Set the raw query
     *
     * @param $query
     * @return $this
     */
    public function raw($query): Builder
    {
        $this->raw = new RawExpression($query);

        return $this;
    }

    /**
     * Set operation return value type
     *
     * @param $valueType
     * @return $this
     */
    public function returnValues($valueType): Builder
    {
        $this->returnValues = $valueType;

        return $this;
    }

    /**
     * Query from dynamodb
     *
     * @return ItemCollection
     */
    public function query(): ItemCollection
    {
        return $this->fetch();
    }

    /**
     * Scan from table
     *
     * @return ItemCollection
     */
    public function scan(): ItemCollection
    {
        return $this->fetch();
    }

    /**
     * Perform update query
     *
     * @param array $values
     * @return array
     */
    public function update(array $values): array
    {
        $this->checkKeyExists();

        foreach ($values as $column => $value) {
            if (is_null($value)) {
                $this->updates['remove'][] = $this->expression->addName($column);
                continue;
            }

            $value = $this->expression->addValue($value);
            if (! Str::contains($column, ':')) {
                $this->updates['set'][] = sprintf('%s = %s', $this->expression->addName($column), $value);
                continue;
            }

            $column = explode(':', $column);
            switch ($column[0]) {
                case 'add':
                    $this->updates['add'][] = sprintf('%s %s', $this->expression->addName($column[1]), $value);
                    break;
                case 'delete':
                    $this->updates['delete'][] = sprintf('%s %s', $this->expression->addName($column[1]), $value);
                    break;
            }
        }

        $query = $this->grammar->compileUpdateQuery($this);

        $response = $this->connection->getClient()->updateItem($query);

        return $this->processor->processAffectedOperation($response);
    }

    /**
     * Handle condition function call
     *
     * @param $name
     * @param $arguments
     * @return $this
     */
    public function __call($name, $arguments)
    {
        $method = Str::snake(preg_replace('/(or)?condition/i', '', $name));

        if (in_array($method, $this->conditionalFunctions, true)) {
            return $this->addConditionFunction($method, $arguments[0], $arguments[1] ?? null, Str::startsWith($name, 'or') ? 'or' : 'and');
        }

        throw new BadMethodCallException(sprintf(
            'Call to undefined method %s::%s()', static::class, $method
        ));
    }
}
