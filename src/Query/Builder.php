<?php

namespace Mehedi\LaravelDynamoDB\Query;

use Illuminate\Database\ConnectionInterface;
use Illuminate\Support\Str;
use InvalidArgumentException;
use Mehedi\LaravelDynamoDB\Collections\ItemCollection;
use Mehedi\LaravelDynamoDB\DynamoDBConnection;

class Builder
{
    /**
     * The database connection instance.
     *
     * @var DynamoDBConnection
     */
    public $connection;

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
     * Filter expressions
     *
     * @var array $filterExpressions
     */
    public $filterExpressions = [];

    /**
     * Table name
     *
     * @var string $table
     */
    public $from;

    /**
     * The database query grammar instance.
     *
     * @var DynamoDBGrammar
     */
    public $grammar;

    /**
     * The database query post processor instance.
     *
     * @var Processor
     */
    public $processor;
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
     * Key attribute of a item
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
     * The maximum number of items to evaluate
     *
     * @var null|numeric $limit
     */
    public $limit;

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
    public $returnValue;

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
     * Decrement a column's value by a given amount.
     *
     * @param $column
     * @param int|float $amount
     * @param array $extra
     * @return array
     *
     * @throws InvalidArgumentException
     */
    public function decrement($column, $amount = 1, array $extra = []): array
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
     * @return array
     */
    public function delete(): array
    {
        $this->checkKeyExists();

        $query = $this->grammar->compileDeleteQuery($this);

        $response = $this->connection->getClient()->deleteItem($query);

        return $this->processor->processAffectedOperation($response);
    }

    /**
     * Fetch data from dynamodb
     *
     * @param string $mode
     * @return ItemCollection
     */
    public function fetch(string $mode): ItemCollection
    {
        $result = $this->connection->getClient()->{$mode}($this->toArray());

        return $this->processor->processItems($result);
    }

    /**
     *  Set the table which the query is targeting.
     *
     * @param string $table
     * @return $this
     */
    public function from(string $table): Builder
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
     * @param string $mode
     * @return object|null
     */
    public function first(array $columns = [], string $mode = FetchMode::QUERY): ?array
    {
        return $this->limit(1)->{$mode}($columns)->first();
    }

    /**
     * Get an item from Database
     *
     * @return array
     */
    public function find(): array
    {
        $this->checkKeyExists();

        $query = $this->grammar->compileGetItem($this);

        $result = $this->connection->getClient()->getItem($query);

        return $this->processor->processItem($result);
    }

    /**
     * Get the database connection instance.
     *
     * @return \Illuminate\Database\ConnectionInterface
     */
    public function getConnection()
    {
        return $this->connection;
    }

    /**
     * Get an item from the database
     *
     * @return array
     * @alias find()
     */
    public function getItem()
    {
        return $this->find();
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
    public function increment(string $column, $amount = 1, array $extra = []): array
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
     * @param array $item
     * @return array|false
     */
    public function insert(array $item)
    {
        foreach ($this->key ?? [] as $keyColumn => $keyValue) {
            $this->condition($keyColumn, '<>', $keyValue);
        }

        return $this->putItem($item);
    }

    /**
     * Insert or replace an item
     *
     * @param array $item
     * @param string $returnValues
     * @return array|false
     */
    public function insertOrReplace(array $item, string $returnValues = ReturnValues::NONE)
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
     * @param int $count
     * @return $this
     */
    public function limit(int $count): Builder
    {
        $this->limit = $count;

        return $this;
    }

    /**
     * Prepare value and operator
     *
     * @param $value
     * @param $operator
     * @param false $useDefault
     * @return array
     */
    public function prepareValueAndOperator($value, $operator, bool $useDefault = false): array
    {
        if ($useDefault) {
            $value = $operator;
            $operator = '=';
        }

        return [$value, $operator];
    }

    /**
     * Put item
     *
     * @param $item
     * @return array|false
     */
    public function putItem($item)
    {
        if (empty($item)) {
            return false;
        }

        $this->item = $item;

        if (!empty($this->key)) {
            $this->item += $this->key;
        }

        $query = $this->connection->queryGrammar->compileInsertQuery($this);

        $response = $this->connection->getClient()->putItem($query);

        return $this->connection->postProcessor->processAffectedOperation($response);
    }

    /**
     * Select item attributes
     *
     * @return $this
     */
    public function select(...$attributes): Builder
    {
        foreach ($attributes as $attribute) {
            $name = $this->expression->addName($attribute);
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
     * @param RawExpression $query
     * @return $this
     */
    public function raw(RawExpression $query): Builder
    {
        $this->raw = $query;

        return $this;
    }

    /**
     * Set operation return value type
     *
     * @param $valueType
     * @return $this
     */
    public function returnValue($valueType): Builder
    {
        $this->returnValue = $valueType;

        return $this;
    }

    /**
     * Query from dynamodb
     *
     * @return ItemCollection
     */
    public function query(): ItemCollection
    {
        return $this->fetch(FetchMode::QUERY);
    }

    /**
     * Scan from table
     *
     * @return ItemCollection
     */
    public function scan(): ItemCollection
    {
        return $this->fetch(FetchMode::SCAN);
    }

    /**
     * Perform update query
     *
     * @param array $item
     * @return array
     */
    public function update(array $item): array
    {
        $this->checkKeyExists();

        foreach ($item as $column => $value) {
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

        return $this->processor->processUpdate($response);
    }
}