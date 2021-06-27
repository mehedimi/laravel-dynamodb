<?php

namespace Mehedi\LaravelDynamoDB\Query;

use Illuminate\Support\Str;
use InvalidArgumentException;
use Mehedi\LaravelDynamoDB\Collections\ItemCollection;
use Mehedi\LaravelDynamoDB\DynamoDBConnection;

class Builder
{
    /**
     * Dynamodb connection
     *
     * @var DynamoDBConnection $connection
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
     * Is testing
     *
     * @var bool $isTesting
     */
    protected $isTesting = false;

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


    public function __construct(DynamoDBConnection $connection)
    {
        $this->connection = $connection;
        $this->expression = new Expression();
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
    protected function addFilter($column, $operator, $value = null, $type = 'and')
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
    protected function addCondition($column, $operator, $value = null, $type = 'and')
    {
        $column = $this->expression->addName($column);
        $value = $this->expression->addValue($value);

        $this->conditionExpressions[] = [sprintf('%s %s %s', $column, $operator, $value), $type];

        return $this;
    }


    /**
     *  Determines the read consistency model
     *
     * @param bool $mode
     * @return $this
     */
    public function consistentRead(bool $mode)
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
    public function condition($column, $operator, $value = null)
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
     * @param int $amount
     * @param array $extra
     * @param string $returnValues
     * @return $this|array
     *
     * @throws InvalidArgumentException
     */
    public function decrement($column, $amount = 1, $extra = [], $returnValues = 'NONE')
    {
        if (! is_numeric($amount)) {
            throw new InvalidArgumentException('Non-numeric value passed to decrement method.');
        }
        $column = $this->expression->addName($column);
        $amount = $this->expression->addValue($amount);

        $this->updates['set'][] = sprintf('%s = %s - %s', $column, $column, $amount);

        return $this->update($extra, $returnValues);
    }

    /**
     * Delete an item
     *
     * @param string $returnValues
     * @return $this|array
     */
    public function delete($returnValues = 'NONE')
    {
        if ($this->isTesting) {
            return $this;
        }

        return $this->connection->postProcessor->processAffectedOperation(
            $this->connection->getClient()->deleteItem(
                $this->connection->queryGrammar->compileDeleteQuery($this, $returnValues)
            )
        );
    }

    /**
     *  Set the table which the query is targeting.
     *
     * @param $table
     * @return $this
     */
    public function from($table)
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
    public function filter($column, $operator, $value = null)
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );

        return $this->addFilter($column, $operator, $value);
    }

    /**
     * Get one item from database by primary key
     *
     * @param array $columns
     * @param string $mode
     * @return object|null
     */
    public function first(array $columns = [], $mode = 'query'): ?array
    {
        return $this->limit(1)->{$mode}($columns);
    }

    /**
     * Get an item from Database
     *
     * @param array $columns
     * @return array
     */
    public function getItem(array $columns = []): array
    {
        if (empty($this->key)) {
            throw new InvalidArgumentException('Please set the primary key using key() method.');
        }

        if (! empty($columns)) {
            $this->select(...$columns);
        }

        return $this->connection->postProcessor->processItem(
            $this->connection->getClient()->getItem(
                $this->connection->queryGrammar->compileGetItem(
                    $this
                )
            )
        );
    }

    /**
     * Is testing
     *
     * @param bool $mode
     * @return $this
     */
    public function inTesting($mode = true): Builder
    {
        $this->isTesting = $mode;

        return $this;
    }

    /**
     * Increment a column's value by a given amount.
     *
     * @param string $column
     * @param int $amount
     * @param array $extra
     * @param string $returnValues
     * @return $this|array
     *
     */
    public function increment(string $column, int $amount = 1, array $extra = [], string $returnValues = 'NONE')
    {
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

        return $this->update($extra, $returnValues);
    }

    /**
     * Insert item
     *
     * @param array $item
     * @param string $returnValues
     * @return $this|false
     */
    public function insert(array $item, string $returnValues = 'NONE')
    {
        foreach ($this->key ?? [] as $keyColumn => $keyValue) {
            $this->condition($keyColumn, '<>', $keyValue);
        }

        return $this->putItem($item, $returnValues);
    }

    /**
     * Insert or replace an item
     *
     * @param array $item
     * @param string $returnValues
     * @return $this|false
     */
    public function insertOrReplace(array $item, string $returnValues = 'NONE')
    {
        return $this->putItem($item, $returnValues);
    }

    /**
     * Add condition expression on key
     *
     * @param string $column
     * @param string $operator
     * @param null $value
     * @return $this
     */
    public function whereKey(string $column, string $operator, $value = null): Builder
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
    public function whereKeyBetween(string $column, string $from, string $to): Builder
    {
        $column = $this->expression->addName($column);
        $from = $this->expression->addValue($from);
        $to = $this->expression->addValue($to);

        $this->keyConditionExpressions[] = sprintf('%s BETWEEN %s AND %s', $column, $from, $to);
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
     * Add begins with key condition
     *
     * @param $column
     * @param $value
     * @return $this
     */
    public function whereKeyBeginsWith($column, $value): Builder
    {
        $column = $this->expression->addName($column);
        $value = $this->expression->addValue($value);

        $this->keyConditionExpressions[] = sprintf('begins_with(%s, %s)', $column, $value);
        return $this;
    }

    /**
     * Limit query result
     *
     * @param $count
     * @return $this
     */
    public function limit($count): Builder
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
     * @param string $returnValues
     * @return $this|array|false
     */
    public function putItem($item, $returnValues = 'NONE')
    {
        if (empty($item)) {
            return false;
        }

        $this->item = $item;

        if (!empty($this->key)) {
            $this->item += $this->key;
        }

        if ($this->isTesting) {
            return $this;
        }

        return $this->connection->postProcessor->processAffectedOperation(
            $this->connection->getClient()->putItem(
                $this->connection->queryGrammar->compileInsertQuery($this, $returnValues)
            )
        );
    }

    /**
     * Select item attributes
     *
     * @return $this
     */
    public function select(): Builder
    {
        $attributes = func_get_args();

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
    public function scanFromBackward(bool $type = true): Builder
    {
        $this->scanIndexForward = ! $type;

        return $this;
    }

    /**
     * Add or filter expression
     *
     * @param $column
     * @param $operator
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
     * Get dynamodb query from builder
     *
     * @return array
     */
    public function toArray(): array
    {
        return $this->connection->queryGrammar->compileQuery($this);
    }

    /**
     * Get dynamodb update query from builder
     *
     * @param string $returnValues
     * @return array
     */
    public function toArrayForDelete($returnValues = 'NONE'): array
    {
        return $this->connection->queryGrammar->compileDeleteQuery($this, $returnValues);
    }

    /**
     * Get dynamodb update query from builder
     *
     * @param string $returnValues
     * @return array
     */
    public function toArrayForUpdate($returnValues = 'NONE'): array
    {
        return $this->connection->queryGrammar->compileUpdateQuery($this, $returnValues);
    }

    /**
     * Get dynamodb insert query from builder
     *
     * @param string $returnValues
     * @return array
     */
    public function toArrayForInsert($returnValues = 'NONE'): array
    {
        return $this->connection->queryGrammar->compileInsertQuery($this, $returnValues);
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
     * Query from dynamodb
     *
     * @return ItemCollection
     */
    public function query(): ItemCollection
    {
        return $this->connection->postProcessor->processItems(
            $this->connection->getClient()->query(
                $this->toArray()
            )
        );
    }

    /**
     * Scan from table
     *
     * @return ItemCollection
     */
    public function scan(): ItemCollection
    {
        return $this->connection->postProcessor->processItems(
            $this->connection->getClient()->scan(
                $this->toArray()
            )
        );
    }

    /**
     * Perform update query
     *
     * @param array $item
     * @param string $returnValues
     * @return array|Builder
     */
    public function update(array $item, $returnValues = 'NONE')
    {
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

        if ($this->isTesting) {
            return $this;
        }

        return $this->connection->postProcessor->processUpdate(
            $this->connection->getClient()->updateItem(
                $this->toArrayForUpdate($returnValues)
            )
        );
    }
}