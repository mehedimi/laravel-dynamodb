<?php

namespace Mehedi\LaravelDynamoDB\Query;

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
     * Query grammer
     *
     * @var Grammar $grammer
     */
    public $grammer;

    /**
     * The name of an index to query.
     *
     * @var string $indexName
     */
    public $indexName;

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


    public function __construct(DynamoDBConnection $connection)
    {
        $this->connection = $connection;
        $this->expression = new Expression();
        $this->grammer = $connection->getDefaultQueryGrammar();
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
     * Select item attributes
     *
     * @return $this
     */
    public function select()
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
    public function scanFromBackward(bool $type = true)
    {
        $this->scanIndexForward = ! $type;

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
     * Add condition expression on key
     *
     * @param $column
     * @param $operator
     * @param null $value
     * @return $this
     */
    public function keyCondition($column, $operator, $value = null)
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
     * @param $column
     * @param $from
     * @param $to
     * @return $this
     */
    public function keyConditionBetween($column, $from, $to)
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
     * @param $column
     * @param $value
     * @return $this
     */
    public function keyConditionBeginsWith($column, $value)
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
    public function limit($count)
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
    public function prepareValueAndOperator($value, $operator, $useDefault = false)
    {
        if ($useDefault) {
            $value = $operator;
            $operator = '=';
        }

        return [$value, $operator];
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
     * Add or filter expression
     *
     * @param $column
     * @param $operator
     * @param null $value
     * @return $this
     */
    public function orFilter($column, $operator = '=', $value = null)
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
    public function toArray()
    {
        return $this->grammer->compileQuery($this);
    }

    /**
     * Set the raw query
     *
     * @param RawExpression $query
     * @return $this
     */
    public function raw(RawExpression $query)
    {
        $this->raw = $query;

        return $this;
    }

    /**
     * Query from dynamodb
     *
     * @return \Illuminate\Support\Collection
     */
    public function query()
    {
        return collect(
            $this->connection->processor->processItems(
                $this->connection->getClient()->query(
                    $this->toArray()
                )
            )
        );
    }

}