<?php

namespace Mehedi\LaravelDynamoDB\Query;

use Illuminate\Database\Grammar as BaseGrammar;
use Mehedi\LaravelDynamoDB\Utils\Marshaler;

class Grammar extends BaseGrammar
{
    /**
     * Select component
     *
     * @var string[] $components
     */
    protected $components = [
        'from',
        'consistentRead',
        'exclusiveStartKey',
        'filterExpressions',
        'indexName',
        'keyConditionExpressions',
        'limit',
        'projectionExpression',
        'scanIndexForward',
        'expression',
        'raw'
    ];

    /**
     * Update components
     *
     * @var string[]
     */
    public $updateComponents = [
        'from',
        'key',
        'expression',
        'raw',
        'updates'
    ];

    /**
     * Insert components
     *
     * @var string[] $insertComponents
     */
    public $insertComponents = [
        'from',
        'item',
        'conditionExpressions',
        'expression',
        'raw'
    ];

    /**
     * Insert components
     *
     * @var string[] $insertComponents
     */
    public $deleteComponents = [
        'from',
        'key',
        'conditionExpressions',
        'expression',
        'raw'
    ];

    /**
     * Get item components
     *
     * @var string[] $getItemComponents
     */
    public $getItemComponents = [
        'key',
        'from',
        'expression',
        'consistentRead',
        'projectionExpression',
    ];

    /**
     * Compile get item query
     *
     * @param Builder $builder
     * @return array
     */
    public function compileGetItem(Builder $builder)
    {
        return $this->compile($builder, $this->getItemComponents);
    }

    /**
     * Compile insert query
     *
     * @param Builder $builder
     * @param $returnValues
     * @return array
     */
    public function compileInsertQuery(Builder $builder, $returnValues)
    {
        $query = $this->compile($builder, $this->insertComponents);

        if (! empty($returnValues)) {
            $query['ReturnValues'] = $returnValues;
        }

        return $query;
    }

    /**
     * Compile delete query
     *
     * @param Builder $builder
     * @param $returnValues
     * @return array
     */
    public function compileDeleteQuery(Builder $builder, $returnValues)
    {
        $query = $this->compile($builder, $this->deleteComponents);

        if (! empty($returnValues)) {
            $query['ReturnValues'] = $returnValues;
        }

        return $query;
    }

    /**
     * Compile update query
     *
     * @param Builder $builder
     * @param $returnValues
     * @return array
     */
    public function compileUpdateQuery(Builder $builder, $returnValues)
    {
        $query = $this->compile($builder, $this->updateComponents);

        if (! empty($returnValues)) {
            $query['ReturnValues'] = $returnValues;
        }

        return $query;
    }

    /**
     * Compile query
     *
     * @param Builder $builder
     * @return array
     */
    public function compileQuery(Builder $builder)
    {
        return $this->compile($builder, $this->components);
    }

    /**
     * Compile components
     *
     * @param Builder $builder
     * @param array $components
     * @return array
     */
    protected function compile(Builder $builder, array $components)
    {
        $query = [];

        foreach ($components as $component) {
            if (isset($builder->{$component})) {
                $method = 'compile'.ucfirst($component);
                $this->{$method}($builder->{$component}, $query);
            }
        }

        return $query;
    }

    /**
     * Compile table name
     *
     * @param $from
     * @param $query
     */
    protected function compileFrom($from, &$query)
    {
        $query['TableName'] = $this->getTablePrefix().$from;
    }

    /**
     * Compile consistent read
     *
     * @param $consistentRead
     * @param $query
     */
    protected function compileConsistentRead($consistentRead, &$query)
    {
        if ($consistentRead) {
            $query['ConsistentRead'] = true;
        }
    }

    /**
     * Compile exclusive start key
     *
     * @param $exclusiveStartKey
     * @param $query
     */
    protected function compileExclusiveStartKey($exclusiveStartKey, &$query)
    {
        if (! empty($exclusiveStartKey)) {
            $query['ExclusiveStartKey'] = $exclusiveStartKey;
        }
    }

    /**
     * Compile filter expressions
     *
     * @param $filterExpressions
     * @param $query
     */
    protected function compileFilterExpressions($filterExpressions, &$query)
    {
        if (empty($filterExpressions)) {
            return;
        }

        $expression = array_shift($filterExpressions);
        $expression = $expression[0];

        $expression .= ' ' . implode(' ', array_map(function ($e){
                return $e[1] . ' ' . $e[0];
            }, $filterExpressions
                )
            );

        $query['FilterExpression'] = $expression;
    }

    /**
     * Compile index name
     *
     * @param $indexName
     * @param $query
     */
    protected function compileIndexName($indexName, &$query)
    {
        if (! empty($indexName)) {
            $query['IndexName'] = $indexName;
        }
    }

    /**
     * Compile key condition expression
     *
     * @param $keyConditionExpressions
     * @param $query
     */
    protected function compileKeyConditionExpressions($keyConditionExpressions, &$query)
    {
        if (empty($keyConditionExpressions)) {
            return;
        }

        $query['KeyConditionExpression'] = implode(' and ', $keyConditionExpressions);
    }

    /**
     * Compile limit
     *
     * @param $limit
     * @param $query
     */
    protected function compileLimit($limit, &$query)
    {
        if ($limit > 0) {
            $query['Limit'] = $limit;
        }
    }

    /**
     * Compile projection expression
     *
     * @param $projectionExpression
     * @param $query
     */
    protected function compileProjectionExpression($projectionExpression, &$query)
    {
        if (empty($projectionExpression)) {
            return;
        }

        $query['ProjectionExpression'] = implode(', ', $projectionExpression);
    }

    /**
     * Compile index forward
     *
     * @param $scanIndexForward
     * @param $query
     */
    protected function scanIndexForward($scanIndexForward, &$query)
    {
        if ($scanIndexForward === false) {
            $query['ScanIndexForward'] = false;
        }
    }

    /**
     * Compile expression names and values
     *
     * @param Expression $expression
     * @param $query
     */
    protected function compileExpression(Expression $expression, &$query)
    {
        if ($expression->hasNames()) {
            $query['ExpressionAttributeNames'] = $expression->getNames();
        }

        if ($expression->hasValues()) {
            $query['ExpressionAttributeValues'] = $expression->getValues();
        }
    }

    /**
     * Compile scan index forward
     *
     * @param $scanIndexForward
     * @param $query
     */
    protected function compileScanIndexForward($scanIndexForward, &$query)
    {
        if ($scanIndexForward === false) {
            $query['ScanIndexForward'] = false;
        }
    }

    /**
     * Compile raw expression
     *
     * @param RawExpression $raw
     * @param $query
     */
    protected function compileRaw(RawExpression $raw, &$query)
    {
        foreach ($raw->toArray() as $key => $value) {
            $query[$key] = $value;
        }
    }

    /**
     * Compile key
     *
     * @param array $key
     * @param $query
     */
    protected function compileKey(array $key, &$query)
    {
        $query['Key'] = Marshaler::marshalItem($key);
    }

    /**
     * Compile update
     *
     * @param array $updates
     * @param $query
     */
    protected function compileUpdates(array $updates, &$query)
    {
        $query['UpdateExpression'] = '';
        foreach ($updates as $operation => $expression) {
            if (empty($expression)) {
                continue;
            }
            $query['UpdateExpression'] .= sprintf('%s %s ', $operation, implode(', ', $expression));
        }

        $query['UpdateExpression'] = trim($query['UpdateExpression']);
    }

    /**
     * Compile item
     *
     * @param array $item
     * @param $query
     */
    protected function compileItem(array $item, &$query)
    {
        $query['Item'] = Marshaler::marshalItem($item);
    }

    /**
     * Compile condition expressions
     *
     * @param array $conditionExpressions
     * @param $query
     */
    protected function compileConditionExpressions(array $conditionExpressions, &$query)
    {
        if (empty($conditionExpressions)) {
            return;
        }

        $query['ConditionExpression'] = array_shift($conditionExpressions)[0];

        foreach ($conditionExpressions as $expression) {
            $query['ConditionExpression'] .= sprintf(' %s %s', $expression[1], $expression[0]);
        }
    }
}