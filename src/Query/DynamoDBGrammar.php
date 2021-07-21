<?php

namespace Mehedi\LaravelDynamoDB\Query;

use Illuminate\Database\Grammar as BaseGrammar;
use Mehedi\LaravelDynamoDB\Contracts\BatchRequest;
use Mehedi\LaravelDynamoDB\Query\Batch\Get;
use Mehedi\LaravelDynamoDB\Query\Batch\Write;
use Mehedi\LaravelDynamoDB\Utils\Marshaler;

class DynamoDBGrammar extends BaseGrammar
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
        'returnValues',
        'updates',
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
        'raw',
        'returnValues'
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
        'raw',
        'returnValues'
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
    public function compileGetItem(Builder $builder): array
    {
        return $this->compile($builder, $this->getItemComponents);
    }

    /**
     * Compile insert query
     *
     * @param Builder $builder
     * @return array
     */
    public function compileInsertQuery(Builder $builder): array
    {
        return $this->compile($builder, $this->insertComponents);
    }

    /**
     * Compile delete query
     *
     * @param Builder $builder
     * @return array
     */
    public function compileDeleteQuery(Builder $builder): array
    {
        return $this->compile($builder, $this->deleteComponents);
    }

    /**
     * Compile update query
     *
     * @param Builder $builder
     * @return array
     */
    public function compileUpdateQuery(Builder $builder): array
    {
        return $this->compile($builder, $this->updateComponents);
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
     * Compile batch write item
     *
     * @param Builder $builder
     * @return array|\array[][]
     */
    public function compileBatchWriteItem(Builder $builder)
    {
        return array_map(function (Write $batch) {
            $query = [
                'RequestItems' => []
            ];

            foreach ($batch->getRequests() as $table => $requests) {
                $query['RequestItems'] = [
                    $this->getTablePrefix().$table => array_map(function (BatchRequest $request) {
                        return [
                            basename(str_replace('\\', '/', get_class($request))) => $request->toArray()
                        ];
                    }, $requests)
                ];
            }

            return $query;
        }, $builder->batchRequests);
    }

    /**
     * Compile batch get item request
     *
     * @param Builder $builder
     * @return array
     */
    public function compileBatchGetItem(Builder $builder)
    {
        return array_map(function (Get $batch) use($builder) {
            $query = [
                'RequestItems' => []
            ];

            foreach ($batch->keys() as $table => $keys) {
                $tableWithPrefix = $this->getTablePrefix().$table;

                $query['RequestItems'] = [
                    $tableWithPrefix => []
                ];

                $this->compileExpression($builder->expression, $query['RequestItems'][$tableWithPrefix]);
                $this->compileProjectionExpression($builder->projectionExpression, $query['RequestItems'][$tableWithPrefix]);
                $this->compileConsistentRead($builder->consistentRead, $query['RequestItems'][$tableWithPrefix]);

                $query['RequestItems'][$tableWithPrefix]['Keys'] = array_map(function ($key) {
                    return Marshaler::marshalItem($key);
                }, $keys);
            }

            return $query;
        }, $builder->batchRequests);
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
            $query['ExclusiveStartKey'] = Marshaler::marshalItem($exclusiveStartKey);
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

    /**
     * Compile return type
     *
     * @param string $returnValue
     * @param $query
     */
    public function compileReturnValues(string $returnValue, &$query)
    {
        $query['ReturnValues'] = $returnValue;
    }
}
