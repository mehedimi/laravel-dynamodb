<?php

namespace Mehedi\LaravelDynamoDB\Query;

use Illuminate\Database\Grammar as BaseGrammar;

class Grammar extends BaseGrammar
{
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
     * Compile query
     *
     * @param Builder $builder
     * @return array
     */
    public function compileQuery(Builder $builder)
    {
        $this->builder = $builder;

        $query = [];

        foreach ($this->components as $component) {
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
        $query['Table'] = $this->getTablePrefix().$from;
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
}