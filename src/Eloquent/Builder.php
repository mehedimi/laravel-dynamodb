<?php

namespace Mehedi\LaravelDynamoDB\Eloquent;

use Mehedi\LaravelDynamoDB\Query\Builder as QueryBuilder;

class Builder
{
    /**
     * Query builder instance
     *
     * @var QueryBuilder $query
     */
    public $query;

    public function __construct(QueryBuilder $query)
    {
        $this->query = $query;
    }


}