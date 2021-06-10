<?php

namespace Mehedi\LaravelDynamoDB\Eloquent;

use Illuminate\Database\Eloquent\Model as BaseModel;

class Model extends BaseModel
{
    public $incrementing = false;

    public $sortKey;

    /**
     * @param \Mehedi\LaravelDynamoDB\Query\Builder $query
     * @return Builder
     */
    public function newEloquentBuilder($query)
    {
        return new Builder($query);
    }

    public function getKey()
    {

    }
}