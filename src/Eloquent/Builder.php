<?php

namespace Mehedi\LaravelDynamoDB\Eloquent;

use Illuminate\Database\Eloquent\ModelNotFoundException;
use Illuminate\Support\Traits\ForwardsCalls;
use Mehedi\LaravelDynamoDB\Query\Builder as QueryBuilder;

/**
 * Class Builder
 *
 * @method static array toArray()
 *
 * @package Mehedi\LaravelDynamoDB\Eloquent
 */
class Builder
{
    use ForwardsCalls;

    /**
     * Query builder instance
     *
     * @var QueryBuilder $query
     */
    public $query;

    /**
     * The model being queried.
     *
     * @var Model
     */
    protected $model;

    /**
     * The methods that should be returned from query builder.
     *
     * @var string[]
     */
    protected $passthru = [
        'toArray',
    ];

    public function __construct(QueryBuilder $query)
    {
        $this->query = $query;
    }

    /**
     * Find a model by its primary key.
     *
     * @param $key
     * @param array $columns
     * @return Model|null
     */
    public function find($key, $columns = [])
    {
        $item = $this->query->key($key)->getItem($columns);

        if (is_null($item)) {
            return null;
        }

        $this->model->setRawAttributes($item);

        return $this->model;
    }

    /**
     * Find a model by its primary key or throw an exception.
     *
     * @param $key
     * @param array $columns
     * @return Model
     *
     * @throws ModelNotFoundException
     */
    public function findOrFail($key, $columns = [])
    {
        $model = $this->find($key, $columns);

        if (is_null($model)) {
            throw (new ModelNotFoundException())
                ->setModel(get_class($this->model), $key);
        }

        return $model;
    }

    /**
     *  Add where condition on key attribute
     *
     * @param $column
     * @param $operator
     * @param null $value
     * @return $this|Builder
     */
    public function whereKey($column, $operator, $value = null)
    {
        $this->query->whereKey(...func_get_args());

        return $this;
    }

    /**
     * @inheritdoc
     *
     * @return Model|null
     */
    public function first($columns = ['*'])
    {
        return $this->query->first($columns);
    }

    /**
     * Query items from database using query mode
     *
     * @param array $columns
     * @return \Mehedi\LaravelDynamoDB\Collections\ItemCollection
     */
    public function query($columns = [])
    {
        if (! empty($columns)) {
            $this->query->select(...$columns);
        }

        return $this->query->query();
    }


    /**
     * Query items from database using scan mode
     *
     * @param array $columns
     * @return \Mehedi\LaravelDynamoDB\Collections\ItemCollection
     */
    public function scan($columns = [])
    {
        if (! empty($columns)) {
            $this->query->select(...$columns);
        }

        return $this->query->scan();
    }

    /**
     * Create a new instance of the model being queried.
     *
     * @param  array  $attributes
     * @return \Illuminate\Database\Eloquent\Model|static
     */
    public function newModelInstance($attributes = [])
    {
        return $this->model->newInstance($attributes)->setConnection(
            $this->query->connection->getName()
        );
    }

    /**
     * Set model instance
     *
     * @param Model $model
     * @return $this
     */
    public function setModel(Model $model)
    {
        $this->model = $model;

        return $this;
    }

    public function __call($method, $parameters)
    {
        if (in_array($method, $this->passthru)) {
            return call_user_func_array([$this->query, $method], $parameters);
        }

        $this->forwardCallTo($this->query, $method, $parameters);

        return $this;
    }

}