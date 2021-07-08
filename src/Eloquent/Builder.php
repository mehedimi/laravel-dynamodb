<?php

namespace Mehedi\LaravelDynamoDB\Eloquent;

use Illuminate\Database\Eloquent\ModelNotFoundException;
use Illuminate\Support\Traits\ForwardsCalls;
use InvalidArgumentException;
use Mehedi\LaravelDynamoDB\Query\Builder as QueryBuilder;

/**
 * Class Builder
 *
 * @method static array toArray()
 * @method Builder inTesting()
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
     * @param null $key
     * @param array $columns
     * @return Model|array|null
     */
    public function find($key = null, array $columns = [])
    {
        if (! empty($key)) {
            $this->key(...$key);
        }

        if (! empty($columns)) {
            $this->query->select(...$columns);
        }

        $item = $this->query->from($this->model->getTable())->find();

        if ($this->query->isTesting) {
            return $item;
        }

        if (is_null($item)) {
            return null;
        }

        $this->model->newFromBuilder($item);

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
    public function query(array $columns = [])
    {
        if (! empty($columns)) {
            $this->query->select(...$columns);
        }

        return $this->query->query()->transform(function ($data) {
            return $this->model->newFromBuilder($data);
        });
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

        return $this->query->scan()->transform(function ($data) {
            return $this->model->newFromBuilder($data);
        });
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

    /**
     * Set model key
     *
     * @param $primaryKey
     * @param null $sortKey
     * @throws InvalidArgumentException
     *
     * @return $this
     */
    public function key($primaryKey, $sortKey = null): Builder
    {
        if (is_null($sortKey) && ! is_null($this->model->getSortKeyName())) {
            throw new InvalidArgumentException('Please define value of '. $this->model->getSortKeyName());
        }

        $key = [
            $this->model->getKeyName() => $primaryKey
        ];

        if (! is_null($sortKey)) {
            $key[$this->model->getSortKeyName()] = $sortKey;
        }
        $this->query->key($key);

        return $this;
    }

    /**
     * Handle non existence method calling
     *
     * @param $method
     * @param $parameters
     * @return $this|false|mixed
     */
    public function __call($method, $parameters)
    {
        if (in_array($method, $this->passthru)) {
            return call_user_func_array([$this->query, $method], $parameters);
        }

        $this->forwardCallTo($this->query, $method, $parameters);

        return $this;
    }

}