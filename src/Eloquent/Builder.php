<?php

namespace Mehedi\LaravelDynamoDB\Eloquent;

use Illuminate\Support\Traits\ForwardsCalls;
use InvalidArgumentException;
use Mehedi\LaravelDynamoDB\Query\Builder as QueryBuilder;
use Mehedi\LaravelDynamoDB\Query\FetchMode;
use Mehedi\LaravelDynamoDB\Query\ReturnValues;
use Illuminate\Database\Eloquent\ModelNotFoundException;

/**
 * Class Builder
 *
 * @method static array toArray()
 * @method Builder inTesting()
 * @method insert(array $attributes)
 * @method Builder keyCondition(string $column, $operator, $value = null)
 * @method Builder keyConditionBetween(string $column, string $from, string $to)
 * @method Builder keyConditionBeginsWith(string $column, string $value)
 *
 * @see  \Mehedi\LaravelDynamoDB\Query\Builder
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
        'toArray', 'insert'
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
     * @return Model|null
     */
    public function find($key, array $columns = [])
    {
        if (is_string($key)) {
            $this->key($key);
        } elseif (is_array($key)) {
            $this->key(...$key);
        }

        if (! empty($columns)) {
            $this->query->select(...$columns);
        }

        $item = $this->query->from($this->model->getTable())->find();


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
    public function findOrFail($key, array $columns = [])
    {
        $model = $this->find($key, $columns);

        if (is_null($model)) {
            throw (new ModelNotFoundException())
                ->setModel(get_class($this->model), $key);
        }

        return $model;
    }

    /**
     *  Get the first item
     *
     * @param string[] $columns
     * @param string $mode
     * @return Model|null
     */
    public function first(array $columns = ['*'], string $mode = FetchMode::QUERY): ?Model
    {
        $item = $this->query->first($columns, $mode);

        return is_null($item) ? null : $this->model->newFromBuilder($item);
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
     * Save a new model and return the instance.
     *
     * @param  array  $attributes
     * @return Model|$this
     */
    public function create(array $attributes = [])
    {
        return tap($this->newModelInstance($attributes), function (Model $instance) {
            $instance->save();
        });
    }

    /**
     * Get the query builder instance
     *
     * @return QueryBuilder
     */
    public function getQuery()
    {
        return $this->query;
    }

    /**
     * Update model
     *
     * @param array $values
     * @param string $returnValues
     * @return array
     */
    public function update(array $values, string $returnValues = ReturnValues::NONE): array
    {
        return $this->query->update($values);
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
     * @param array $attributes
     * @return \Illuminate\Database\Eloquent\Model|static
     */
    public function newModelInstance(array $attributes = [])
    {
        return $this->model->newInstance($attributes)->setConnection(
            $this->query->connection->getName()
        );
    }

    /**
     * Set a model instance for the model being queried.
     *
     * @param  Model  $model
     * @return $this
     */
    public function setModel(Model $model): Builder
    {
        $this->model = $model;
        $this->query->from($model->getTable());

        return $this;
    }

    /**
     * Get the model instance being queried.
     *
     * @return Model
     */
    public function getModel(): Model
    {
        return $this->model;
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