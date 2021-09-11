<?php

namespace Mehedi\LaravelDynamoDB\Eloquent;

use Illuminate\Support\Traits\ForwardsCalls;
use InvalidArgumentException;
use Mehedi\LaravelDynamoDB\Collections\ItemCollection;
use Mehedi\LaravelDynamoDB\Concerns\BuildQueries;
use Mehedi\LaravelDynamoDB\Query\Builder as QueryBuilder;
use Mehedi\LaravelDynamoDB\Query\FetchMode;
use Illuminate\Database\Eloquent\ModelNotFoundException;

/**
 * Class Builder
 *
 * @method static array toArray()
 * @method insert(array $attributes)
 * @method Builder keyCondition(string $column, $operator, $value = null)
 * @method Builder keyConditionBetween(string $column, string $from, string $to)
 * @method Builder keyConditionBeginsWith(string $column, string $value)
 * @method Builder getQuery()
 *
 * @see  \Mehedi\LaravelDynamoDB\Query\Builder
 */
class Builder extends \Illuminate\Database\Eloquent\Builder
{
    use ForwardsCalls, BuildQueries;

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
        'toArray', 'insert', 'putItemBatch', 'deleteItemBatch'
    ];

    /**
     * Get a paginator only supporting simple next link.
     *
     * @param int $perPage
     * @param array $columns
     * @param string $cursorName
     * @param null $cursor
     * @return \Mehedi\LaravelDynamoDB\Pagination\CursorPaginator
     */
    public function cursorPaginate($perPage = null, $columns = [], $cursorName = 'cursor', $cursor = null)
    {
        return $this->query->cursorPaginate($perPage, $columns, $cursorName, $cursor)
            ->through(function ($item) {
                 return $this->model->newFromBuilder($item);
            });
    }

    /**
     * Find a model by its primary key.
     *
     * @param null $key
     * @param array $columns
     * @return Model|null
     */
    public function find($key, $columns = [])
    {
        if (is_string($key)) {
            $this->key($key);
        } elseif (is_array($key)) {
            $this->key(...$key);
        }

        if (! empty($columns)) {
            $this->query->select($columns);
        }

        $item = $this->query->from($this->model->getTable())->getItem();

        return array_key_exists('Item', $item) ? $this->model->newFromBuilder($item['Item']) : null;
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
     *  Get the first item
     *
     * @param string[] $columns
     * @return Model|null
     */
    public function first($columns = []): ?Model
    {
        $item = $this->query->first($columns);

        return is_null($item) ? null : $this->model->newFromBuilder($item);
    }

    /**
     * Get the first record matching the attributes or create it.
     *
     * @param array|string $key
     * @param array $values
     * @return Model
     */
    public function firstOrCreate(array $key = [], array $values = []): Model
    {
        $model = $this->firstOrNew($key, $values);

        if ($model->exists) {
            return $model;
        }

        $model->save();

        return $model;
    }

    /**
     * Get the first record matching the attributes or instantiate it.
     *
     * @param array $key
     * @param array $values
     * @return Model
     */
    public function firstOrNew(array $key = [], array $values = [])
    {
        $model = $this->find($key);

        return is_null($model) ? $this->newModelInstance($values)->setKey($key) : $model;
    }

    /**
     * Create or update a record matching the attributes, and fill it with values.
     *
     * @param array $key
     * @param array $values
     * @return Model
     */
    public function updateOrCreate(array $key = [], array $values = [])
    {
        $model = $this->firstOrNew($key, $values);

        $model->exists ? $model->update($values) : $model->save();

        return $model;
    }

    /**
     * Alias of getItemBatch
     *
     * @param $keys
     * @param array $columns
     * @return \Illuminate\Support\Collection
     * @alias getItemBatch()
     */
    public function findMany($keys, $columns = [])
    {
        if (!empty($columns)) {
            $this->select($columns);
        }

        return $this->getItemBatch($keys);
    }

    /**
     * Find many models in a single request
     *
     * @param $keys
     * @return \Illuminate\Support\Collection
     */
    public function getItemBatch($keys)
    {
        $primaryKey = $this->model->getKeysName();

        $keys = array_map(function ($key) use ($primaryKey) {
            return array_combine($primaryKey, $key);
        }, $keys);

        return $this->query->getItemBatch($keys, $this->readChunkSize)->transform(function ($item) {
            return $this->model->newFromBuilder($item);
        });
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
     * Get items collection
     *
     * @param array $columns
     * @return ItemCollection
     */
    public function get($columns = [])
    {
        $this->checkFetchMode($this->query->fetchMode);

        return call_user_func_array([$this, $this->query->fetchMode], [$columns]);
    }

    /**
     * Validate fetch mode
     *
     * @param $mode
     */
    protected function checkFetchMode($mode)
    {
        if (! in_array($mode, [FetchMode::QUERY, FetchMode::SCAN])) {
            throw new InvalidArgumentException('Invalid fetch mode: '. $mode);
        }
    }

    /**
     * Save a new model and return the instance.
     *
     * @param  array  $attributes
     * @return Model
     */
    public function create(array $attributes = []): Model
    {
        return tap($this->newModelInstance($attributes), function (Model $instance) {
            $instance->save();
        });
    }

    /**
     * Update model
     *
     * @param array $values
     * @return array
     */
    public function update(array $values)
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
}
