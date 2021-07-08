<?php

namespace Mehedi\LaravelDynamoDB\Eloquent;

use Illuminate\Database\Eloquent\Model as BaseModel;
use Mehedi\LaravelDynamoDB\Collections\ItemCollection;

/**
 * Class Model
 *
 * @method static Builder whereKey($column, $operator, $value = null)
 * @method static Builder key($primaryKey, $sortKey = null)
 * @method static Builder inTesting()
 * @method static Model find($key, $column = [])
 *
 * @see  \Mehedi\LaravelDynamoDB\Eloquent\Builder
 */

abstract class Model extends BaseModel
{
    /**
     * @inheritdoc
     */
    public $incrementing = false;

    /**
     * Sort key of table
     *
     * @var string $sortKey
     */
    protected $sortKey;

    /**
     * @param \Mehedi\LaravelDynamoDB\Query\Builder $query
     * @return Builder
     */
    public function newEloquentBuilder($query): Builder
    {
        return new Builder($query);
    }

    /**
     * Get primary key of a model
     *
     * @return array|mixed
     */
    public function getKey()
    {
        if (is_null($this->sortKey)) {
            return $this->getAttribute($this->primaryKey);
        }

        return [
            $this->primaryKey => $this->getAttribute($this->primaryKey),
            $this->sortKey => $this->getAttribute($this->sortKey)
        ];
    }

    /**
     * Get the sort key name
     *
     * @return string
     */
    public function getSortKeyName()
    {
        return $this->sortKey;
    }

    /**
     * @inheritdoc
     *
     * @return Builder
     */
    public static function query(): Builder
    {
        return (new static())->newQuery();
    }

    /**
     * Get the queueable identity for the entity.
     *
     * @return mixed
     */
    public function getQueueableId()
    {
        $key = $this->getKey();

        return is_array($key) ? implode('-', $key) : $key;
    }

    /**
     * @inheritdoc
     */
    public static function all($columns = [])
    {
        return static::query()->scan($columns);
    }

    /**
     * Create a new Eloquent Collection instance.
     *
     * @param array $models
     * @return ItemCollection
     */
    public function newCollection(array $models = []): ItemCollection
    {
        return (new ItemCollection($models));
    }

    /**
     * Get a new query builder that doesn't have any global scopes.
     *
     * @return Builder
     */
    public function newQueryWithoutScopes(): Builder
    {
        return $this->newModelQuery();
    }

    /**
     * Set sort key name
     *
     * @param $name
     * @return $this
     */
    public function setSortKeyName($name)
    {
        $this->sortKey = $name;

        return $this;
    }


    /**
     * Handle dynamic method calls into the model.
     *
     * @param  string  $method
     * @param  array  $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        return $this->forwardCallTo($this->newQuery(), $method, $parameters);
    }
}