<?php

namespace Mehedi\LaravelDynamoDB\Eloquent;

use Illuminate\Database\Eloquent\Model as BaseModel;
use Mehedi\LaravelDynamoDB\Collections\ItemCollection;

/**
 * Class Model
 *
 * @method static Builder keyCondition($column, $operator, $value = null)
 * @method static Builder key($primaryKey, $sortKey = null)
 * @method static Builder select(...$attributes)
 * @method static Model find($key, $column = [])
 * @method static Model findOrFail($key, $column = [])
 * @method static Model create(array $attributes)
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
     * @return array
     */
    public function getKey()
    {
        $key = [
            $this->primaryKey => $this->getAttribute($this->primaryKey)
        ];

        if (! is_null($this->sortKey)) {
            $key[$this->sortKey] = $this->getAttribute($this->sortKey);
        }

        return $key;
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
    public static function query()
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
     * Save the model
     *
     * @param array $options
     * @return array|bool
     */
    public function save(array $options = [])
    {
        $this->mergeAttributesFromClassCasts();

        $query = $this->newModelQuery();

        if ($this->fireModelEvent('saving') === false) {
            return false;
        }

        if ($this->exists) {
            $saved = !$this->isDirty() || $this->updatePerform($query);
        } else {
            $saved = $this->insertPerform($query);

            if (! $this->getConnectionName() &&
                $connection = $query->getConnection()) {
                $this->setConnection($connection->getName());
            }
        }

        // If the model is successfully saved, we need to do a few more things once
        // that is done. We will call the "saved" method here to run any actions
        // we need to happen after a model gets successfully saved right here.
        if ($saved) {
            $this->finishSave($options);
        }

        return $saved;
    }

    /**
     * @param Builder $query
     * @return array|bool
     */
    protected function insertPerform(Builder $query)
    {
        if ($this->fireModelEvent('creating') === false) {
            return false;
        }

        // First we'll need to create a fresh query instance and touch the creation and
        // update timestamps on this model, which are maintained by us for developer
        // convenience. After, we will just continue saving these model instances.
        if ($this->usesTimestamps()) {
            $this->updateTimestamps();
        }

        $attributes = $this->getAttributesForInsert();

        if (empty($attributes)) {
            return true;
        }


        $response = $query->key(...array_values($this->getKey()))->insert($attributes);

        // We will go ahead and set the exists property to true, so that it is set when
        // the created event is fired, just in case the developer tries to update it
        // during the event. This will allow them to do so and run an update here.
        $this->exists = true;

        $this->wasRecentlyCreated = true;

        $this->fireModelEvent('created', false);

        return $response;
    }

    /**
     * @param $query
     * @return bool
     */
    protected function updatePerform($query)
    {
        // If the updating event returns false, we will cancel the update operation so
        // developers can hook Validation systems into their models and cancel this
        // operation if the model does not pass validation. Otherwise, we update.
        if ($this->fireModelEvent('updating') === false) {
            return false;
        }

        // First we need to create a fresh query instance and touch the creation and
        // update timestamp on the model which are maintained by us for developer
        // convenience. Then we will just continue saving the model instances.
        if ($this->usesTimestamps()) {
            $this->updateTimestamps();
        }

        // Once we have run the update operation, we will fire the "updated" event for
        // this model instance. This will allow developers to hook into these after
        // models are updated, giving them a chance to do any special processing.
        $dirty = $this->getDirty();

        if (count($dirty) > 0) {
            $this->setKeysForSaveQuery($query)->update($dirty);

            $this->syncChanges();

            $this->fireModelEvent('updated', false);
        }

        return true;
    }

    /**
     * Set the keys for a save update query.
     *
     * @param  Builder  $query
     * @return Builder
     */
    protected function setKeysForSaveQuery($query): Builder
    {
        $key = (array) $this->getKey();

        $query->key(...array_values($key));

        return $query;
    }

    /**
     * @inheritDoc
     */
    public function fresh($with = [])
    {
        if (! $this->exists) {
            return;
        }

        return $this->find($this->getKey());
    }

    /**
     * @inheritDoc
     */
    public function refresh()
    {
        if (! $this->exists) {
            return $this;
        }

        /** @var \Mehedi\LaravelDynamoDB\Query\Builder $query */
        $query = $this->newQuery()->getQuery();

        $attributes = $query->key($this->getKey())->getItem();

        $this->setRawAttributes($attributes);

        $this->syncOriginal();

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
