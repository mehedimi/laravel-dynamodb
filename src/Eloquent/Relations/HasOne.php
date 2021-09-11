<?php

namespace Mehedi\LaravelDynamoDB\Eloquent\Relations;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\HasOne as BaseHasOne;

class HasOne extends BaseHasOne
{
    public function addConstraints()
    {
        if (static::$constraints) {
            /** @var \Mehedi\LaravelDynamoDB\Eloquent\Builder $query */
            $query = $this->getRelationQuery();

            $query->query->key([
                $this->getForeignPartitionKeyName() => $this->getForeignPartionKey(),
                $this->getForeignSortKeyName() => $this->getForeignSortKey(),
            ]);
        }
    }

    /**
     * Get foreign partition key name
     *
     * @return string
     */
    public function getForeignPartitionKeyName()
    {
        return $this->foreignKey[0];
    }

    /**
     * Get foreign partition key name
     *
     * @return string
     */
    public function getForeignPartionKey()
    {
        return $this->parent->getAttribute($this->localKey[0]);
    }

    /**
     * Get foreign sort key name
     *
     * @return mixed
     */
    public function getForeignSortKeyName()
    {
        return $this->foreignKey[1];
    }

    /**
     * Get foreign partition key value
     *
     * @return string
     */
    public function getForeignSortKey()
    {
        return $this->parent->getAttribute($this->localKey[1]);
    }

    /**
     * Get the results of the relationship.
     *
     * @return mixed
     */
    public function getResults()
    {
        if (is_null($this->getParentKey())) {
            return $this->getDefaultFor($this->parent);
        }

        return $this->query->find(null) ?: $this->getDefaultFor($this->parent);
    }

    /**
     * Get the key value of the parent's local key.
     *
     * @return mixed
     */
    public function getParentKey()
    {
        return $this->parent->getKey();
    }

    /**
     * Set the constraints for an eager load of the relation.
     *
     * @param  array  $models
     * @return void
     */
    public function addEagerConstraints(array $models)
    {
        dd($models);
        $whereIn = $this->whereInMethod($this->parent, $this->localKey);

        $this->getRelationQuery()->{$whereIn}(
            $this->foreignKey, $this->getKeys($models, $this->localKey)
        );
    }
}
