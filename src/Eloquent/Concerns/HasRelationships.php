<?php

namespace Mehedi\LaravelDynamoDB\Eloquent\Concerns;

use Mehedi\LaravelDynamoDB\Eloquent\Relations\HasOne;

trait HasRelationships
{
    /**
     * Define a one-to-one relationship.
     *
     * @param  string  $related
     * @param  array|null  $foreignKey
     * @param  array|null  $localKey
     * @return \Mehedi\LaravelDynamoDB\Eloquent\Relations\HasOne
     */
    public function hasOne($related, $foreignKey = null, $localKey = null)
    {
        $instance = $this->newRelatedInstance($related);

        $foreignKey = $foreignKey ?? $instance->getKeysName();

        $localKey = $localKey ?? $this->getKeysName();


        return new HasOne($instance->newQuery(), $this, $foreignKey, $localKey);
    }
}
