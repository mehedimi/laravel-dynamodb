<?php

namespace Mehedi\LaravelDynamoDB\Concerns;

trait BuildQueries
{
    /**
     * Chunk the results of the query.
     *
     * @param int $count
     * @param callable $callback
     * @return bool
     */
    public function chunk($count, callable $callback): bool
    {
        $this->limit((int) $count);

        $page = 1;

        do {
            $results = $this->get();

            if (call_user_func_array($callback, [$results, $page]) === false) {
                return false;
            }

            if ($hasNextItems = $results->hasNextItems()) {
                $this->exclusiveStartKey($results->getLastEvaluatedKey());
            }

            unset($results);

            $page++;

        } while ($hasNextItems);

        return true;
    }
}
