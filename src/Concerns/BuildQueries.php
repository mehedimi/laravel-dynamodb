<?php

namespace Mehedi\LaravelDynamoDB\Concerns;

use Mehedi\LaravelDynamoDB\Query\FetchMode;

trait BuildQueries
{
    /**
     * Chunk the results of the query.
     *
     * @param int $count
     * @param callable $callback
     * @param string $mode
     * @return bool
     */
    public function chunk(int $count, callable $callback, string $mode = FetchMode::QUERY): bool
    {
        $this->limit($count);

        $page = 1;

        do {
            $results = $this->get([], $mode);

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
