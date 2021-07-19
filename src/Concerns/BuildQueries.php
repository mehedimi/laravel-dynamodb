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
    protected function chunk(int $count, callable $callback, $mode = FetchMode::QUERY)
    {
        $this->limit($count);
        $page = 1;

        do {
            $results = $this->get([], $mode);

            if (call_user_func($callback, [$results, $page]) === false) {
                return false;
            }

            $hasNextItems = $results->hasNextItems();

            unset($results);

            $page++;

        } while ($hasNextItems);

        return true;
    }
}
