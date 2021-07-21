<?php

namespace Mehedi\LaravelDynamoDB\Query\Batch;

use InvalidArgumentException;
use Mehedi\LaravelDynamoDB\Contracts\BatchRequest;

class Write
{
    /**
     * List of requests
     *
     * @var array $requests
     */
    protected $requests = [];

    /**
     * Make a batch instance
     *
     * @return static
     */
    public static function make()
    {
        return new self();
    }

    /**
     * Add an operation
     *
     * @param $table
     * @param BatchRequest $request
     */
    public function add($table, BatchRequest $request)
    {
        $this->makeTableSpace($table);

        $this->requests[$table][] = $request;
    }

    /**
     * Add many operations
     *
     * @param $table
     * @param array $requests
     * @return $this
     */
    public function addMany($table, array $requests)
    {
        $this->makeTableSpace($table);

        foreach ($requests as $request) {
            if (! ($request instanceof BatchRequest)) {
                throw new InvalidArgumentException('Please implement ~BatchRequest~ interface with your batch request');
            }

            $this->requests[$table][] = $request;
        }

        return $this;
    }

    /**
     * Add an array inside of table name key
     *
     * @param $table
     */
    public function makeTableSpace($table)
    {
        if (! array_key_exists($table, $this->requests)) {
            $this->requests[$table] = [];
        }
    }

    /**
     * Get requests array
     *
     * @return array
     */
    public function getRequests()
    {
        return $this->requests;
    }
}
