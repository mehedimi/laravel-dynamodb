<?php

namespace Mehedi\LaravelDynamoDB\Query\Batch;

class Get
{
    /**
     * @var array $keys
     */
    protected $keys = [];

    /**
     * Make an instance of Get class
     *
     * @return Get
     */
    public static function make()
    {
        return new self();
    }

    /**
     * Add a single key
     *
     * @param $table
     * @param $key
     * @return $this
     */
    public function add($table, $key)
    {
        $this->makeTableSpace($table);

        $this->keys[$table][] = $key;

        return $this;
    }

    /**
     * Add multiple keys
     *
     * @param $table
     * @param $keys
     * @return $this
     */
    public function addMany($table, $keys)
    {
        $this->makeTableSpace($table);

        $this->keys[$table] += $keys;

        return $this;
    }

    /**
     * Add an array inside of table name key
     *
     * @param $table
     */
    public function makeTableSpace($table)
    {
        if (! array_key_exists($table, $this->keys)) {
            $this->keys[$table] = [];
        }
    }

    /**
     * Get keys
     *
     * @return array
     */
    public function keys()
    {
        return $this->keys;
    }
}
