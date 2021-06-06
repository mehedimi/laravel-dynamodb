<?php

namespace Mehedi\LaravelDynamoDB\Query;

use Mehedi\LaravelDynamoDB\Utils\Marshaler;
use Mehedi\LaravelDynamoDB\Utils\NumberIterator;

class Expression
{
    protected $nameIterator;

    protected $valueIterator;

    /**
     * Store all attribute name
     *
     * @var array $names
     */
    protected $names = [];

    /**
     * Store all attribute name
     *
     * @var array $values
     */
    protected $values = [];

    /**
     * Expression constructor.
     */
    public function __construct()
    {
        $this->nameIterator = new NumberIterator('#');
        $this->valueIterator = new NumberIterator(':');
    }

    /**
     * Add value
     *
     * @param $value
     * @return false|int|string
     */
    public function addValue($value)
    {
        if (! in_array($value, $this->values, true)) {
            $this->values[$this->getValueKey()] = $value;
        }

        return array_search($value, $this->values, true);
    }

    /**
     * Add name
     *
     * @param $name
     * @return false|int|string
     */
    public function addName($name)
    {
        if (! in_array($name, $this->names, true)) {
            $this->names[$this->getNameKey()] = $name;
        }

        return array_search($name, $this->names, true);
    }


    /**
     * Get the incremented name key
     *
     * @return string
     */
    protected function getNameKey()
    {
        return $this->getKey($this->nameIterator);
    }

    /**
     * Get incremented value key
     *
     * @return mixed
     */
    protected function getValueKey()
    {
        return $this->getKey($this->valueIterator);
    }

    /**
     * Get key name
     *
     * @param $iterator
     * @return mixed
     */
    protected function getKey(&$iterator)
    {
        $key = $iterator->current();
        $iterator->next();
        return $key;
    }

    /**
     * Is empty values
     *
     * @return bool
     */
    public function hasValues()
    {
        return ! empty($this->values);
    }

    /**
     * Is empty names
     *
     * @return bool
     */
    public function hasNames()
    {
        return ! empty($this->names);
    }

    /**
     * Get names
     *
     * @return array
     */
    public function getNames()
    {
        return $this->names;
    }

    /**
     * Get values
     *
     * @return array
     */
    public function getValues()
    {
        return Marshaler::marshalItem($this->values);
    }
}