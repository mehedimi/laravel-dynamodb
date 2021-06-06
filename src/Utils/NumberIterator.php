<?php
namespace Mehedi\LaravelDynamoDB\Utils;

use Iterator;

class NumberIterator implements Iterator
{
    /**
     * Counter
     *
     * @var int|mixed $counter
     */
    protected $counter;

    /**
     * Prefix
     *
     * @var string $prefix
     */
    protected $prefix;

    public function __construct($prefix, $counter = 1)
    {
        $this->prefix = $prefix;
        $this->counter = $counter;
    }

    /**
     * Get current value with prefix
     *
     * @return string
     */
    public function current()
    {
        return "{$this->prefix}{$this->counter}";
    }

    /**
     * Next counter
     */
    public function next()
    {
        $this->counter++;
    }

    /**
     * Get key
     *
     * @return integer
     */
    public function key()
    {
        return $this->counter;
    }

    /**
     * @return bool
     */
    public function valid()
    {
        return $this->counter > 0;
    }

    /**
     *  Rewind counter
     */
    public function rewind()
    {
        $this->counter = 0;
    }
}