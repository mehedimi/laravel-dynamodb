<?php
namespace Mehedi\LaravelDynamoDB\Query\Batch;

use Mehedi\LaravelDynamoDB\Contracts\BatchRequest;
use Mehedi\LaravelDynamoDB\Utils\Marshaler;

class PutRequest implements BatchRequest
{
    /**
     * @var array
     */
    protected $item;

    public function __construct($item)
    {
        $this->item = $item;
    }

    /**
     * Processed request payload
     *
     * @return array
     */
    public function toArray() : array
    {
        return [
            'Item' => Marshaler::marshalItem($this->item)
        ];
    }

    /**
     * Make an instance of PutRequest
     *
     * @param $item
     * @return PutRequest
     */
    public static function make($item) : PutRequest
    {
        return new self($item);
    }
}
