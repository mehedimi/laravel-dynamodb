<?php
namespace Mehedi\LaravelDynamoDB\Query\Batch;

use Mehedi\LaravelDynamoDB\Contracts\BatchRequest;
use Mehedi\LaravelDynamoDB\Utils\Marshaler;

class DeleteRequest implements BatchRequest
{
    /**
     * @var array
     */
    protected $key;

    public function __construct($key)
    {
        $this->key = $key;
    }

    /**
     * Processed request payload
     *
     * @return array
     */
    public function toArray() : array
    {
        return [
            'Key' => Marshaler::marshalItem($this->key)
        ];
    }

    /**
     * Make an instance of PutRequest
     *
     * @param $key
     * @return DeleteRequest
     */
    public static function make($key) : DeleteRequest
    {
        return new self($key);
    }
}
