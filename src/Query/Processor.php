<?php

namespace Mehedi\LaravelDynamoDB\Query;

use Aws\Result;
use Mehedi\LaravelDynamoDB\Utils\Marshaler;

class Processor
{
    /**
     * Process multiple items
     *
     * @param Result $result
     * @return array|array[]
     */
    public function processItems(Result $result)
    {
        return array_map(function ($item) {
            return Marshaler::marshalItem($item);
        }, $result->toArray());
    }
}