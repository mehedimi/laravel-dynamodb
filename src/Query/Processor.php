<?php

namespace Mehedi\LaravelDynamoDB\Query;

use Aws\Result;
use Mehedi\LaravelDynamoDB\Collections\ItemCollection;
use Mehedi\LaravelDynamoDB\Utils\Marshaler;

class Processor
{
    /**
     * Process multiple items
     *
     * @param Result $result
     * @return ItemCollection
     */
    public function processItems(Result $result)
    {
        $data = $result->toArray();

        $data['Items'] = array_map(function ($item) {
            return Marshaler::unMarshalItem($item);
        }, $data['Items']);

        return (new ItemCollection($data));
    }

    /**
     * Process update response
     *
     * @param Result $result
     * @return array
     */
    public function processUpdate(Result $result)
    {
        return Marshaler::unMarshalItem($result->toArray());
    }

    /**
     * Process insert response
     *
     * @param Result $result
     * @return array
     */
    public function processAffectedOperation(Result $result)
    {
        return Marshaler::unMarshalItem($result->toArray());
    }
}