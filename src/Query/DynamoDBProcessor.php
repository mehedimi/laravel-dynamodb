<?php

namespace Mehedi\LaravelDynamoDB\Query;

use Aws\Result;
use Mehedi\LaravelDynamoDB\Collections\ItemCollection;
use Mehedi\LaravelDynamoDB\Utils\Marshaler;

class DynamoDBProcessor
{
    /**
     * Process multiple items
     *
     * @param Result $result
     * @return ItemCollection
     */
    public function processItems(Result $result): ItemCollection
    {
        $data = $result->toArray();

        $data['Items'] = array_map(function ($item) {
            return Marshaler::unMarshalItem($item);
        }, $data['Items']);

        $lastEvaluatedKey = array_key_exists('LastEvaluatedKey', $data)
            ? Marshaler::unMarshalItem($data['LastEvaluatedKey'], false)
            : null;

        return (new ItemCollection($data['Items']))
            ->setItemsCount($data['ScannedCount'])
            ->setItemsCount($data['Count'])
            ->setMetaData($data['@metadata'])
            ->setLastEvaluatedKey($lastEvaluatedKey);
    }

    /**
     * Process single item
     *
     * @param Result $result
     * @return array
     */
    public function processItem(Result $result): array
    {
        $data = $result->toArray();

        if (array_key_exists('Item', $data)) {
            $data['Item'] = Marshaler::unMarshalItem($data['Item']);
        }

        return $data;
    }

    /**
     * Process update response
     *
     * @param Result $result
     * @return array
     */
    public function processUpdate(Result $result): array
    {
        return Marshaler::unMarshalItem($result->toArray());
    }

    /**
     * Process insert response
     *
     * @param Result $result
     * @return array
     */
    public function processAffectedOperation(Result $result): array
    {
        $data = $result->toArray();

        if (array_key_exists('Attributes', $data)) {
            $data['Attributes'] = Marshaler::unMarshalItem($data['Attributes']);
        }

        return $data;
    }
}
