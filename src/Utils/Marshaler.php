<?php

namespace Mehedi\LaravelDynamoDB\Utils;

class Marshaler
{
    /**
     * @var \Aws\DynamoDb\Marshaler $instance
     */
    protected static $instance;

    /**
     * Get singleton instance
     *
     * @return \Aws\DynamoDb\Marshaler
     */
    protected static function getInstance()
    {
        if (is_null(self::$instance)) {
            self::$instance = new \Aws\DynamoDb\Marshaler();
        }

        return self::$instance;
    }

    /**
     * Marshal item
     *
     * @param $item
     * @return array
     */
    public static function marshalItem($item)
    {
        return self::getInstance()->marshalItem($item);
    }

    /**
     * Un Marshal Item
     *
     * @param $item
     * @return array
     */
    public static function unMarshalItem($item)
    {
        return self::getInstance()->unmarshalItem($item, true);
    }
}