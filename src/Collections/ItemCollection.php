<?php

namespace Mehedi\LaravelDynamoDB\Collections;

use Illuminate\Support\Collection;

class ItemCollection extends Collection
{
    /**
     * Response meta data
     *
     * @var array $metaData
     */
    protected $metaData = [];

    /**
     * Total item scanned
     *
     * @var int $scannedCount
     */
    protected $scannedCount = 0;

    /**
     * Total return items count
     *
     * @var int
     */
    protected $count = 0;

    /**
     * Last evaluate key
     *
     * @var array|null
     */
    protected $lastEvaluatedKey;

    public function __construct($data = [])
    {
        parent::__construct($data);
    }

    /**
     * Set meta data
     *
     * @param $meta
     * @return $this
     */
    public function setMetaData($meta): ItemCollection
    {
        $this->metaData = $meta;

        return $this;
    }

    /**
     * Get meta data
     *
     * @return array
     */
    public function getMetaData(): array
    {
        return $this->metaData;
    }

    /**
     * Get items count
     *
     * @return int
     */
    public function getItemsCount(): int
    {
        return $this->count;
    }

    /**
     * Get the last evaluate key
     *
     * @return array|null
     */
    public function getLastEvaluatedKey()
    {
        return $this->lastEvaluatedKey;
    }

    /**
     * Has next any items
     *
     * @return bool
     */
    public function hasNextItems()
    {
        return is_array($this->lastEvaluatedKey);
    }

    /**
     * Get scanned row count
     *
     * @return int
     */
    public function getScannedCount(): int
    {
        return $this->scannedCount;
    }

    /**
     * Set scanned count
     *
     * @param $count
     * @return $this
     */
    public function setScannedCount($count): ItemCollection
    {
        $this->scannedCount = $count;

        return $this;
    }

    /**
     * Set items count
     *
     * @param $count
     * @return $this
     */
    public function setItemsCount($count): ItemCollection
    {
        $this->count = $count;

        return $this;
    }

    /**
     * Set last evaluate key
     *
     * @param $key
     * @return $this
     */
    public function setLastEvaluatedKey($key)
    {
        $this->lastEvaluatedKey = $key;

        return $this;
    }
}
