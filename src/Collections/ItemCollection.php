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
     * @var int|mixed
     */
    protected $count = 0;

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

    public function getItemsCount()
    {
        return $this->count;
    }

    public function getScannedCount()
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
}