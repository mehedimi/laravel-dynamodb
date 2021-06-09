<?php

namespace Mehedi\LaravelDynamoDB\Collections;

use Illuminate\Support\Collection;

class ItemCollection extends Collection
{
    protected $metaData = [];

    protected $scannedCount = 0;

    protected $count = 0;

    public function __construct($data = [])
    {
        parent::__construct($data['Items']);

        $this->scannedCount = $data['ScannedCount'];
        $this->count = $data['Count'];
    }

    /**
     * Set meta data
     *
     * @param $meta
     * @return $this
     */
    public function setMetaData($meta)
    {
        $this->metaData = $meta;

        return $this;
    }

    /**
     * Get meta data
     *
     * @return array
     */
    public function getMetaData()
    {
        return $this->metaData;
    }
}