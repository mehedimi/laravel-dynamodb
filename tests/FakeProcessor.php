<?php

namespace Mehedi\LaravelDynamoDB\Tests;

use Illuminate\Database\Query\Processors\Processor;

class FakeProcessor extends Processor
{
    public function processUpdate($result)
    {
        return $result;
    }

    public function processAffectedOperation($result)
    {
        return $result;
    }

    public function processBatchWriteItems($result)
    {
        return $result;
    }

    public function processBatchGetItems($result)
    {
        return $result;
    }
}
