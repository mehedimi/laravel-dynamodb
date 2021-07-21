<?php

namespace Mehedi\LaravelDynamoDB\Contracts;

interface BatchRequest
{
    public function toArray() : array;
}
