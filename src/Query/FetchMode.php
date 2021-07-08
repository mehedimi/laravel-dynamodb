<?php

namespace Mehedi\LaravelDynamoDB\Query;

class FetchMode
{
    /**
     * Fetch using query method
     */
    const QUERY = 'query';

    /**
     * Fetch using scan method
     */
    const SCAN = 'scan';
}