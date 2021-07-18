<?php


namespace Mehedi\LaravelDynamoDB\Query;


class ReturnValues
{
    /**
     * Return nothing
     */
    const NONE = 'NONE';

    /**
     * Return all old values
     */
    const ALL_OLD = 'ALL_OLD';

    /**
     * Return updated old values
     */
    const UPDATED_OLD = 'UPDATED_OLD';

    /**
     * Return only new values
     */
    const ALL_NEW = 'ALL_NEW';

    /**
     * Return only updated new values
     */
    const UPDATED_NEW = 'UPDATED_NEW';
}