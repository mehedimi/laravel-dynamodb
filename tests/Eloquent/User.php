<?php


namespace Mehedi\LaravelDynamoDB\Tests\Eloquent;


use Mehedi\LaravelDynamoDB\Eloquent\Model;

class User extends Model
{
    protected $primaryKey = 'primary';

    protected $sortKey = 'sort';

    protected $guarded = [];
}