<?php

namespace Mehedi\LaravelDynamoDB\Tests;

use Mockery as m;
use PHPUnit\Framework\TestCase;

class BatchRequestTest extends TestCase
{
    use MockHelpers;

    protected function tearDown(): void
    {
        m::close();
    }

    /**
    * @test
    **/
    function it_make_put_item_request()
    {
        $builder = $this->getBuilder();

        $items = [
            [
                'pk' => 'key'
            ],
            [
                'ol' => 'lo',
                'is_active' => true
            ]
        ];

        $expected = [
            'RequestItems' => [
                'users' => [
                    [
                        'PutRequest' => [
                            'Item' => [
                                'pk' => ['S' => 'key']
                            ]
                        ]
                    ],
                    [
                        'PutRequest' => [
                            'Item' => [
                                'ol' => ['S' => 'lo'],
                                'is_active' => ['BOOL' => true]
                            ]
                        ]
                    ],
                ]
            ]
        ];

        $query = $builder->from('users')->putItemBatch($items)[0];

        $this->assertEquals($expected, $query);
    }

    /**
    * @test
    **/
    function it_make_delete_item_request()
    {
        $builder = $this->getBuilder();

        $items = [
            [
                'pk' => 'key'
            ]
        ];

        $expected = [
            'RequestItems' => [
                'users' => [
                    [
                        'DeleteRequest' => [
                            'Key' => [
                                'pk' => ['S' => 'key']
                            ]
                        ]
                    ]
                ]
            ]
        ];

        $query = $builder->from('users')->deleteItemBatch($items)[0];

        $this->assertEquals($expected, $query);
    }

    /**
    * @test
    **/
    function it_make_batch_get_item_request()
    {
        $builder = $this->getBuilder();

        $items = [
            [
                'pk' => 'key'
            ]
        ];

        $expected = [
            'RequestItems' => [
                'users' => [
                    'Keys' => [
                        ['pk' => ['S' => 'key']]
                    ]
                ]
            ]
        ];

        $query = $builder->from('users')->getItemBatch($items)[0];

        $this->assertEquals($expected, $query);
    }

}
