# Laravel DynamoDB
Use aws dynamodb as a database on Laravel

![Banner](https://banners.beyondco.de/Laravel%20DynamoDB.png?theme=light&packageManager=composer+require&packageName=mehedimi%2Flaravel-dynamodb&pattern=architect&style=style_1&description=Use+aws+dynamodb+on+Laravel&md=1&showWatermark=0&fontSize=100px&images=document-search)

### Installation
```shell
composer require mehedimi/laravel-dynamodb
```
### Configuration
Add dynamodb configs to config/database.php:
```
'connections' => [
    'dynamodb' => [
        'driver' => 'dynamodb',
        'region' => env('AWS_DEFAULT_REGION'),
        'access_key' => env('AWS_ACCESS_KEY_ID'),
        'secret_key' => env('AWS_SECRET_ACCESS_KEY'),
        'endpoint' => env('AWS_ENDPOINT'),
    ],
    ...
],
```

### Use general raw PHP project
```php
<?php
    $connection = new \Mehedi\LaravelDynamoDB\DynamoDBConnection([
        // Configuration goes here
    ]);
    
    $connection->table('Users')->query();
```

For more documentation click here