<?php


namespace Mehedi\LaravelDynamoDB\Pagination;

use Illuminate\Pagination\Cursor;

class CursorStorage
{
    /**
     * @var Cursor $cursor
     */
    protected $cursor;

    public function __construct($cursor)
    {
        $payload = is_null($cursor) ? ['p' => [], 'n' => []] : $cursor->toArray();

        $this->cursor = new Cursor($payload);
    }

    /**
     * Make an instance of cursor
     *
     * @param $cursor
     * @return CursorStorage
     */
    public static function make($cursor)
    {
        return new self($cursor);
    }


    /**
     * Has any next cursor
     *
     * @return bool
     */
    public function hasNextCursor()
    {
        return ! empty($this->nextCursor());
    }

    /**
     * Get the next cursor
     *
     * @return array
     */
    public function nextCursor()
    {
       return $this->cursor->parameter('n');
    }

    /**
     * Cursor object
     *
     * @return Cursor
     */
    public function cursor()
    {
        return $this->cursor;
    }

    /**
     * Get next cursor object
     *
     * @param $key
     * @return Cursor
     */
    public function nextCursorObject($key)
    {
        $previous = $this->cursor->parameter('p');
        $next = $this->cursor->parameter('n');

        if ( !empty($next)) {
            $previous[] = $next;
        }

        return new Cursor([
            'p' => $previous,
            'n' => $key
        ]);
    }

    /**
     * Get previous cursor object
     *
     * @return Cursor
     */
    public function previousCursorObject()
    {
        /** @var array $previous */
        $previous = $this->cursor->parameter('p');

        $next = array_pop($previous) ?? [];

        return new Cursor([
            'p' => $previous,
            'n' => $next
        ]);
    }
}
