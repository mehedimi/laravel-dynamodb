<?php

namespace Mehedi\LaravelDynamoDB\Pagination;

use ArrayAccess;
use Countable;
use Illuminate\Contracts\Pagination\CursorPaginator as PaginatorContract;
use Illuminate\Contracts\Support\Arrayable;
use Illuminate\Contracts\Support\Jsonable;
use Illuminate\Pagination\AbstractCursorPaginator;
use Illuminate\Pagination\Paginator;
use IteratorAggregate;
use JsonSerializable;
use Mehedi\LaravelDynamoDB\Collections\ItemCollection;

class CursorPaginator extends AbstractCursorPaginator implements PaginatorContract, Arrayable, ArrayAccess, JsonSerializable, Countable, Jsonable, IteratorAggregate
{
    /**
     * All of the items being paginated.
     *
     * @var \Mehedi\LaravelDynamoDB\Collections\ItemCollection
     */
    protected $items;

    /**
     * Create a new paginator instance.
     *
     * @param  mixed  $items
     * @param  int  $perPage
     * @param  \Illuminate\Pagination\Cursor|null  $cursor
     * @param  array  $options (path, query, fragment, pageName)
     * @return void
     */
    public function __construct($items, $perPage, $cursor = null, array $options = [])
    {
        $this->options = $options;


        $this->perPage = $perPage;
        $this->cursor = $cursor;
        $this->path = $this->path !== '/' ? rtrim($this->path, '/') : $this->path;

        $this->setItems($items);
    }

    public function setItems($items)
    {
        $this->items = $items instanceof ItemCollection ? $items : ItemCollection::make($items);
    }

    /**
     * Determine if there are enough items to split into multiple pages.
     *
     * @return bool
     */
    public function hasPages()
    {
        return $this->items->isNotEmpty();
    }

    /**
     * Has next pages
     *
     * @return bool
     */
    public function hasMorePages()
    {
        return $this->items->hasNextItems();
    }

    /**
     * Determine if the paginator is on the first page.
     *
     * @return bool
     */
    public function onFirstPage()
    {
        return empty($this->cursor->parameter('p')) && empty($this->cursor->parameter('n'));
    }

    /**
     * Get the "cursor" that points to the previous set of items.
     *
     * @return \Illuminate\Pagination\Cursor|null
     */
    public function previousCursor()
    {
        if ($this->onFirstPage()) {
            return null;
        }

        return CursorStorage::make($this->cursor)->previousCursorObject();
    }

    /**
     * Get the "cursor" that points to the next set of items.
     *
     * @return \Illuminate\Pagination\Cursor|null
     */
    public function nextCursor()
    {
        if ($this->hasMorePages()) {
            return CursorStorage::make($this->cursor)->nextCursorObject($this->items->getLastEvaluatedKey());
        }

        return null;
    }

    /**
     * Render the paginator using the given view.
     *
     * @param  string|null  $view
     * @param  array  $data
     * @return \Illuminate\Contracts\Support\Htmlable
     */
    public function links($view = null, $data = [])
    {
        return $this->render($view, $data);
    }

    /**
     * Render the paginator using a given view.
     *
     * @param  string|null  $view
     * @param  array  $data
     * @return \Illuminate\Contracts\Support\Htmlable
     */
    public function render($view = null, $data = [])
    {
        return static::viewFactory()->make($view ?: Paginator::$defaultSimpleView, array_merge($data, [
            'paginator' => $this,
        ]));
    }

    /**
     * Get the instance as an array.
     *
     * @return array
     */
    public function toArray()
    {
        return [
            'data' => $this->items->toArray(),
            'path' => $this->path(),
            'per_page' => $this->perPage(),
            'next_page_url' => $this->nextPageUrl(),
            'prev_page_url' => $this->previousPageUrl(),
        ];
    }

    /**
     * Convert the object into something JSON serializable.
     *
     * @return array
     */
    public function jsonSerialize()
    {
        return $this->toArray();
    }

    /**
     * Convert the object to its JSON representation.
     *
     * @param  int  $options
     * @return string
     */
    public function toJson($options = 0)
    {
        return json_encode($this->jsonSerialize(), $options);
    }
}
