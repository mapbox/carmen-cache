# JSCache

Creates an in-memory key-value store mapping phrases  and language IDs
to lists of corresponding grids (grids ie are integer representations of occurrences of the phrase within an index)

**Parameters**

-   `id` **[String](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/String)**
-   `filename` **[String](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/String)**

**Examples**

```javascript
const cache = require('@mapbox/carmen-cache');
const JSCache = new cache.JSCache('a', 'filename');
```

Returns **[Object](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Object)**

# get

Retrieves data exactly matching phrase and language settings by id

**Parameters**

-   `id` **[String](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/String)**
-   `optional` **[Array](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Array)** ; array of languages

**Examples**

```javascript
const cache = require('@mapbox/carmen-cache');
const JSCache = new cache.JSCache('a');

JSCache.get(id, languages);
 // => [grid, grid, grid, grid... ]
```

Returns **[Array](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Array)** integers referring to grids

# get

Retrieves grid that at least partially matches phrase and/or language inputs

**Parameters**

-   `id` **[String](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/String)**
-   `matches_prefix` **[Number](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Number)** whether or do an exact match (0), prefix scan(1), or word boundary scan(2); used for autocomplete
-   `optional` **[Array](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Array)** ; array of languages

**Examples**

```javascript
const cache = require('@mapbox/carmen-cache');
const JSCache = new cache.JSCache('a');

JSCache.get(id, languages);
 // => [grid, grid, grid, grid... ]
```

Returns **[Array](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Array)** integers referring to grids

# pack

Writes an identical copy JSCache from another JSCache; not really used

**Parameters**

-   `String`  , filename

**Examples**

```javascript
const cache = require('@mapbox/carmen-cache');
const JSCache = new cache.JSCache('a');

cache.pack('filename');
```

Returns **[Boolean](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Boolean)**

# list

lists the keys in the JSCache object

**Parameters**

-   `id` **[String](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/String)**

**Examples**

```javascript
const cache = require('@mapbox/carmen-cache');
const JSCache = new cache.JSCache('a');

cache.list('a', (err, result) => {
   if (err) throw err;
   console.log(result);
});
```

Returns **[Array](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Array)** Set of keys/ids

# MemoryCache

Creates an in-memory key-value store mapping phrases  and language IDs
to lists of corresponding grids (grids ie are integer representations of occurrences of the phrase within an index)

**Parameters**

-   `id` **[String](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/String)**

**Examples**

```javascript
const cache = require('@mapbox/carmen-cache');
const MemoryCache = new cache.MemoryCache(id, languages);
```

Returns **[Array](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Array)** grid of integers

# set

Replaces or appends the data for a given key

**Parameters**

-   `id` **[String](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/String)**
-   `data` **[Array](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Array)** an array of numbers where each number represents a grid
-   `langfield_type` **[Array](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Array)** an array of relevant languages
-   `T` **[Boolean](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Boolean)** : append to data, F: replace data

**Examples**

```javascript
const cache = require('@mapbox/carmen-cache');
const MemoryCache = new cache.MemoryCache('a');

cache.set('a', [1,2,3], (err, result) => {
     if (err) throw err;
     console.log(result)
});
```

Returns **** undefined

# PhrasematchSubqObject

The PhrasematchSubqObject type describes the metadata known about possible matches to be assessed for stacking by
coalesce as seen from Javascript. Note: it is of similar purpose to the PhrasematchSubq C++ struct type, but differs
slightly in specific field names and types.

**Properties**

-   `phrase` **[String](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/String)** The matched string
-   `weight` **[Number](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Number)** A float between 0 and 1 representing how much of the query this string covers
-   `prefix` **[Number](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Number)** whether or do an exact match (0), prefix scan(1), or word boundary scan(2); used for autocomplete
-   `idx` **[Number](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Number)** an identifier of the index the match came from; opaque to carmen-cache but returned in results
-   `zoom` **[Number](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Number)** the configured tile zoom level for the index
-   `mask` **[Number](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Number)** a bitmask representing which tokens in the original query the subquery covers
-   `languages` **[Array](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Array)&lt;[Number](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Number)>** a list of the language IDs to be considered matching
-   `cache` **[Object](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Object)** the carmen-cache from the index in which the match was found

# coalesceCallback

**Parameters**

-   `err`  error if any, or null if not
-   `results` **[Array](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Array)&lt;CoalesceResult>** the results of the coalesce operation

# CoalesceResult

A member of the result set from a coalesce operation.

**Properties**

-   `x` **[Number](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Number)** the X tile coordinate of the result
-   `y` **[Number](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Number)** the Y tile coordinate of the result
-   `relev` **[Number](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Number)** the computed relevance of the result
-   `score` **[Number](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Number)** the computed score of the result
-   `id` **[Number](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Number)** the feature ID of the result
-   `idx` **[Number](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Number)** the index ID (preserved from the inbound subquery) of the index the result came from
-   `tmpid` **[Number](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Number)** a composite ID used for uniquely identifying features across indexes that incorporates the ID and IDX
-   `distance` **[Number](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Number)** the distance metric computed using the feature and proximity, if supplied; 0 otherwise
-   `scoredist` **[Number](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Number)** the composite score incorporating the feature's score with the distance (or the score if distance is 0)
-   `matches_language` **[Boolean](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Boolean)** whether or not the match is valid for one of the languages in the inbound languages array

# coalesce

The coalesce function determines whether or not phrase matches in different
carmen indexes align spatially, and computes information about successful matches
such as combined relevance and score. The computation is done on the thread pool,
and exposed asynchronously to JS via a callback argument.

**Parameters**

-   `phrasematches` **[Array](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Array)&lt;PhrasematchSubqObject>** an array of PhrasematchSubqObject objects, each of which describes a match candidate
-   `options` **[Object](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Object)** options for how to perform the coalesce operation that aren't specific to a particular subquery
    -   `options.radius` **\[[Number](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Number)]** the fall-off radius for determining how wide-reaching the effect of proximity bias is
    -   `options.centerzxy` **\[[Array](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Array)&lt;[Number](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Number)>]** a 3-number array representing the ZXY of the tile on which the proximity point can be found
    -   `options.bboxzxy` **\[[Array](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Array)&lt;[Number](https://developer.mozilla.org/docs/Web/JavaScript/Reference/Global_Objects/Number)>]** a 5-number array representing the zoom, minX, minY, maxX, and maxY values of the tile cover of the requested bbox, if any
-   `callback` **coalesceCallback** the callback function
