var carmenCache = require('../index.js');
var tape = require('tape');
var fs = require('fs');
var mp53 = Math.pow(2,53);

var tmpdir = "/tmp/temp." + Math.random().toString(36).substr(2, 5);
fs.mkdirSync(tmpdir);
var tmpidx = 0;
var tmpfile = function() { return tmpdir + "/" + (tmpidx++) + ".dat"; };

tape('#list', function(assert) {
    var cache = new carmenCache.MemoryCache('a');
    cache._set('5', [0,1,2]);
    assert.deepEqual(cache.list(), ['5']);
    assert.end();
});

tape('#get + #set', function(assert) {
    var cache = new carmenCache.MemoryCache('a');

    for (var i = 0; i < 5; i++) {
        var id = Math.random().toString(36).replace(/[^a-z]+/g, '').substr(0, 5);
        assert.deepEqual(cache._get(id), undefined, id + ' not set');
        cache._set(id, [0,1,2]);
        assert.deepEqual(cache._get(id), [0, 1, 2], id + ' set to 0,1,2');
        cache._set(id, [3,4,5]);
        assert.deepEqual(cache._get(id), [3, 4, 5], id + ' set to 3,4,5');
        cache._set(id, [6,7,8], true);
        assert.deepEqual(cache._get(id), [3, 4, 5, 6, 7, 8], id + ' set to 3,4,5,6,7,8');
    }

    assert.end();
});

tape('#pack', function(assert) {
    var cache = new carmenCache.MemoryCache('a');
    cache._set('5', [0,1,2]);
    // set should replace data
    cache._set('5', [0,1,2,4]);
    assert.throws(cache._set.bind(null, '5', []), 'can\'t set empty term');

    // fake data
    var array = [];
    for (var i=0;i<10000;++i) array.push(0);

    // now test packing data created via load
    var packer = new carmenCache.MemoryCache('a');
    packer._set('5', array);
    packer._set('6', array);

    // invalid args
    assert.throws(function() { var loader = new carmenCache.RocksDBCache('a'); });
    assert.throws(function() { var loader = new carmenCache.MemoryCache('a'); loader.pack(1); });
    assert.throws(function() { var loader = new carmenCache.RocksDBCache('a', 1); });
    assert.throws(function() { var loader = new carmenCache.RocksDBCache('a', null); });
    assert.throws(function() { var loader = new carmenCache.RocksDBCache('a', {}); });
    assert.throws(function() { var loader = new carmenCache.RocksDBCache('a', new Buffer('a')); });

    // grab data right back out
    var directLoad = tmpfile();
    packer.pack(directLoad)
    var loader = new carmenCache.RocksDBCache('a', directLoad);
    assert.deepEqual(loader._get('5'), array);
    assert.deepEqual(loader._get('6'), array);

    assert.end();
});

tape('#load', function(assert) {
    var cache = new carmenCache.MemoryCache('a');
    assert.equal(cache.id, 'a');

    assert.equal(cache._get('5'), undefined);
    assert.deepEqual(cache.list(), []);

    cache._set('5', [0,1,2]);
    assert.deepEqual(cache._get('5'), [0,1,2]);
    assert.deepEqual(cache.list(), ['5']);

    cache._set('21', [5,6]);
    assert.deepEqual(cache._get('21'), [5,6]);
    assert.deepEqual(cache.list(), ['21', '5'], 'keys in type');

    // cache A serializes data, cache B loads serialized data.
    var pack = tmpfile();
    cache.pack(pack);
    var loader = new carmenCache.RocksDBCache('b', pack);
    assert.deepEqual(loader._get('21'), [6,5]);
    assert.deepEqual(loader.list(), ['21', '5'], 'keys in shard');
    assert.end();
});

tape('#dot suffix', function(assert) {
    var cache = new carmenCache.MemoryCache('mem');

    cache._set('test', [2,1,0]);
    cache._set('test.', [7,6,5,4]);
    cache._set('something', [4,3,2]);
    cache._set('else.', [9,8,7]);

    // cache A serializes data, cache B loads serialized data.
    var pack = tmpfile();
    cache.pack(pack);
    var loader = new carmenCache.RocksDBCache('packed', pack);

    [cache, loader].forEach(function(c) {
        assert.deepEqual(c._get('test'), [2,1,0], 'exact get without dot where both exist for ' + c.id);
        assert.deepEqual(c._get('test.'), [7,6,5,4], 'exact get with dot where both exist for ' + c.id);
        assert.deepEqual(c._get('something'), [4,3,2], 'exact get without dot where non-dot exists for ' + c.id);
        assert.deepEqual(c._get('something.'), undefined, 'exact get with dot where non-dot exists for ' + c.id);
        assert.deepEqual(c._get('else'), undefined, 'exact get without dot where dot exists for ' + c.id);
        assert.deepEqual(c._get('else.'), [9,8,7], 'exact get with dot where dot exists for ' + c.id);

        assert.deepEqual(c._get('test', true), [7,6,5,4,2,1,0], 'ignore-prefix get where both exist for ' + c.id);
        assert.deepEqual(c._get('something', true), [4,3,2], 'ignore-prefix get where non-dot exists for ' + c.id);
        assert.deepEqual(c._get('else', true), [9,8,7], 'ignore-prefix get where dot exists for ' + c.id);

        assert.deepEqual(c._getByPrefix('te'), [2,1,0], 'partial getbyprefix where both exist for ' + c.id);
        assert.deepEqual(c._getByPrefix('test'), [7,6,5,4,2,1,0], 'complete getbyprefix where both exist for ' + c.id);
        assert.deepEqual(c._getByPrefix('so'), [4,3,2], 'partial getbyprefix where non-dot exists for ' + c.id);
        assert.deepEqual(c._getByPrefix('something'), [4,3,2], 'complete getbyprefix where non-dot exists for ' + c.id);
        assert.deepEqual(c._getByPrefix('el'), undefined, 'partial getbyprefix where dot exists for ' + c.id);
        assert.deepEqual(c._getByPrefix('else'), [9,8,7], 'complete getbyprefix where dot exists for ' + c.id);
    });

    assert.deepEqual(loader.list('grid'), [ 'else.', 'something', 'test', 'test.' ], 'keys in shard');
    assert.end();
});