var carmenCache = require('../index.js');
var tape = require('tape');
var fs = require('fs');
var mp53 = Math.pow(2,53);

var tmpdir = "/tmp/temp." + Math.random().toString(36).substr(2, 5);
fs.mkdirSync(tmpdir);
var tmpidx = 0;
var tmpfile = function() { return tmpdir + "/" + (tmpidx++) + ".dat"; };

var sorted = function(arr) {
    return [].concat(arr).sort();
}

var sortedDescending = function(arr) {
    return [].concat(arr).sort(function (a, b) { return b - a; });
}

/*
tape('list', function(assert) {
    var cache = new carmenCache.MemoryCache('a');
    cache._set('5', [0,1,2]);
    assert.deepEqual(cache.list().map(function(x) { return x[0]; }), ['5']);
    assert.end();
});

tape('get / set / list / pack / load (simple)', function(assert) {
    var cache = new carmenCache.MemoryCache('a');

    var ids = [];
    for (var i = 0; i < 5; i++) {
        var id = Math.random().toString(36).replace(/[^a-z]+/g, '').substr(0, 5);
        ids.push(id);

        assert.deepEqual(cache._get(id), undefined, id + ' not set');
        cache._set(id, [0,1,2]);
        assert.deepEqual(cache._get(id), sortedDescending([0, 1, 2]), id + ' set to 0,1,2');
        cache._set(id, [3,4,5]);
        assert.deepEqual(cache._get(id), sortedDescending([3, 4, 5]), id + ' set to 3,4,5');
        cache._set(id, [6,7,8], null, true);
        assert.deepEqual(cache._get(id), sortedDescending([3, 4, 5, 6, 7, 8]), id + ' set to 3,4,5,6,7,8');
    }

    assert.deepEqual(sorted(cache.list().map(function(x) { return x[0]; })), sorted(ids), "mem ids match");

    var pack = tmpfile();
    cache.pack(pack);
    var loader = new carmenCache.RocksDBCache('b', pack);

    for (var i = 0; i < 5; i++) {
        var id = ids[i];

        assert.deepEqual(cache._get(id), sortedDescending([3, 4, 5, 6, 7, 8]), id + ' set to 3,4,5,6,7,8');
    }

    assert.deepEqual(sorted(loader.list().map(function(x) { return x[0]; })), sorted(ids), "rocks ids match");
    assert.end();
});

tape('get / set / list / pack / load (with lang codes)', function(assert) {
    var cache = new carmenCache.MemoryCache('a');

    var ids = [];
    var expected = [];
    for (var i = 0; i < 5; i++) {
        var id = Math.random().toString(36).replace(/[^a-z]+/g, '').substr(0, 5);
        ids.push(id);

        assert.deepEqual(cache._get(id), undefined, id + ' not set');
        cache._set(id, [0,1,2], [0]);
        cache._set(id, [7,8,9], [1]);
        cache._set(id, [12,13,14], [0,1]);
        assert.deepEqual(cache._get(id, [0]), sortedDescending([0, 1, 2]), id + ' for lang [0] set to 0,1,2');
        assert.deepEqual(cache._get(id, [1]), sortedDescending([7, 8, 9]), id + ' for lang [1] set to 7,8,9');
        assert.deepEqual(cache._get(id, [0,1]), sortedDescending([12, 13, 14]), id + ' for lang [0,1] set to 12,13,14');
        assert.false(cache._get(id), id + ' without lang code returns nothing');
        cache._set(id, [3,4,5], [0]);
        assert.deepEqual(cache._get(id, [0]), sortedDescending([3,4,5]), id + ' for lang [0] set to 3,4,5');
        assert.deepEqual(cache._get(id, [0,1]), sortedDescending([12, 13, 14]), id + ' for lang [0,1] still set to 12,13,14');
        cache._set(id, [6,7,8], [0], true);
        assert.deepEqual(cache._get(id, [0]), sortedDescending([3, 4, 5, 6, 7, 8]), id + ' for lang [0] set to 3,4,5,6,7,8');
        assert.deepEqual(cache._get(id, [0,1]), sortedDescending([12, 13, 14]), id + ' for lang [0,1] still set to 12,13,14');
        assert.false(cache._get(id), id + ' without lang code still returns nothing');

        expected.push([id, [0]]);
        expected.push([id, [1]]);
        expected.push([id, [0,1]]);
    }

    assert.deepEqual(sorted(cache.list().map(JSON.stringify)), sorted(expected.map(JSON.stringify)), "mem ids and langs match");

    var pack = tmpfile();
    cache.pack(pack);
    var loader = new carmenCache.RocksDBCache('b', pack);

    for (var i = 0; i < 5; i++) {
        var id = ids[i];

        assert.deepEqual(cache._get(id, [1]), sortedDescending([7, 8, 9]), id + ' for lang [1] set to 7,8,9');
        assert.deepEqual(cache._get(id, [0,1]), sortedDescending([12, 13, 14]), id + ' for lang [0,1] set to 12,13,14');
        assert.false(cache._get(id), id + ' without lang code returns nothing');
        assert.deepEqual(cache._get(id, [0]), sortedDescending([3, 4, 5, 6, 7, 8]), id + ' for lang [0] set to 3,4,5,6,7,8');
        assert.deepEqual(cache._get(id, [0,1]), sortedDescending([12, 13, 14]), id + ' for lang [0,1] still set to 12,13,14');
    }

    assert.deepEqual(sorted(cache.list().map(JSON.stringify)), sorted(expected.map(JSON.stringify)), "rocks ids and langs match");
    assert.end();
});
*/

tape('pack', function(assert) {
    var Grid = require('./grid.js');
    var cache = new carmenCache.MemoryCache('a');
    cache._set('new york', [0,1,2]);
    // set should replace data
    cache._set('new york', [0,1,2,4]);
    assert.throws(cache._set.bind(null, '5', []), 'can\'t set empty term');

    // fake data
    var ny = [];
    for (var i=0;i<100;++i) {
        ny.push(Grid.encode({
            id: i,
            x: Math.floor(i/100 * Math.pow(2,8)),
            y: Math.floor(i/100 * Math.pow(2,8)),
            relev: 1,
            score: 1
        }));
    }
    var dc = [];
    for (var i=0;i<5;++i) {
        dc.push(Grid.encode({
            id: i,
            x: Math.floor(i/5 * Math.pow(2,8)),
            y: Math.floor(i/5 * Math.pow(2,8)),
            relev: 1,
            score: 1
        }));
    }

    // now test packing data created via load
    var packer = new carmenCache.MemoryCache('a');
    packer._set('new york', ny);
    packer._set('washington', dc);

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
    assert.deepEqual(loader._get('new york'), sortedDescending(ny));
    assert.deepEqual(loader._get('washington'), sortedDescending(dc));

    console.warn(loader.list().map(JSON.stringify));

    assert.end();
});
