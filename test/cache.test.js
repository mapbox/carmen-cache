var Cache = require('../index.js').Cache;
var tape = require('tape');
var fs = require('fs');
var mp53 = Math.pow(2,53);

tape('#list', function(assert) {
    var cache = new Cache('a', 1);
    cache._set('term', 0, '5', [0,1,2]);
    assert.deepEqual(cache.list('term'), [0]);
    assert.end();
});

tape('#has', function(assert) {
    var cache = new Cache('a', 1);

    for (var i = 0; i < 5; i++) {
        var shard = Math.floor(Math.random() * mp53);
        assert.deepEqual(cache.has('term', shard), false, shard + ' x');
        cache._set('term', shard, '5', [0,1,2]);
        assert.deepEqual(cache.has('term', shard), true, shard + ' has');
    }

    assert.end();
});

tape('#get + #set', function(assert) {
    var cache = new Cache('a', 1);

    for (var i = 0; i < 5; i++) {
        var id = Math.random().toString(36).replace(/[^a-z]+/g, '').substr(0, 5);
        assert.deepEqual(cache._get('term', 0, id), undefined, id + ' not set');
        cache._set('term', 0, id, [0,1,2]);
        assert.deepEqual(cache._get('term', 0, id), [0, 1, 2], id + ' set to 0,1,2');
        cache._set('term', 0, id, [3,4,5]);
        assert.deepEqual(cache._get('term', 0, id), [3, 4, 5], id + ' set to 3,4,5');
        cache._set('term', 0, id, [6,7,8], true);
        assert.deepEqual(cache._get('term', 0, id), [3, 4, 5, 6, 7, 8], id + ' set to 3,4,5,6,7,8');
    }

    assert.end();
});

tape('#pack', function(assert) {
    var cache = new Cache('a', 1);
    cache._set('term', 0, '5', [0,1,2]);
    b = cache.pack('term', 0);
    assert.deepEqual(cache.pack('term', 0).length, 2065);
    // set should replace data
    cache._set('term', 0, '5', [0,1,2,4]);
    assert.deepEqual(cache.pack('term', 0).length, 2066);
    //cache._set('term', 0, '5', []);
    //assert.deepEqual(cache.pack('term', 0).length, 5);

    // fake data
    var array = [];
    for (var i=0;i<10000;++i) array.push(0);

    // now test packing data created via load
    var packer = new Cache('a', 1);
    packer._set('term', 0, '5', array);
    packer._set('term', 1, '6', array);

    var loader = new Cache('a', 1);

    // invalid args
    assert.throws(function() { loader.pack() });
    assert.throws(function() { loader.pack('term') });
    assert.throws(function() { loader.pack(1) });
    assert.throws(function() { loader.pack('term','foo') });
    assert.throws(function() { loader.loadSync() });
    assert.throws(function() { loader.loadSync(1) });
    assert.throws(function() { loader.loadSync(null) });
    assert.throws(function() { loader.loadSync({}) });
    assert.throws(function() { loader.loadSync(new Buffer('a'),1,'foo') });
    assert.throws(function() { loader.loadSync(new Buffer('a'),'term','foo') });

    // grab data right back out
    loader.loadSync(packer.pack('term',0), 'term', 0);
    assert.deepEqual(loader.pack('term', 0).length, 12063);
    assert.deepEqual(loader._get('term', 0, '5'), array);

    // grab data right back out
    loader.loadSync(packer.pack('term', 1), 'term', 1);
    assert.deepEqual(loader.pack('term', 1).length, 12063);
    assert.deepEqual(loader._get('term', 1, '6'), array);

    // try to grab data that does not exist
    assert.throws(function() { loader.pack('term', 99999999999999) });

    assert.end();
});

tape('#load', function(assert) {
    var cache = new Cache('a', 1);
    assert.equal(cache.id, 'a');

    assert.equal(cache._get('term', 0, '5'), undefined);
    assert.deepEqual(cache.list('term'), []);

    // invalid args
    assert.throws(function() { cache.list() });
    assert.throws(function() { cache.list(1) });

    cache._set('term', 0, '5', [0,1,2]);
    assert.deepEqual(cache._get('term', 0, '5'), [0,1,2]);
    assert.deepEqual(cache.list('term'), [0]);

    cache._set('term', 0, '21', [5,6]);
    assert.deepEqual(cache._get('term', 0, '21'), [5,6]);
    assert.deepEqual(cache.list('term'), [0], 'single shard');
    assert.deepEqual(cache.list('term', 0), ['21', '5'], 'keys in shard');

    // cache A serializes data, cache B loads serialized data.
    var pack = cache.pack('term', 0);
    var loader = new Cache('b', 1);
    loader.loadSync(pack, 'term', 0);
    assert.deepEqual(loader._get('term', 0, '21'), [6,5]);
    assert.deepEqual(loader.list('term'), [0], 'single shard');
    assert.deepEqual(loader.list('term', 0), ['21', '5'], 'keys in shard');
    assert.end();
});

tape('#unload on empty data', function(assert) {
    var cache = new Cache('a', 1);
    assert.equal(cache.unload('term',5), false);
    assert.deepEqual(cache.has('term', 5), false);
    assert.end();
});

tape('#unload after set', function(assert) {
    var cache = new Cache('a', 1);
    cache._set('term', 0, '0', [0,1,2]);
    assert.deepEqual(cache.has('term', 0), true);
    assert.equal(cache.unload('term',0), true);
    assert.deepEqual(cache.has('term', 0), false);
    assert.end();
});

tape('#unload after load', function(assert) {
    var cache = new Cache('a', 1);
    var array = [];
    for (var i=0;i<10000;++i) {
        array.push(0);
    }
    cache._set('term', 0, '5', array);
    var pack = cache.pack('term', 0);
    var loader = new Cache('b', 1);
    loader.loadSync(pack, 'term', 0);
    assert.deepEqual(loader._get('term', 0, '5'), array);
    assert.deepEqual(loader.list('term'), [0], 'single shard');
    assert.deepEqual(loader.has('term', 0), true);
    assert.equal(loader.unload('term',0), true);
    assert.deepEqual(loader.has('term', 0), false);
    assert.end();
});
