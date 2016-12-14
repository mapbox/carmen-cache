var Cache = require('../index.js').Cache;
var tape = require('tape');
var fs = require('fs');
var mp53 = Math.pow(2,53);

var tmpdir = "/tmp/temp." + Math.random().toString(36).substr(2, 5);
fs.mkdirSync(tmpdir);
var tmpidx = 0;
var tmpfile = function() { return tmpdir + "/" + (tmpidx++) + ".dat"; };

tape('#list', function(assert) {
    var cache = new Cache('a', 1);
    cache._set('term', '5', [0,1,2]);
    assert.deepEqual(cache.list('term'), ['5']);
    assert.end();
});

tape('#has', function(assert) {
    var cache = new Cache('a', 1);

    cache._set('term', 'abc', [0,1,2]);
    assert.deepEqual(cache.has('term'), true, 'has term');

    assert.end();
});

tape('#get + #set', function(assert) {
    var cache = new Cache('a', 1);

    for (var i = 0; i < 5; i++) {
        var id = Math.random().toString(36).replace(/[^a-z]+/g, '').substr(0, 5);
        assert.deepEqual(cache._get('term', id), undefined, id + ' not set');
        cache._set('term', id, [0,1,2]);
        assert.deepEqual(cache._get('term', id), [0, 1, 2], id + ' set to 0,1,2');
        cache._set('term', id, [3,4,5]);
        assert.deepEqual(cache._get('term', id), [3, 4, 5], id + ' set to 3,4,5');
        cache._set('term', id, [6,7,8], true);
        assert.deepEqual(cache._get('term', id), [3, 4, 5, 6, 7, 8], id + ' set to 3,4,5,6,7,8');
    }

    assert.end();
});

tape('#pack', function(assert) {
    var cache = new Cache('a', 1);
    cache._set('term', '5', [0,1,2]);
    b = tmpfile();
    cache.pack(b, 'term');
    //assert.deepEqual(cache.pack('term', 0).length, 2065);
    // set should replace data
    cache._set('term', '5', [0,1,2,4]);
    //assert.deepEqual(cache.pack('term', 0).length, 2066);
    assert.throws(cache._set.bind(null, 'term', 5, []), 'can\'t set empty term');

    // fake data
    var array = [];
    for (var i=0;i<10000;++i) array.push(0);

    // now test packing data created via load
    var packer = new Cache('a', 1);
    packer._set('term', '5', array);
    packer._set('term', '6', array);

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
    var directLoad = tmpfile();
    packer.pack(directLoad, 'term')
    loader.loadSync(directLoad, 'term');
    //assert.deepEqual(loader.pack('term', 0).length, 12063);
    assert.deepEqual(loader._get('term', '5'), array);
    assert.deepEqual(loader._get('term', '6'), array);

    assert.end();
});

tape('#load', function(assert) {
    var cache = new Cache('a', 1);
    assert.equal(cache.id, 'a');

    assert.equal(cache._get('term', '5'), undefined);
    assert.deepEqual(cache.list('term'), []);

    // invalid args
    assert.throws(function() { cache.list() });
    assert.throws(function() { cache.list(1) });

    cache._set('term', '5', [0,1,2]);
    assert.deepEqual(cache._get('term', '5'), [0,1,2]);
    assert.deepEqual(cache.list('term'), ['5']);

    cache._set('term', '21', [5,6]);
    assert.deepEqual(cache._get('term', '21'), [5,6]);
    assert.deepEqual(cache.list('term'), ['21', '5'], 'keys in type');

    // cache A serializes data, cache B loads serialized data.
    var pack = tmpfile();
    cache.pack(pack, 'term');
    var loader = new Cache('b', 1);
    loader.loadSync(pack, 'term');
    assert.deepEqual(loader._get('term', '21'), [6,5]);
    assert.deepEqual(loader.list('term'), ['21', '5'], 'keys in shard');
    assert.end();
});

tape('#unload on empty data', function(assert) {
    var cache = new Cache('a', 1);
    assert.equal(cache.unload('term'), false);
    assert.deepEqual(cache.has('term'), false);
    assert.end();
});

tape('#unload after set', function(assert) {
    var cache = new Cache('a', 1);
    cache._set('term', '0', [0,1,2]);
    assert.deepEqual(cache.has('term'), true);
    assert.equal(cache.unload('term'), true);
    assert.deepEqual(cache.has('term'), false);
    assert.end();
});

tape('#unload after load', function(assert) {
    var cache = new Cache('a', 1);
    var array = [];
    for (var i=0;i<10000;++i) {
        array.push(0);
    }
    cache._set('term', '5', array);
    var pack = tmpfile();
    cache.pack(pack, 'term');
    var loader = new Cache('b', 1);
    loader.loadSync(pack, 'term');
    assert.deepEqual(loader._get('term', '5'), array);
    assert.deepEqual(loader.list('term'), ['5'], 'keys in type');
    assert.deepEqual(loader.has('term'), true);
    assert.equal(loader.unload('term'), true);
    assert.deepEqual(loader.has('term'), false);
    assert.end();
});
