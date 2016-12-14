var Cache = require('../index.js').Cache;
var tape = require('tape');
var fs = require('fs');
var mp53 = Math.pow(2,53);

var tmpdir = "/tmp/temp." + Math.random().toString(36).substr(2, 5);
fs.mkdirSync(tmpdir);
var tmpidx = 0;
var tmpfile = function() { return tmpdir + "/" + (tmpidx++) + ".dat"; };

Cache.prototype.set = function(type, key, data) {
    this._set(type, Cache.shard(key), key, data);
}

Cache.prototype.get = function(type, key) {
    return this._get(type, Cache.shard(key), key);
}

tape('#merge concat', function(assert) {
    var cacheA = new Cache('a');
    cacheA.set('term', '....1', [0,1,2,3]);
    cacheA.set('term', '....2', [0,1,2,3]);

    var cacheB = new Cache('b');
    cacheB.set('term', '....1', [10,11,12,13]);
    cacheB.set('term', '....3', [10,11,12,13]);

    var pbfA = tmpfile();
    cacheA.pack('term', +Cache.shard('....'), pbfA);
    var pbfB = tmpfile();
    cacheB.pack('term', +Cache.shard('....'), pbfB);

    var merged = tmpfile();
    var cacheC = new Cache('c');
    cacheA.merge(pbfA, pbfB, merged, 'concat', function(err) {
        assert.ifError(err);
        cacheC.loadSync(merged, 'term', +Cache.shard('....'));
        assert.deepEqual(cacheC.get('term', '....2').sort(numSort), [0,1,2,3], 'a-only');
        assert.deepEqual(cacheC.get('term', '....3').sort(numSort), [10,11,12,13], 'b-only');
        assert.deepEqual(cacheC.get('term', '....1').sort(numSort), [0,1,2,3,10,11,12,13], 'a-b-merged');
        assert.end();
    })
});

tape('#merge freq', function(assert) {
    var cacheA = new Cache('a');
    // these would not ordinarily all be in the same shard, but force them to be
    // in the same shard to make this test actually work
    cacheA._set('freq', 0, '__MAX__', [1]);
    cacheA._set('freq', 0, '__COUNT__', [1]);
    cacheA._set('freq', 0, '3', [1]);

    var cacheB = new Cache('b');
    cacheB._set('freq', 0, '__MAX__', [2]);
    cacheB._set('freq', 0, '__COUNT__', [2]);
    cacheB._set('freq', 0, '4', [2]);

    var pbfA = tmpfile();
    cacheA.pack('freq', 0, pbfA);
    var pbfB = tmpfile();
    cacheB.pack('freq', 0, pbfB);

    var merged = tmpfile();
    var cacheC = new Cache('c');
    cacheA.merge(pbfA, pbfB, merged, 'freq', function(err) {
        assert.ifError(err);
        cacheC.loadSync(merged, 'freq', 0);
        assert.deepEqual(cacheC._get('freq', 0, '__MAX__').sort(numSort), [2], 'a-b-max');
        assert.deepEqual(cacheC._get('freq', 0, '__COUNT__').sort(numSort), [3], 'a-b-sum');
        assert.deepEqual(cacheC._get('freq', 0, '3').sort(numSort), [1], 'a-only');
        assert.deepEqual(cacheC._get('freq', 0, '4').sort(numSort), [2], 'b-only');
        assert.end();
    })
});

// tape('#merge invalid pbf throws JS error', function(assert) {
//     var cacheA = new Cache('a');
//     cacheA.merge(new Buffer("phony protobuf"), new Buffer("phony protobuf"), 'freq', function(err, merged) {
//         assert.ok(err);
//         if (err) {
//             assert.ok(err.message.indexOf("unknown pbf field type exception") > -1);
//         }
//         assert.end();
//     })
// });

function numSort(a, b) { return a - b; }
