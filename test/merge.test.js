var Cache = require('../index.js').Cache;
var tape = require('tape');
var fs = require('fs');
var mp53 = Math.pow(2,53);

var tmpdir = "/tmp/temp." + Math.random().toString(36).substr(2, 5);
fs.mkdirSync(tmpdir);
var tmpidx = 0;
var tmpfile = function() { return tmpdir + "/" + (tmpidx++) + ".dat"; };

tape('#merge concat', function(assert) {
    var cacheA = new Cache('a');
    cacheA._set('term', '....1', [0,1,2,3]);
    cacheA._set('term', '....2', [0,1,2,3]);

    var cacheB = new Cache('b');
    cacheB._set('term', '....1', [10,11,12,13]);
    cacheB._set('term', '....3', [10,11,12,13]);

    var pbfA = tmpfile();
    cacheA.pack(pbfA, 'term');
    var pbfB = tmpfile();
    cacheB.pack(pbfB, 'term');

    var merged = tmpfile();
    var cacheC = new Cache('c');
    cacheA.merge(pbfA, pbfB, merged, 'concat', function(err) {
        assert.ifError(err);
        cacheC.loadSync(merged, 'term');
        assert.deepEqual(cacheC._get('term', '....2').sort(numSort), [0,1,2,3], 'a-only');
        assert.deepEqual(cacheC._get('term', '....3').sort(numSort), [10,11,12,13], 'b-only');
        assert.deepEqual(cacheC._get('term', '....1').sort(numSort), [0,1,2,3,10,11,12,13], 'a-b-merged');
        assert.end();
    })
});

tape('#merge freq', function(assert) {
    var cacheA = new Cache('a');
    // these would not ordinarily all be in the same shard, but force them to be
    // in the same shard to make this test actually work
    cacheA._set('freq', '__MAX__', [1]);
    cacheA._set('freq', '__COUNT__', [1]);
    cacheA._set('freq', '3', [1]);

    var cacheB = new Cache('b');
    cacheB._set('freq', '__MAX__', [2]);
    cacheB._set('freq', '__COUNT__', [2]);
    cacheB._set('freq', '4', [2]);

    var pbfA = tmpfile();
    cacheA.pack(pbfA, 'freq');
    var pbfB = tmpfile();
    cacheB.pack(pbfB, 'freq');

    var merged = tmpfile();
    var cacheC = new Cache('c');
    cacheA.merge(pbfA, pbfB, merged, 'freq', function(err) {
        assert.ifError(err);
        cacheC.loadSync(merged, 'freq', 0);
        assert.deepEqual(cacheC._get('freq', '__MAX__').sort(numSort), [2], 'a-b-max');
        assert.deepEqual(cacheC._get('freq', '__COUNT__').sort(numSort), [3], 'a-b-sum');
        assert.deepEqual(cacheC._get('freq', '3').sort(numSort), [1], 'a-only');
        assert.deepEqual(cacheC._get('freq', '4').sort(numSort), [2], 'b-only');
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
