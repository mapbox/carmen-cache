var Cache = require('../index.js').Cache;
var tape = require('tape');
var fs = require('fs');
var mp53 = Math.pow(2,53);

tape('#merge concat', function(assert) {
    var cacheA = new Cache('a');
    cacheA._set('term', 0, 1, [0,1,2,3]);
    cacheA._set('term', 0, 2, [0,1,2,3]);

    var cacheB = new Cache('b');
    cacheB._set('term', 0, 1, [10,11,12,13]);
    cacheB._set('term', 0, 3, [10,11,12,13]);

    var pbfA = cacheA.pack('term', 0);
    var pbfB = cacheB.pack('term', 0);

    var cacheC = new Cache('c');
    cacheA.merge(pbfA, pbfB, 'concat', function(err, merged) {
        assert.ifError(err);
        cacheC.loadSync(merged, 'term', 0);
        assert.deepEqual(cacheC._get('term', 0, 2).sort(numSort), [0,1,2,3], 'a-only');
        assert.deepEqual(cacheC._get('term', 0, 3).sort(numSort), [10,11,12,13], 'b-only');
        assert.deepEqual(cacheC._get('term', 0, 1).sort(numSort), [0,1,2,3,10,11,12,13], 'a-b-merged');
        assert.end();
    })
});

tape('#merge freq', function(assert) {
    var cacheA = new Cache('a');
    cacheA._set('freq', 0, 1, [1]);
    cacheA._set('freq', 0, 2, [1]);
    cacheA._set('freq', 0, 3, [1]);

    var cacheB = new Cache('b');
    cacheB._set('freq', 0, 1, [2]);
    cacheB._set('freq', 0, 2, [2]);
    cacheB._set('freq', 0, 4, [2]);

    var pbfA = cacheA.pack('freq', 0);
    var pbfB = cacheB.pack('freq', 0);

    var cacheC = new Cache('c');
    cacheA.merge(pbfA, pbfB, 'freq', function(err, merged) {
        assert.ifError(err);
        cacheC.loadSync(merged, 'freq', 0);
        assert.deepEqual(cacheC._get('freq', 0, 1).sort(numSort), [2], 'a-b-max');
        assert.deepEqual(cacheC._get('freq', 0, 2).sort(numSort), [3], 'a-b-sum');
        assert.deepEqual(cacheC._get('freq', 0, 3).sort(numSort), [1], 'a-only');
        assert.deepEqual(cacheC._get('freq', 0, 4).sort(numSort), [2], 'b-only');
        assert.end();
    })
});

tape('#merge invalid pbf throws JS error', function(assert) {
    var cacheA = new Cache('a');
    cacheA.merge(new Buffer("phony protobuf"), new Buffer("phony protobuf"), 'freq', function(err, merged) {
        assert.ok(err);
        if (err) {
            assert.ok(err.message.indexOf("unknown pbf field type exception") > -1);            
        }
        assert.end();
    })
});

function numSort(a, b) { return a - b; }
