var Cache = require('../index.js').Cache;
var tape = require('tape');
var fs = require('fs');
var mp53 = Math.pow(2,53);

Cache.prototype.set = function(type, key, data) {
    this._set(type, Cache.shard(key), key, data);
}

Cache.prototype.get = function(type, key) {
    return this._get(type, Cache.shard(key), key);
}

tape('#merge concat', function(assert) {
    var cacheA = new Cache('a');
    cacheA.set('term', '...1', [0,1,2,3]);
    cacheA.set('term', '...2', [0,1,2,3]);

    var cacheB = new Cache('b');
    cacheB.set('term', '...1', [10,11,12,13]);
    cacheB.set('term', '...3', [10,11,12,13]);

    var pbfA = cacheA.pack('term', +Cache.shard('...'));
    var pbfB = cacheB.pack('term', +Cache.shard('...'));

    var cacheC = new Cache('c');
    cacheA.merge(pbfA, pbfB, 'concat', function(err, merged) {
        assert.ifError(err);
        cacheC.loadSync(merged, 'term', +Cache.shard('...'));
        assert.deepEqual(cacheC.get('term', '...2').sort(numSort), [0,1,2,3], 'a-only');
        assert.deepEqual(cacheC.get('term', '...3').sort(numSort), [10,11,12,13], 'b-only');
        assert.deepEqual(cacheC.get('term', '...1').sort(numSort), [0,1,2,3,10,11,12,13], 'a-b-merged');
        assert.end();
    })
});

tape('#merge freq', function(assert) {
    var cacheA = new Cache('a');
    cacheA.set('freq', '...1', [1]);
    cacheA.set('freq', '...2', [1]);
    cacheA.set('freq', '...3', [1]);

    var cacheB = new Cache('b');
    cacheB.set('freq', '...1', [2]);
    cacheB.set('freq', '...2', [2]);
    cacheB.set('freq', '...4', [2]);

    var pbfA = cacheA.pack('freq', +Cache.shard('...'));
    var pbfB = cacheB.pack('freq', +Cache.shard('...'));

    var cacheC = new Cache('c');
    cacheA.merge(pbfA, pbfB, 'freq', function(err, merged) {
        assert.ifError(err);
        cacheC.loadSync(merged, 'freq', +Cache.shard('...'));
        assert.deepEqual(cacheC.get('freq', '...1').sort(numSort), [2], 'a-b-max');
        assert.deepEqual(cacheC.get('freq', '...2').sort(numSort), [3], 'a-b-sum');
        assert.deepEqual(cacheC.get('freq', '...3').sort(numSort), [1], 'a-only');
        assert.deepEqual(cacheC.get('freq', '...4').sort(numSort), [2], 'b-only');
        assert.end();
    })
});

function numSort(a, b) { return a - b; }
