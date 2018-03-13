'use strict';
const carmenCache = require('../index.js');
const Cache = carmenCache.MemoryCache;
const RocksDBCache = carmenCache.RocksDBCache;
const tape = require('tape');
const fs = require('fs');

const tmpdir = '/tmp/temp.' + Math.random().toString(36).substr(2, 5);
fs.mkdirSync(tmpdir);
let tmpidx = 0;
const tmpfile = function() { return tmpdir + '/' + (tmpidx++) + '.dat'; };

tape('#merge concat', (assert) => {
    const cacheA = new Cache('a');
    cacheA._set('....1', [0,1,2,3]);
    cacheA._set('....2', [0,1,2,3]);

    const cacheB = new Cache('b');
    cacheB._set('....1', [10,11,12,13]);
    cacheB._set('....3', [10,11,12,13]);

    const pbfA = tmpfile();
    cacheA.pack(pbfA, 'term');
    const pbfB = tmpfile();
    cacheB.pack(pbfB, 'term');

    const merged = tmpfile();
    RocksDBCache.merge(pbfA, pbfB, merged, 'concat', (err) => {
        assert.ifError(err);
        const cacheC = new RocksDBCache('c', merged);
        assert.deepEqual(cacheC._get('....2').sort(numSort), [0,1,2,3], 'a-only');
        assert.deepEqual(cacheC._get('....3').sort(numSort), [10,11,12,13], 'b-only');
        assert.deepEqual(cacheC._get('....1').sort(numSort), [0,1,2,3,10,11,12,13], 'a-b-merged');
        assert.end();
    });
});

tape('#merge freq', (assert) => {
    const cacheA = new Cache('a');
    // these would not ordinarily all be in the same shard, but force them to be
    // in the same shard to make this test actually work
    cacheA._set('__MAX__', [1]);
    cacheA._set('__COUNT__', [1]);
    cacheA._set('3', [1]);

    const cacheB = new Cache('b');
    cacheB._set('__MAX__', [2]);
    cacheB._set('__COUNT__', [2]);
    cacheB._set('4', [2]);

    const pbfA = tmpfile();
    cacheA.pack(pbfA);
    const pbfB = tmpfile();
    cacheB.pack(pbfB);

    const merged = tmpfile();
    RocksDBCache.merge(pbfA, pbfB, merged, 'freq', (err) => {
        assert.ifError(err);
        const cacheC = new RocksDBCache('c', merged);
        assert.deepEqual(cacheC._get('__MAX__').sort(numSort), [2], 'a-b-max');
        assert.deepEqual(cacheC._get('__COUNT__').sort(numSort), [3], 'a-b-sum');
        assert.deepEqual(cacheC._get('3').sort(numSort), [1], 'a-only');
        assert.deepEqual(cacheC._get('4').sort(numSort), [2], 'b-only');
        assert.end();
    });
});

function numSort(a, b) { return a - b; }
