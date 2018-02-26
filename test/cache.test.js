'use strict';
const carmenCache = require('../index.js');
const tape = require('tape');
const fs = require('fs');

// These tests are testing to ensure both MemoryCache and RocksDBCache are identical.
// It loops over each to check that they are the same.
// Check API.md to see what each of these actually tests.

const tmpdir = '/tmp/temp.' + Math.random().toString(36).substr(2, 5);
fs.mkdirSync(tmpdir);
let tmpidx = 0;
const tmpfile = function() { return tmpdir + '/' + (tmpidx++) + '.dat'; };

const sorted = function(arr) {
    return [].concat(arr).sort();
};

const sortedDescending = function(arr) {
    return [].concat(arr).sort((a, b) => { return b - a; });
};

tape('list', (assert) => {
    const cache = new carmenCache.MemoryCache('a');
    cache._set('5', [0,1,2]);
    assert.deepEqual(cache.list().map((x) => { return x[0]; }), ['5']);
    assert.end();
});

tape('get / set / list / pack / load (simple)', (assert) => {
    const cache = new carmenCache.MemoryCache('a');

    const ids = [];
    for (let i = 0; i < 5; i++) {
        const id = Math.random().toString(36).replace(/[^a-z]+/g, '').substr(0, 5);
        ids.push(id);

        assert.deepEqual(cache._get(id), undefined, id + ' not set');
        cache._set(id, [0,1,2]);
        assert.deepEqual(cache._get(id), sortedDescending([0, 1, 2]), id + ' set to 0,1,2');
        cache._set(id, [3,4,5]);
        assert.deepEqual(cache._get(id), sortedDescending([3, 4, 5]), id + ' set to 3,4,5');
        cache._set(id, [6,7,8], null, true);
        assert.deepEqual(cache._get(id), sortedDescending([3, 4, 5, 6, 7, 8]), id + ' set to 3,4,5,6,7,8');
    }

    assert.deepEqual(sorted(cache.list().map((x) => { return x[0]; })), sorted(ids), 'mem ids match');

    const pack = tmpfile();
    cache.pack(pack);
    const loader = new carmenCache.RocksDBCache('b', pack);

    for (let i = 0; i < 5; i++) {
        const id = ids[i];

        assert.deepEqual(cache._get(id), sortedDescending([3, 4, 5, 6, 7, 8]), id + ' set to 3,4,5,6,7,8');
    }

    assert.deepEqual(sorted(loader.list().map((x) => { return x[0]; })), sorted(ids), 'rocks ids match');
    assert.end();
});

tape('get / set / list / pack / load (with lang codes)', (assert) => {
    const cache = new carmenCache.MemoryCache('a');

    const ids = [];
    const expected = [];
    for (let i = 0; i < 5; i++) {
        const id = Math.random().toString(36).replace(/[^a-z]+/g, '').substr(0, 5);
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

    assert.deepEqual(sorted(cache.list().map(JSON.stringify)), sorted(expected.map(JSON.stringify)), 'mem ids and langs match');

    const pack = tmpfile();
    cache.pack(pack);

    for (let i = 0; i < 5; i++) {
        const id = ids[i];

        assert.deepEqual(cache._get(id, [1]), sortedDescending([7, 8, 9]), id + ' for lang [1] set to 7,8,9');
        assert.deepEqual(cache._get(id, [0,1]), sortedDescending([12, 13, 14]), id + ' for lang [0,1] set to 12,13,14');
        assert.false(cache._get(id), id + ' without lang code returns nothing');
        assert.deepEqual(cache._get(id, [0]), sortedDescending([3, 4, 5, 6, 7, 8]), id + ' for lang [0] set to 3,4,5,6,7,8');
        assert.deepEqual(cache._get(id, [0,1]), sortedDescending([12, 13, 14]), id + ' for lang [0,1] still set to 12,13,14');
    }

    assert.deepEqual(sorted(cache.list().map(JSON.stringify)), sorted(expected.map(JSON.stringify)), 'rocks ids and langs match');
    assert.end();
});

tape('pack', (assert) => {
    const cache = new carmenCache.MemoryCache('a');
    cache._set('5', [0,1,2]);
    // set should replace data
    cache._set('5', [0,1,2,4]);
    assert.throws(cache._set.bind(null, '5', []), 'can\'t set empty term');

    // fake data
    const array = [];
    for (let i = 0; i < 10000; ++i) array.push(i);

    // now test packing data created via load
    const packer = new carmenCache.MemoryCache('a');
    packer._set('5', array);
    packer._set('6', array);

    // invalid args
    assert.throws(() => { new carmenCache.RocksDBCache('a'); });
    assert.throws(() => { const loader = new carmenCache.MemoryCache('a'); loader.pack(); });
    assert.throws(() => { const loader = new carmenCache.MemoryCache('a'); loader.pack(1); });
    assert.throws(() => { new carmenCache.RocksDBCache('a', 1); });
    assert.throws(() => { new carmenCache.RocksDBCache('a', null); });
    assert.throws(() => { new carmenCache.RocksDBCache('a', {}); });
    assert.throws(() => { new carmenCache.RocksDBCache('a', new Buffer('a')); });

    // grab data right back out
    const directLoad = tmpfile();
    packer.pack(directLoad);
    const loader = new carmenCache.RocksDBCache('a', directLoad);
    assert.deepEqual(loader._get('5'), sortedDescending(array));
    assert.deepEqual(loader._get('6'), sortedDescending(array));

    // test what happens when you pack a rocksdbcache
    assert.throws(() => { loader.pack(); }, 'filename is required');
    assert.throws(() => { loader.pack(1); }, 'filename must be a string');
    assert.throws(() => { loader.pack(directLoad); }, 'can\'t pack into an already-loaded file');
    assert.ok(() =>  { loader.pack(tmpfile()); }, 'repacking works');

    assert.end();
});
