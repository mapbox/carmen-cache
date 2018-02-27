'use strict';
const carmenCache = require('../index.js');
const test = require('tape');
const fs = require('fs');

const tmpdir = '/tmp/temp.' + Math.random().toString(36).substr(2, 5);
fs.mkdirSync(tmpdir);
let tmpidx = 0;
const tmpfile = function() { return tmpdir + '/' + (tmpidx++) + '.dat'; };

const words = [
    'first street',
    '1st st',
    'frank blvd',
    'frank boulevard',
    'fred road',
    'apple lane',
    'pear avenue',
    'pear ave',
    'burbarg',
    'buerbarg'
].sort();

const norm = {
    'first street': '1st st',
    'frank boulevard': 'frank blvd',
    'pear avenue': 'pear ave',
    // for inconsistently normalized text, allow storing more than one possible normalization
    'burbarg': ['burbarg', 'buerbarg']
};

const file = tmpfile();

// test creating the cache and writing it to disk
test('write/dump', (t) => {
    const cache = new carmenCache.NormalizationCache(file, false);

    const map = [];
    for (const key of Object.keys(norm).sort()) {
        const val = (Array.isArray(norm[key]) ? norm[key] : [norm[key]]).map((x) => { return words.indexOf(x); }).sort();
        map.push([words.indexOf(key), val]);
    }

    // These tests just illustrate what the mapping actually is storing.
    // Note the mapping is based on index position of sorted text based
    // on how dawg-cache stores.
    t.deepEqual(words, [
        '1st st',
        'apple lane',
        'buerbarg',
        'burbarg',
        'first street',
        'frank blvd',
        'frank boulevard',
        'fred road',
        'pear ave',
        'pear avenue'
    ], 'confirm map sorted order simulating dawg text order');
    t.deepEqual(map[0], [3, [2, 3]], 'burbarg => [buerbarg, burbarg]');
    t.deepEqual(map[1], [4, [0]], 'first street => 1st st');
    t.deepEqual(map[2], [6, [5]], 'frank boulevard => frank blvd');
    t.deepEqual(map[3], [9, [8]], 'pear avenue => pear ave');

    cache.writeBatch(map);

    t.deepEqual(map, cache.getAll(), 'dumped contents match input');

    // test some invalid input
    t.throws(() => { cache.writeBatch(); }, 'throws on invalid arguments');
    t.throws(() => { cache.writeBatch(7); }, 'throws on invalid arguments');
    t.throws(() => { cache.writeBatch([7]); }, 'throws on invalid arguments');
    t.throws(() => { cache.writeBatch([[7]]); }, 'throws on invalid arguments');
    t.throws(() => { cache.writeBatch([[7, 'asdf']]); }, 'throws on invalid arguments');

    return t.end();
});

// tests reading cache from disk
test('read', (t) => {
    const cache = new carmenCache.NormalizationCache(file, true);

    t.deepEqual(cache.get(words.indexOf('first street')), [words.indexOf('1st st')], 'normalization value for "first street" is as expected');
    t.equal(cache.get(8888), undefined, 'returns no value for an index not in the cache');

    const firstWithPrefix = function(p) {
        for (let i = 0; i < words.length; i++) if (words[i].startsWith(p)) return i;
    };

    const countWithPrefix = function(p) {
        let c = 0;
        for (let i = 0; i < words.length; i++) if (words[i].startsWith(p)) c++;
        return c;
    };

    t.deepEqual(cache.getPrefixRange(firstWithPrefix('f'), countWithPrefix('f')), [words.indexOf('1st st')], 'found normalization for 1st st but not frank boulevard');
    t.deepEqual(cache.getPrefixRange(firstWithPrefix('frank'), countWithPrefix('frank')), [], 'found nothing because all normalizations share the searched prefix');
    t.deepEqual(cache.getPrefixRange(firstWithPrefix('frank bo'), countWithPrefix('frank bo')), [words.indexOf('frank blvd')], 'found frank boulevard because no prefixes are shared');

    t.deepEqual(cache.getPrefixRange(firstWithPrefix('bu'), countWithPrefix('bu')), [], 'found nothing because all normalizations share the searched prefix');
    t.deepEqual(cache.getPrefixRange(firstWithPrefix('bue'), countWithPrefix('bue')), [], 'found nothing because bue... doesn\'t normalize to anything');
    t.deepEqual(cache.getPrefixRange(firstWithPrefix('bur'), countWithPrefix('bur')), [words.indexOf('buerbarg')], 'found buerbarg but not burbarg because burbarg shares a prefix with itself');
    t.deepEqual(cache.get(firstWithPrefix('bur')), [words.indexOf('buerbarg'), words.indexOf('burbarg')], 'found buerbarg and burbarg with regular get because nothing gets filtered');

    // test some invalid input
    t.throws(() => { new carmenCache.NormalizationCache(); }, 'throws on invalid arguments');
    t.throws(() => { new carmenCache.NormalizationCache(7); }, 'throws on invalid arguments');
    t.throws(() => { new carmenCache.NormalizationCache('asdf', 7); }, 'throws on invalid arguments');

    t.throws(() => { new carmenCache.NormalizationCache('/proc', true); }, 'throws on invalid arguments');


    t.throws(() => { cache.get(); }, 'throws on invalid arguments');
    t.throws(() => { cache.getPrefixRange('asdf'); }, 'throws on invalid arguments');

    t.throws(() => { cache.getPrefixRange(); }, 'throws on invalid arguments');
    t.throws(() => { cache.getPrefixRange('asdf'); }, 'throws on invalid arguments');
    t.throws(() => { cache.getPrefixRange('asdf', 'asdf'); }, 'throws on invalid arguments');
    t.throws(() => { cache.getPrefixRange(1, 'asdf'); }, 'throws on invalid arguments');
    t.throws(() => { cache.getPrefixRange(1, 1, 'asdf'); }, 'throws on invalid arguments');
    t.throws(() => { cache.getPrefixRange(1, 1, 1, 'asdf'); }, 'throws on invalid arguments');

    t.end();
});
