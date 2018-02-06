const carmenCache = require('../index.js');
const tape = require('tape');
const fs = require('fs');

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

tape('write/dump', (assert) => {
    const cache = new carmenCache.NormalizationCache(file, false);

    const map = [];
    for (const key of Object.keys(norm).sort()) {
        const val = (Array.isArray(norm[key]) ? norm[key] : [norm[key]]).map((x) => { return words.indexOf(x); }).sort();
        map.push([words.indexOf(key), val]);
    }

    // These tests just illustrate what the mapping actually is storing.
    // Note the mapping is based on index position of sorted text based
    // on how dawg-cache stores.
    assert.deepEqual(words, [
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
    assert.deepEqual(map[0], [3, [2, 3]], 'burbarg => [buerbarg, burbarg]');
    assert.deepEqual(map[1], [4, [0]], 'first street => 1st st');
    assert.deepEqual(map[2], [6, [5]], 'frank boulevard => frank blvd');
    assert.deepEqual(map[3], [9, [8]], 'pear avenue => pear ave');

    cache.writeBatch(map);

    assert.deepEqual(map, cache.getAll(), 'dumped contents match input');

    // test some invalid input
    assert.throws(() => { cache.writeBatch(); });
    assert.throws(() => { cache.writeBatch(7); });
    assert.throws(() => { cache.writeBatch([7]); });
    assert.throws(() => { cache.writeBatch([[7]]); });
    assert.throws(() => { cache.writeBatch([[7, 'asdf']]); });

    return assert.end();
});

tape('read', (assert) => {
    const cache = new carmenCache.NormalizationCache(file, true);

    assert.deepEqual(cache.get(words.indexOf('first street')), [words.indexOf('1st st')]);
    assert.equal(cache.get(8888), undefined);

    const firstWithPrefix = function(p) {
        const f = [];
        for (let i = 0; i < words.length; i++) if (words[i].startsWith(p)) return i;
    };

    const countWithPrefix = function(p) {
        let c = 0;
        for (let i = 0; i < words.length; i++) if (words[i].startsWith(p)) c++;
        return c;
    };

    assert.deepEqual(cache.getPrefixRange(firstWithPrefix('f'), countWithPrefix('f')), [words.indexOf('1st st')], 'found normalization for 1st st but not frank boulevard');
    assert.deepEqual(cache.getPrefixRange(firstWithPrefix('frank'), countWithPrefix('frank')), [], 'found nothing because all normalizations share the searched prefix');
    assert.deepEqual(cache.getPrefixRange(firstWithPrefix('frank bo'), countWithPrefix('frank bo')), [words.indexOf('frank blvd')], 'found frank boulevard because no prefixes are shared');

    assert.deepEqual(cache.getPrefixRange(firstWithPrefix('bu'), countWithPrefix('bu')), [], 'found nothing because all normalizations share the searched prefix');
    assert.deepEqual(cache.getPrefixRange(firstWithPrefix('bue'), countWithPrefix('bue')), [], 'found nothing because bue... doesn\'t normalize to anything');
    assert.deepEqual(cache.getPrefixRange(firstWithPrefix('bur'), countWithPrefix('bur')), [words.indexOf('buerbarg')], 'found buerbarg but not burbarg because burbarg shares a prefix with itself');
    assert.deepEqual(cache.get(firstWithPrefix('bur')), [words.indexOf('buerbarg'), words.indexOf('burbarg')], 'found buerbarg and burbarg with regular get because nothing gets filtered');

    // test some invalid input
    assert.throws(() => { new carmenCache.NormalizationCache(); });
    assert.throws(() => { new carmenCache.NormalizationCache(7); });
    assert.throws(() => { new carmenCache.NormalizationCache('asdf', 7); });

    assert.throws(() => { new carmenCache.NormalizationCache('/proc', true); });


    assert.throws(() => { cache.get(); });
    assert.throws(() => { cache.getPrefixRange('asdf'); });

    assert.throws(() => { cache.getPrefixRange(); });
    assert.throws(() => { cache.getPrefixRange('asdf'); });
    assert.throws(() => { cache.getPrefixRange('asdf', 'asdf'); });
    assert.throws(() => { cache.getPrefixRange(1, 'asdf'); });
    assert.throws(() => { cache.getPrefixRange(1, 1, 'asdf'); });
    assert.throws(() => { cache.getPrefixRange(1, 1, 1, 'asdf'); });

    assert.end();
});
