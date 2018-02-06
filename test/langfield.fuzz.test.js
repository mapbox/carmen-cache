'use strict';
const carmenCache = require('../index.js');
const tape = require('tape');
const fs = require('fs');

const tmpdir = '/tmp/temp.' + Math.random().toString(36).substr(2, 5);
fs.mkdirSync(tmpdir);
let tmpidx = 0;
const tmpfile = function() { return tmpdir + '/' + (tmpidx++) + '.dat'; };

tape('language fuzzing', (assert) => {
    const cache = new carmenCache.MemoryCache('a');

    const records = new Map();
    for (let j = 0; j < 1000; j++) {
        const phrase = Math.random().toString(36).substr(2, 3 + Math.floor(Math.random() * 6));
        const numLanguages = Math.floor(Math.random() * 4);
        let languages;
        if (numLanguages === 0) {
            languages = null;
        } else {
            languages = new Set();
            for (let i = 0; i < numLanguages; i++) {
                languages.add(Math.floor(Math.random() * 128));
            }
            languages = Array.from(languages).sort((a, b) => { return a - b; });
        }
        const recordId = phrase + '-' + (languages == null ? 'null' : languages.join('-'));
        records.set(recordId, { phrase: phrase, languages: languages });

        cache._set(phrase, [1], languages);
    }

    let list = cache.list();
    assert.equal(list.length, records.size, 'got the same number of items out as went in');
    let hasAll = true;
    for (const item of list) {
        const recordId = item[0] + '-' + (item[1] == null ? 'null' : item[1].join('-'));
        hasAll = hasAll && records.has(recordId);
    }
    assert.ok(hasAll, 'all records and languages came out that went in');

    const pack = tmpfile();
    cache.pack(pack);
    const loader = new carmenCache.RocksDBCache('b', pack);

    list = loader.list();
    assert.equal(list.length, records.size, 'got the same number of items out as went in');
    hasAll = true;
    for (const item of list) {
        const recordId = item[0] + '-' + (item[1] == null ? 'null' : item[1].join('-'));
        hasAll = hasAll && records.has(recordId);
    }
    assert.ok(hasAll, 'all records and languages came out that went in');

    assert.end();
});
