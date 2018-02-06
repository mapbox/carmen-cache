const carmenCache = require('../index.js');
const tape = require('tape');
const fs = require('fs');
const mp53 = Math.pow(2,53);
const Grid = require('./grid.js');

const tmpdir = '/tmp/temp.' + Math.random().toString(36).substr(2, 5);
fs.mkdirSync(tmpdir);
let tmpidx = 0;
const tmpfile = function() { return tmpdir + '/' + (tmpidx++) + '.dat'; };

const getIds = function(grids) {
    return grids.map((x) => { return x.id; }).sort((a, b) => { return a - b; });
};

const getByLanguageMatch = function(grids, match) {
    return grids.filter((x) => { return x.matches_language == match; });
};

tape('getMatching', (assert) => {
    const cache = new carmenCache.MemoryCache('mem');

    cache._set('test', [
        Grid.encode({ id: 1, x: 1, y: 1, relev: 1, score: 1 }),
        Grid.encode({ id: 2, x: 1, y: 1, relev: 1, score: 1 }),
        Grid.encode({ id: 3, x: 1, y: 1, relev: 1, score: 1 })
    ]);
    cache._set('test', [
        Grid.encode({ id: 11, x: 1, y: 1, relev: 1, score: 1 }),
        Grid.encode({ id: 12, x: 1, y: 1, relev: 1, score: 1 }),
        Grid.encode({ id: 13, x: 1, y: 1, relev: 1, score: 1 })
    ], [0]);
    cache._set('test', [
        Grid.encode({ id: 21, x: 1, y: 1, relev: 1, score: 1 }),
        Grid.encode({ id: 22, x: 1, y: 1, relev: 1, score: 1 }),
        Grid.encode({ id: 23, x: 1, y: 1, relev: 1, score: 1 })
    ], [1]);
    cache._set('test', [
        Grid.encode({ id: 31, x: 1, y: 1, relev: 1, score: 1 }),
        Grid.encode({ id: 32, x: 1, y: 1, relev: 1, score: 1 }),
        Grid.encode({ id: 33, x: 1, y: 1, relev: 1, score: 1 })
    ], [1,2]);
    cache._set('test', [
        Grid.encode({ id: 41, x: 1, y: 1, relev: 1, score: 1 }),
        Grid.encode({ id: 42, x: 1, y: 1, relev: 1, score: 1 }),
        Grid.encode({ id: 43, x: 1, y: 1, relev: 1, score: 1 })
    ], [3,4]);

    cache._set('testy', [
        Grid.encode({ id: 51, x: 1, y: 1, relev: 1, score: 1 }),
        Grid.encode({ id: 52, x: 1, y: 1, relev: 1, score: 1 }),
        Grid.encode({ id: 53, x: 1, y: 1, relev: 1, score: 1 })
    ], [0]);
    cache._set('tentacle', [
        Grid.encode({ id: 61, x: 1, y: 1, relev: 1, score: 1 }),
        Grid.encode({ id: 62, x: 1, y: 1, relev: 1, score: 1 }),
        Grid.encode({ id: 63, x: 1, y: 1, relev: 1, score: 1 })
    ], [0]);

    cache._set('hello', [
        Grid.encode({ id: 71, x: 1, y: 1, relev: 1, score: 1 }),
        Grid.encode({ id: 72, x: 1, y: 1, relev: 1, score: 1 }),
        Grid.encode({ id: 73, x: 1, y: 1, relev: 1, score: 1 })
    ], [0]);

    // cache A serializes data, cache B loads serialized data.
    const pack = tmpfile();
    cache.pack(pack);
    const loader = new carmenCache.RocksDBCache('packed', pack);

    [cache, loader].forEach((c) => {
        const test_all_langs_no_prefix = cache._getMatching('test', false);
        assert.deepEqual(
            getIds(test_all_langs_no_prefix),
            [1, 2, 3, 11, 12, 13, 21, 22, 23, 31, 32, 33, 41, 42, 43],
            "getMatching for 'test' with no prefix match and no language includes all IDs for 'test'"
        );
        assert.deepEqual(
            getIds(test_all_langs_no_prefix),
            getIds(getByLanguageMatch(test_all_langs_no_prefix, true)),
            "getMatching for 'test' with no prefix match and no language includes only match_language: true"
        );

        const test_all_langs_with_prefix = cache._getMatching('test', true);
        assert.deepEqual(
            getIds(test_all_langs_with_prefix),
            [1, 2, 3, 11, 12, 13, 21, 22, 23, 31, 32, 33, 41, 42, 43, 51, 52, 53],
            "getMatching for 'test' with prefix match and no language includes all IDs for 'test' and 'testy'"
        );
        assert.deepEqual(
            getIds(test_all_langs_with_prefix),
            getIds(getByLanguageMatch(test_all_langs_with_prefix, true)),
            "getMatching for 'test' with prefix match and no language includes only match_language: true"
        );

        assert.false(cache._getMatching('te', false), "getMatching for 'te' with no prefix match returns nothing");

        const te_all_langs_with_prefix = cache._getMatching('te', true);
        assert.deepEqual(
            getIds(te_all_langs_with_prefix),
            [1, 2, 3, 11, 12, 13, 21, 22, 23, 31, 32, 33, 41, 42, 43, 51, 52, 53, 61, 62, 63],
            "getMatching for 'te' with prefix match and no language includes all IDs for 'test' and 'testy' and 'tentacle'"
        );
        assert.deepEqual(
            getIds(te_all_langs_with_prefix),
            getIds(getByLanguageMatch(te_all_langs_with_prefix, true)),
            "getMatching for 'te' with prefix match and no language includes only match_language: true"
        );

        const test_all_langs_with_prefix_0 = cache._getMatching('test', true, [0]);
        const test_all_langs_with_prefix_0_matched = getByLanguageMatch(test_all_langs_with_prefix_0, true);
        const test_all_langs_with_prefix_0_unmatched = getByLanguageMatch(test_all_langs_with_prefix_0, false);
        assert.deepEqual(
            getIds(test_all_langs_with_prefix_0),
            [1, 2, 3, 11, 12, 13, 21, 22, 23, 31, 32, 33, 41, 42, 43, 51, 52, 53],
            "getMatching for 'test' with prefix match and language [0] includes all IDs for 'test' and 'testy'"
        );
        assert.deepEqual(
            getIds(test_all_langs_with_prefix_0_matched),
            [1, 2, 3, 11, 12, 13, 51, 52, 53],
            "getMatching for 'test' with prefix match and language [0] includes language_match: true for IDs for 'test' and 'testy' with language 0"
        );
        assert.deepEqual(
            getIds(test_all_langs_with_prefix_0_unmatched),
            [21, 22, 23, 31, 32, 33, 41, 42, 43],
            "getMatching for 'test' with prefix match and language [0] includes language_match: false for IDs for 'test' and 'testy' without language 0"
        );
        assert.deepEqual(
            getIds(test_all_langs_with_prefix_0.slice(0, test_all_langs_with_prefix_0_matched.length)),
            getIds(test_all_langs_with_prefix_0_matched),
            'all the language-matching results come first'
        );

        const te_all_langs_with_prefix_0 = cache._getMatching('te', true, [0]);
        const te_all_langs_with_prefix_0_matched = getByLanguageMatch(te_all_langs_with_prefix_0, true);
        const te_all_langs_with_prefix_0_unmatched = getByLanguageMatch(te_all_langs_with_prefix_0, false);
        assert.deepEqual(
            getIds(te_all_langs_with_prefix_0),
            [1, 2, 3, 11, 12, 13, 21, 22, 23, 31, 32, 33, 41, 42, 43, 51, 52, 53, 61, 62, 63],
            "getMatching for 'te' with prefix match and language [0] includes all IDs for 'tentacle' and 'test' and 'testy'"
        );
        assert.deepEqual(
            getIds(te_all_langs_with_prefix_0_matched),
            [1, 2, 3, 11, 12, 13, 51, 52, 53, 61, 62, 63],
            "getMatching for 'te' with prefix match and language [0] includes language_match: true for IDs for 'tentacle' and 'test' and 'testy' with language 0"
        );
        assert.deepEqual(
            getIds(te_all_langs_with_prefix_0_unmatched),
            [21, 22, 23, 31, 32, 33, 41, 42, 43],
            "getMatching for 'te' with prefix match and language [0] includes language_match: false for IDs for 'tentacle' and 'test' and 'testy' without language 0"
        );
        assert.deepEqual(
            getIds(te_all_langs_with_prefix_0.slice(0, te_all_langs_with_prefix_0_matched.length)),
            getIds(te_all_langs_with_prefix_0_matched),
            'all the language-matching results come first'
        );

        const test_all_langs_with_prefix_1 = cache._getMatching('test', true, [1]);
        const test_all_langs_with_prefix_1_matched = getByLanguageMatch(test_all_langs_with_prefix_1, true);
        const test_all_langs_with_prefix_1_unmatched = getByLanguageMatch(test_all_langs_with_prefix_1, false);
        assert.deepEqual(
            getIds(test_all_langs_with_prefix_1),
            [1, 2, 3, 11, 12, 13, 21, 22, 23, 31, 32, 33, 41, 42, 43, 51, 52, 53],
            "getMatching for 'test' with prefix match and language [1] includes all IDs for 'test' and 'testy'"
        );
        assert.deepEqual(
            getIds(test_all_langs_with_prefix_1_matched),
            [1, 2, 3, 21, 22, 23, 31, 32, 33],
            "getMatching for 'test' with prefix match and language [1] includes language_match: true for IDs for 'test' and 'testy' for both language 1 and language 0,1"
        );
        assert.deepEqual(
            getIds(test_all_langs_with_prefix_1_unmatched),
            [11, 12, 13, 41, 42, 43, 51, 52, 53],
            "getMatching for 'test' with prefix match and language [1] includes language_match: false for IDs for 'test' and 'testy' without language 1"
        );
        assert.deepEqual(
            getIds(test_all_langs_with_prefix_1.slice(0, test_all_langs_with_prefix_1_matched.length)),
            getIds(test_all_langs_with_prefix_1_matched),
            'all the language-matching results come first'
        );

        const test_all_langs_with_prefix_7 = cache._getMatching('test', true, [7]);
        const test_all_langs_with_prefix_7_matched = getByLanguageMatch(test_all_langs_with_prefix_7, true);
        const test_all_langs_with_prefix_7_unmatched = getByLanguageMatch(test_all_langs_with_prefix_7, false);
        assert.deepEqual(
            getIds(test_all_langs_with_prefix_7),
            [1, 2, 3, 11, 12, 13, 21, 22, 23, 31, 32, 33, 41, 42, 43, 51, 52, 53],
            "getMatching for 'test' with prefix match and language [7] includes all IDs for 'test' and 'testy'"
        );
        assert.deepEqual(
            getIds(test_all_langs_with_prefix_7_matched),
            [1, 2, 3],
            "getMatching for 'test' with prefix match and language [7] includes only language_match: true for IDs for 'test' and 'testy' with no language set"
        );
        assert.deepEqual(
            getIds(test_all_langs_with_prefix_7_unmatched),
            [11, 12, 13, 21, 22, 23, 31, 32, 33, 41, 42, 43, 51, 52, 53],
            "getMatching for 'test' with prefix match and language [7] includes language_match: false for all IDs for 'test' and 'testy' with a language set"
        );
        assert.deepEqual(
            getIds(test_all_langs_with_prefix_7.slice(0, test_all_langs_with_prefix_7_matched.length)),
            getIds(test_all_langs_with_prefix_7_matched),
            'all the language-matching results come first'
        );
    });

    // assert.deepEqual(loader.list('grid'), [ 'else.', 'something', 'test', 'test.' ], 'keys in shard');
    assert.end();
});
