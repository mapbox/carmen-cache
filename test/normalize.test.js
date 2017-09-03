var carmenCache = require('../index.js');
var tape = require('tape');
var fs = require('fs');

var tmpdir = "/tmp/temp." + Math.random().toString(36).substr(2, 5);
fs.mkdirSync(tmpdir);
var tmpidx = 0;
var tmpfile = function() { return tmpdir + "/" + (tmpidx++) + ".dat"; };

var sorted = function(arr) {
    return [].concat(arr).sort();
}

var sortedDescending = function(arr) {
    return [].concat(arr).sort(function (a, b) { return b - a; });
}

var words = [
    "first street",
    "1st st",
    "frank blvd",
    "frank boulevard",
    "fred road",
    "apple lane",
    "pear avenue",
    "pear ave",
].sort();

var norm = {
    'first street': '1st st',
    'frank boulevard': 'frank blvd',
    'pear avenue': 'pear ave'
}

var file = tmpfile();

tape('write/dump', function(assert) {
    var cache = new carmenCache.NormalizationCache(file, false);

    var map = [];
    for (var key in norm) {
        map.push([words.indexOf(key), words.indexOf(norm[key])])
    }
    cache.writeBatch(map);

    assert.deepEqual(map, cache.getAll(), "dumped contents match input");

    // test some invalid input
    assert.throws(function() { cache.writeBatch(); });
    assert.throws(function() { cache.writeBatch(7); });

    return assert.end();
});

tape('read', function(assert) {
    var cache = new carmenCache.NormalizationCache(file, true);

    assert.equal(cache.get(words.indexOf('first street')), words.indexOf('1st st'));
    assert.equal(cache.get(8888), undefined);

    // find the indexes of all the keys that start with f
    var f = [];
    for (var i = 0; i < words.length; i++) if (words[i].charAt(0) == 'f') f.push(i);
    assert.deepEqual(cache.getPrefixRange(f[0], f.length), [words.indexOf('1st st')], 'found normalization for 1st st but not frank boulevard');

    assert.deepEqual(cache.getPrefixRange(words.indexOf('frank boulevard'), 1), [words.indexOf('frank blvd')], 'found frank boulevard because no prefixes are shared');

    // test some invalid input
    assert.throws(function() { new carmenCache.NormalizationCache() });
    assert.throws(function() { new carmenCache.NormalizationCache(7) });
    assert.throws(function() { new carmenCache.NormalizationCache('asdf', 7) });

    assert.throws(function() { new carmenCache.NormalizationCache('/proc', true) });


    assert.throws(function() { cache.get() });
    assert.throws(function() { cache.getPrefixRange('asdf') });

    assert.throws(function() { cache.getPrefixRange() });
    assert.throws(function() { cache.getPrefixRange('asdf') });
    assert.throws(function() { cache.getPrefixRange('asdf', 'asdf') });
    assert.throws(function() { cache.getPrefixRange(1, 'asdf') });
    assert.throws(function() { cache.getPrefixRange(1, 1, 'asdf') });
    assert.throws(function() { cache.getPrefixRange(1, 1, 1, 'asdf') });

    assert.end();
});