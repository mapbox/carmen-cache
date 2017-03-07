var carmenCache = require('../index.js');
var tape = require('tape');
var fs = require('fs');
var mp53 = Math.pow(2,53);

var tmpdir = "/tmp/temp." + Math.random().toString(36).substr(2, 5);
fs.mkdirSync(tmpdir);
var tmpidx = 0;
var tmpfile = function() { return tmpdir + "/" + (tmpidx++) + ".dat"; };

tape('language fuzzing', function(assert) {
    var cache = new carmenCache.MemoryCache('a');

    var records = new Map();
    for (var j = 0; j < 1000; j++) {
        var phrase = Math.random().toString(36).substr(2, 3 + Math.floor(Math.random() * 6));
        var numLanguages = Math.floor(Math.random() * 4);
        var languages;
        if (numLanguages == 0) {
            languages = null;
        } else {
            languages = new Set();
            for (var i = 0; i < numLanguages; i++) {
                languages.add(Math.floor(Math.random() * 128));
            }
            languages = Array.from(languages).sort(function(a, b) { return a - b; });
        }
        var recordId = phrase + "-" + (languages == null ? "null" : languages.join("-"));
        records.set(recordId, {phrase: phrase, languages: languages});

        cache._set(phrase, [1], languages);
    }

    var list = cache.list();
    assert.equal(list.length, records.size, "got the same number of items out as went in");
    var hasAll = true;
    for (var item of list) {
        var recordId = item[0] + "-" + (item[1] == null ? "null" : item[1].join("-"));
        hasAll = hasAll && records.has(recordId);
    }
    assert.ok(hasAll, "all records and languages came out that went in");

    var pack = tmpfile();
    cache.pack(pack);
    var loader = new carmenCache.RocksDBCache('b', pack);

    list = loader.list();
    assert.equal(list.length, records.size, "got the same number of items out as went in");
    var hasAll = true;
    for (var item of list) {
        var recordId = item[0] + "-" + (item[1] == null ? "null" : item[1].join("-"));
        hasAll = hasAll && records.has(recordId);
    }
    assert.ok(hasAll, "all records and languages came out that went in");

    assert.end();
});