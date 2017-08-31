var MemoryCache = require('../index.js').MemoryCache;
var RocksDBCache = require('../index.js').RocksDBCache;
var Grid = require('./grid.js');
var coalesce = require('../index.js').coalesce;
var test = require('tape');
var fs = require('fs');

var tmpdir = "/tmp/temp." + Math.random().toString(36).substr(2, 5);
fs.mkdirSync(tmpdir);
var tmpidx = 0;
var tmpfile = function() { return tmpdir + "/" + (tmpidx++) + ".dat"; };

(function() {
    var memcache = new MemoryCache('a', 0);
    memcache._set('1', [
        Grid.encode({
            id: 2,
            x: 2,
            y: 2,
            relev: 0.8,
            score: 3
        }),
        Grid.encode({
            id: 3,
            x: 8556,
            y: 5443,
            relev: 1,
            score: 1
        }),
        Grid.encode({
            id: 1,
            x: 1,
            y: 1,
            relev: 1,
            score: 7
        }),
    ]);

    [memcache].forEach(function(cache) {
        test('coalesceSingle proximity: ' + cache.id, function(assert) {
            coalesce([{
                cache: cache,
                mask: 1 << 0,
                idx: 0,
                zoom: 14,
                weight: 1,
                phrase: '1',
                prefix: false,
            }], {
                // centerzxy: [14, 8643.97265625, 5400.75]
                centerzxy: [14, 8643, 5400]
            }, function(err, res) {
                assert.ifError(err);
                assert.deepEqual(res[0].relev, 1, '0.relev');
                assert.deepEqual(res[0][0], { matches_language: true, distance: 0, id: 3, idx: 0, relev: 1.0, score: 1, scoredist: 112.5, tmpid: 3, x: 3, y: 3 }, '0.0');
                assert.deepEqual(res[1].relev, 1, '1.relev');
                assert.deepEqual(res[1][0], { matches_language: true, distance: 8, id: 1, idx: 0, relev: 1.0, score: 7, scoredist: 7, tmpid: 1, x: 1, y: 1 }, '1.0');
                assert.deepEqual(res[2].relev, 0.8, '2.relev');
                assert.deepEqual(res[2][0], { matches_language: true, distance: 2, id: 2, idx: 0, relev: 0.8, score: 3, scoredist: 3, tmpid: 2, x: 2, y: 2 }, '2.0');
                assert.end();
            });
        });
    });
})();


