var MemoryCache = require('../index.js').MemoryCache;
var Grid = require('./grid.js');
var coalesce = require('../index.js').coalesce;
var test = require('tape');

(function() {
    var cache = new MemoryCache('a', 0);
    var ne = {
        id: 1,
        x: 200,
        y: 200,
        relev: 1,
        score: 1
    };
    var se = {
        id: 2,
        x: 200,
        y: 0,
        relev: 1,
        score: 1
    };
    var sw = {
        id: 3,
        x: 0,
        y: 0,
        relev: 1,
        score: 1
    };
    var nw = {
        id: 4,
        x: 0,
        y: 200,
        relev: 1,
        score: 1
    };
    cache._set('1', [
        Grid.encode(ne),
        Grid.encode(se),
        Grid.encode(sw),
        Grid.encode(nw),
    ]);

    // This centerpoint favors the N/S direction over E/W slightly to make
    // expected output order obvious
    test('proximity ne', function(assert) {
        coalesce([{
            cache: cache,
            mask: 1 << 0,
            idx: 0,
            zoom: 14,
            weight: 1,
            phrase: '1',
            prefix: false
        }], {
            tileradius: 160,
            centerzxy: [14, 100 + 10, 100 + 15]
        }, function(err, res) {
            assert.ifError(err);
            assert.deepEqual(res.map((cover) => cover[0].id), [ne.id, nw.id, se.id, sw.id], 'right order');
            assert.deepEqual(res.map((cover) => Math.floor(cover[0].distance)), [ 123, 139, 146, 159 ], 'distances check out');
            assert.end();
        });
    });

    test('proximity se', function(assert) {
        coalesce([{
            cache: cache,
            mask: 1 << 0,
            idx: 0,
            zoom: 14,
            weight: 1,
            phrase: '1',
            prefix: false
        }], {
            tileradius: 160,
            centerzxy: [14, 100 + 10, 100 - 15]
        }, function(err, res) {
            assert.ifError(err);
            assert.deepEqual(res.map((cover) => cover[0].id), [se.id, sw.id, ne.id, nw.id], 'right order');
            assert.deepEqual(res.map((cover) => Math.floor(cover[0].distance)), [ 123, 139, 146, 159 ], 'distances check out');
            assert.end();
        });
    });

    test('proximity sw', function(assert) {
        coalesce([{
            cache: cache,
            mask: 1 << 0,
            idx: 0,
            zoom: 14,
            weight: 1,
            phrase: '1',
            prefix: false
        }], {
            tileradius: 160,
            centerzxy: [14, 100 - 10, 100 - 15]
        }, function(err, res) {
            assert.ifError(err);
            assert.deepEqual(res.map((cover) => cover[0].id), [sw.id, se.id, nw.id, ne.id], 'right order');
            assert.deepEqual(res.map((cover) => Math.floor(cover[0].distance)), [ 123, 139, 146, 159 ], 'distances check out');
            assert.end();
        });
    });

    test('proximity nw', function(assert) {
        coalesce([{
            cache: cache,
            mask: 1 << 0,
            idx: 0,
            zoom: 14,
            weight: 1,
            phrase: '1',
            prefix: false
        }], {
            tileradius: 160,
            centerzxy: [14, 100 - 10, 100 + 15]
        }, function(err, res) {
            assert.ifError(err);
            assert.deepEqual(res.map((cover) => cover[0].id), [nw.id, ne.id, sw.id, se.id], 'right order');
            assert.deepEqual(res.map((cover) => Math.floor(cover[0].distance)), [ 123, 139, 146, 159 ], 'distances check out');
            assert.end();
        });
    });
})();


