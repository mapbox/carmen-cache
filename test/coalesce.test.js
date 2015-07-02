var Cache = require('../index.js').Cache;
var Grid = require('./grid.js');
var coalesce = require('../index.js').Cache.coalesce;
var test = require('tape');

test('coalesce args', function(assert) {
    assert.throws(function() {
        coalesce();
    }, /Arg 1 must be a PhrasematchSubq array/, 'throws');

    assert.throws(function() {
        coalesce([
            {
                cache: new Cache('a', 1),
                idx: 0,
                zoom: 0,
                weight: 0.5,
                shardlevel: 1,
                phrase: 1
            },
            {
                cache: new Cache('b', 1),
                idx: 1,
                zoom: 1,
                weight: 0.5,
                shardlevel: 1,
                phrase: 1
            },
        ]);
    }, /Arg 2 must be an options object/, 'throws');

    assert.throws(function() {
        coalesce([], { centerzxy:[0,0,0] } );
    }, /Arg 3 must be a callback/, 'throws');

    assert.throws(function() {
        coalesce([], { centerzxy:[0,0,0] }, 5 );
    }, /Arg 3 must be a callback/, 'throws');

    assert.end();
});

(function() {
    var cache = new Cache('a', 0);
    cache._set('grid', 0, 1, [
        Grid.encode({
            id: 2,
            x: 2,
            y: 2,
            relev: 0.8,
            score: 3
        }),
        Grid.encode({
            id: 3,
            x: 3,
            y: 3,
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
    test('coalesceSingle', function(assert) {
        coalesce([{
            cache: cache,
            idx: 0,
            zoom: 2,
            weight: 1,
            shardlevel: 0,
            phrase: 1
        }], {}, function(err, res) {
            assert.ifError(err);
            assert.deepEqual(res[0].relev, 1, '0.relev');
            assert.deepEqual(res[0][0], { distance: 0, id: 1, idx: 0, relev: 1.0, score: 7, tmpid: 1, x: 1, y: 1 }, '0.0');
            assert.deepEqual(res[1].relev, 1, '1.relev');
            assert.deepEqual(res[1][0], { distance: 0, id: 3, idx: 0, relev: 1.0, score: 1, tmpid: 3, x: 3, y: 3 }, '1.0');
            assert.deepEqual(res[2].relev, 0.8, '2.relev');
            assert.deepEqual(res[2][0], { distance: 0, id: 2, idx: 0, relev: 0.8, score: 3, tmpid: 2, x: 2, y: 2 }, '2.0');
            assert.end();
        });
    });
    test('coalesceSingle proximity', function(assert) {
        coalesce([{
            cache: cache,
            idx: 0,
            zoom: 2,
            weight: 1,
            shardlevel: 0,
            phrase: 1
        }], {
            centerzxy: [3,3,3]
        }, function(err, res) {
            assert.ifError(err);
            assert.deepEqual(res[0].relev, 1, '0.relev');
            assert.deepEqual(res[0][0], { distance: 0, id: 3, idx: 0, relev: 1.0, score: 1, tmpid: 3, x: 3, y: 3 }, '0.0');
            assert.deepEqual(res[1].relev, 1, '1.relev');
            assert.deepEqual(res[1][0], { distance: 4, id: 1, idx: 0, relev: 1.0, score: 7, tmpid: 1, x: 1, y: 1 }, '1.0');
            assert.deepEqual(res[2].relev, 0.8, '2.relev');
            assert.deepEqual(res[2][0], { distance: 2, id: 2, idx: 0, relev: 0.8, score: 3, tmpid: 2, x: 2, y: 2 }, '2.0');
            assert.end();
        });
    });
})();

(function() {
    var a = new Cache('a', 0);
    var b = new Cache('b', 0);
    a._set('grid', 0, 1, [
        Grid.encode({
            id: 1,
            x: 1,
            y: 1,
            relev: 1,
            score: 1
        }),
        Grid.encode({
            id: 2,
            x: 2,
            y: 2,
            relev: 1,
            score: 1
        }),
    ]);
    b._set('grid', 0, 1, [
        Grid.encode({
            id: 2,
            x: 2,
            y: 2,
            relev: 1,
            score: 7
        }),
        Grid.encode({
            id: 3,
            x: 3,
            y: 3,
            relev: 1,
            score: 1
        }),
        Grid.encode({
            id: 1,
            x: 1,
            y: 1,
            relev: 1,
            score: 3
        }),
    ]);
    test('coalesceUV', function(assert) {
        coalesce([{
            cache: a,
            idx: 0,
            zoom: 1,
            weight: 0.5,
            shardlevel: 0,
            phrase: 1
        }, {
            cache: b,
            idx: 1,
            zoom: 2,
            weight: 0.5,
            shardlevel: 0,
            phrase: 1
        }], {}, function(err, res) {
            assert.ifError(err);
            // sorts by relev, score
            assert.deepEqual(res[0].relev, 1, '0.relev');
            assert.deepEqual(res[0][0], { distance: 0, id: 2, idx: 1, relev: 0.5, score: 7, tmpid: 33554434, x: 2, y: 2 }, '0.0');
            assert.deepEqual(res[0][1], { distance: 0, id: 1, idx: 0, relev: 0.5, score: 1, tmpid: 1, x: 1, y: 1 }, '0.1');
            assert.deepEqual(res[1].relev, 1, '1.relev');
            assert.deepEqual(res[1][0], { distance: 0, id: 3, idx: 1, relev: 0.5, score: 1, tmpid: 33554435, x: 3, y: 3 }, '1.0');
            assert.deepEqual(res[1][1], { distance: 0, id: 1, idx: 0, relev: 0.5, score: 1, tmpid: 1, x: 1, y: 1 }, '1.1');
            assert.end();
        });
    });
    test('coalesceUV proximity', function(assert) {
        coalesce([{
            cache: a,
            idx: 0,
            zoom: 1,
            weight: 0.5,
            shardlevel: 0,
            phrase: 1
        }, {
            cache: b,
            idx: 1,
            zoom: 2,
            weight: 0.5,
            shardlevel: 0,
            phrase: 1
        }], {
            centerzxy: [2,3,3]
        }, function(err, res) {
            assert.ifError(err);
            // sorts by relev, score
            assert.deepEqual(res[0].relev, 1, '0.relev');
            assert.deepEqual(res[0][0], { distance: 0, id: 3, idx: 1, relev: 0.5, score: 1, tmpid: 33554435, x: 3, y: 3 }, '0.0');
            assert.deepEqual(res[0][1], { distance: 4, id: 1, idx: 0, relev: 0.5, score: 1, tmpid: 1, x: 1, y: 1 }, '0.1');
            assert.deepEqual(res[1].relev, 1, '1.relev');
            assert.deepEqual(res[1][0], { distance: 2, id: 2, idx: 1, relev: 0.5, score: 7, tmpid: 33554434, x: 2, y: 2 }, '1.0');
            assert.deepEqual(res[1][1], { distance: 4, id: 1, idx: 0, relev: 0.5, score: 1, tmpid: 1, x: 1, y: 1 }, '1.1');
            assert.end();
        });
    });
})();


// Multi sandwich scenario
(function() {
    var a = new Cache('a', 0);
    var b = new Cache('b', 0);
    a._set('grid', 0, 1, [
        Grid.encode({
            id: 3,
            x: 0,
            y: 0,
            relev: 1,
            score: 1
        }),
        Grid.encode({
            id: 4,
            x: 0,
            y: 0,
            relev: 1,
            score: 1
        })
    ]);
    b._set('grid', 0, 1, [
        Grid.encode({
            id: 1,
            x: 1,
            y: 1,
            relev: 1,
            score: 1
        }),
        Grid.encode({
            id: 2,
            x: 1,
            y: 1,
            relev: 1,
            score: 1
        })
    ]);
    test('coalesceMulti sandwich', function(assert) {
        coalesce([{
            cache: a,
            idx: 0,
            zoom: 0,
            weight: 0.5,
            shardlevel: 0,
            phrase: 1
        }, {
            cache: b,
            idx: 1,
            zoom: 1,
            weight: 0.5,
            shardlevel: 0,
            phrase: 1
        }], {}, function(err, res) {
            assert.ifError(err);
            assert.equal(res.length, 3, 'res length = 3');
            // sorts by relev, score
            assert.deepEqual(res[0].map(function(f) { return f.id; }), [3,2], '0.relev = 1');
            assert.deepEqual(res[1].map(function(f) { return f.id; }), [4,2], '0.relev = 1');
            assert.deepEqual(res[2].map(function(f) { return f.id; }), [1,3], '0.relev = 1');
            assert.end();
        });
    });
})();
