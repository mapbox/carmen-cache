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
                cache: new Cache('a'),
                mask: 1 << 0,
                idx: 0,
                zoom: 0,
                weight: 0.5,
                phrase: 1
            },
            {
                cache: new Cache('b'),
                mask: 1 << 1,
                idx: 1,
                zoom: 1,
                weight: 0.5,
                phrase: 1
            },
        ]);
    }, /Arg 2 must be an options object/, 'throws');

    assert.throws(function() {
        coalesce([], { centerzxy:[0,0,0] } );
    }, /Arg 3 must be a callback/, 'throws');

    assert.throws(function() {
        coalesce([-1]);
    }, /All items in array must be valid/, 'throws');

    assert.throws(function() {
        coalesce([undefined]);
    }, /All items in array must be valid/, 'throws');

    assert.throws(function() {
        coalesce([{idx:-1}]);
    }, /encountered idx value too large to fit/, 'throws');

    assert.throws(function() {
        coalesce([{zoom:-1}]);
    }, /encountered zoom value too large to fit/, 'throws');

    assert.throws(function() {
        coalesce([], { centerzxy:[-1,0,0] } );
    }, /value in array too large/, 'throws');

    assert.throws(function() {
        coalesce([], { centerzxy:[4294967296,0,0] } );
    }, /value in array too large/, 'throws');

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
            mask: 1 << 0,
            idx: 0,
            zoom: 2,
            weight: 1,
            phrase: 1
        }], {}, function(err, res) {
            assert.ifError(err);
            assert.deepEqual(res[0].relev, 1, '0.relev');
            assert.deepEqual(res[0][0], { distance: 0, id: 1, idx: 0, relev: 1.0, score: 7, scoredist: 7, tmpid: 1, x: 1, y: 1 }, '0.0');
            assert.deepEqual(res[1].relev, 1, '1.relev');
            assert.deepEqual(res[1][0], { distance: 0, id: 3, idx: 0, relev: 1.0, score: 1, scoredist: 1, tmpid: 3, x: 3, y: 3 }, '1.0');
            assert.deepEqual(res[2].relev, 0.8, '2.relev');
            assert.deepEqual(res[2][0], { distance: 0, id: 2, idx: 0, relev: 0.8, score: 3, scoredist: 3, tmpid: 2, x: 2, y: 2 }, '2.0');
            assert.end();
        });
    });
    test('coalesceSingle proximity', function(assert) {
        coalesce([{
            cache: cache,
            mask: 1 << 0,
            idx: 0,
            zoom: 2,
            weight: 1,
            phrase: 1
        }], {
            centerzxy: [3,3,3]
        }, function(err, res) {
            assert.ifError(err);
            assert.deepEqual(res[0].relev, 1, '0.relev');
            assert.deepEqual(res[0][0], { distance: 0, id: 3, idx: 0, relev: 1.0, score: 1, scoredist: 100, tmpid: 3, x: 3, y: 3 }, '0.0');
            assert.deepEqual(res[1].relev, 1, '1.relev');
            assert.deepEqual(res[1][0], { distance: 4, id: 1, idx: 0, relev: 1.0, score: 7, scoredist: 7, tmpid: 1, x: 1, y: 1 }, '1.0');
            assert.deepEqual(res[2].relev, 0.8, '2.relev');
            assert.deepEqual(res[2][0], { distance: 2, id: 2, idx: 0, relev: 0.8, score: 3, scoredist: 3, tmpid: 2, x: 2, y: 2 }, '2.0');
            assert.end();
        });
    });

    test('coalesceSingle bbox', function(assert) {
        coalesce([{
            cache: cache,
            mask: 1 << 0,
            idx: 0,
            zoom: 2,
            weight: 1,
            phrase: 1
        }], {
            bboxzxy: [2, 1, 1, 1, 1]
        }, function(err, res) {
            assert.ifError(err);
            assert.deepEqual(res[0].relev, 1, '1.relev');
            assert.deepEqual(res.length, 1);
            assert.deepEqual(res[0][0], { distance: 0, id: 1, idx: 0, relev: 1.0, score: 7, scoredist: 7, tmpid: 1, x: 1, y: 1 }, '1.0');
            assert.end();
        });
    });

})();

(function() {
    var cache = new Cache('a', 0);
    var grids = [];
    var encoded1 = Grid.encode({
        id: 1,
        x: 1,
        y: 1,
        relev: 1,
        score: 0
    });
    for (var i = 0; i < 80; i++) grids.push(encoded1);
    grids.push(Grid.encode({
        id: 2,
        x: 1,
        y: 1,
        relev: 1,
        score: 0
    }));

    cache._set('grid', 0, 1, grids);
    test('coalesceSingle', function(assert) {
        coalesce([{
            cache: cache,
            mask: 1 << 0,
            idx: 0,
            zoom: 2,
            weight: 1,
            phrase: 1
        }], {}, function(err, res) {
            assert.ifError(err);
            assert.equal(res.length, 2);
            assert.deepEqual(res[0].relev, 1, '0.relev');
            assert.deepEqual(res[0][0], { distance: 0, id: 1, idx: 0, relev: 1.0, score: 0, scoredist: 0, tmpid: 1, x: 1, y: 1 }, '0.0');
            assert.deepEqual(res[1].relev, 1, '1.relev');
            assert.deepEqual(res[1][0], { distance: 0, id: 2, idx: 0, relev: 1.0, score: 0, scoredist: 0, tmpid: 2, x: 1, y: 1 }, '0.0');
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
            mask: 1 << 1,
            idx: 0,
            zoom: 1,
            weight: 0.5,
            phrase: 1
        }, {
            cache: b,
            mask: 1 << 0,
            idx: 1,
            zoom: 2,
            weight: 0.5,
            phrase: 1
        }], {}, function(err, res) {
            assert.ifError(err);
            // sorts by relev, score
            assert.deepEqual(res[0].relev, 1, '0.relev');
            assert.deepEqual(res[0][0], { distance: 0, id: 2, idx: 1, relev: 0.5, score: 7, scoredist: 7, tmpid: 33554434, x: 2, y: 2 }, '0.0');
            assert.deepEqual(res[0][1], { distance: 0, id: 1, idx: 0, relev: 0.5, score: 1, scoredist: 1, tmpid: 1, x: 1, y: 1 }, '0.1');
            assert.deepEqual(res[1].relev, 1, '1.relev');
            assert.deepEqual(res[1][0], { distance: 0, id: 3, idx: 1, relev: 0.5, score: 1, scoredist: 1, tmpid: 33554435, x: 3, y: 3 }, '1.0');
            assert.deepEqual(res[1][1], { distance: 0, id: 1, idx: 0, relev: 0.5, score: 1, scoredist: 1, tmpid: 1, x: 1, y: 1 }, '1.1');
            assert.end();
        });
    });
    test('coalesceUV proximity', function(assert) {
        coalesce([{
            cache: a,
            mask: 1 << 1,
            idx: 0,
            zoom: 1,
            weight: 0.5,
            phrase: 1
        }, {
            cache: b,
            mask: 1 << 0,
            idx: 1,
            zoom: 2,
            weight: 0.5,
            phrase: 1
        }], {
            centerzxy: [2,3,3]
        }, function(err, res) {
            assert.ifError(err);
            // sorts by relev, score
            assert.deepEqual(res[0].relev, 1, '0.relev');
            assert.deepEqual(res[0][0], { distance: 0, id: 3, idx: 1, relev: 0.5, score: 1, scoredist: 100, tmpid: 33554435, x: 3, y: 3 }, '0.0');
            assert.deepEqual(res[0][1], { distance: 4, id: 1, idx: 0, relev: 0.5, score: 1, scoredist: 1, tmpid: 1, x: 1, y: 1 }, '0.1');
            assert.deepEqual(res[1].relev, 1, '1.relev');
            assert.deepEqual(res[1][0], { distance: 2, id: 2, idx: 1, relev: 0.5, score: 7, scoredist: 7, tmpid: 33554434, x: 2, y: 2 }, '1.0');
            assert.deepEqual(res[1][1], { distance: 4, id: 1, idx: 0, relev: 0.5, score: 1, scoredist: 1, tmpid: 1, x: 1, y: 1 }, '1.1');
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
            x: 0,
            y: 0,
            relev: 1,
            score: 1
        }),
    ]);
    b._set('grid', 0, 1, [
        Grid.encode({
            id: 2,
            x: 4800,
            y: 6200,
            relev: 1,
            score: 7
        }),
        Grid.encode({
            id: 3,
            x: 4600,
            y: 6200,
            relev: 1,
            score: 1
        })
    ]);
    test('coalesce scoredist (close proximity)', function(assert) {
        coalesce([{
            cache: a,
            mask: 1 << 1,
            idx: 0,
            zoom: 0,
            weight: 0.5,
            phrase: 1
        }, {
            cache: b,
            mask: 1 << 0,
            idx: 1,
            zoom: 14,
            weight: 0.5,
            phrase: 1
        }], {
            centerzxy: [14,4601,6200]
        }, function(err, res) {
            assert.ifError(err);
            assert.deepEqual(res[0][0].id, 3, 'matches feat 3');
            assert.deepEqual(res[1][0].id, 2, 'matches feat 2');
            assert.deepEqual(res[0][0].distance < res[1][0].distance, true, 'feat 3 is closer than feat2');
            assert.end();
        });
    });
    test('coalesce scoredist (far proximity)', function(assert) {
        coalesce([{
            cache: a,
            mask: 1 << 1,
            idx: 0,
            zoom: 0,
            weight: 0.5,
            phrase: 1
        }, {
            cache: b,
            mask: 1 << 0,
            idx: 1,
            zoom: 14,
            weight: 0.5,
            phrase: 1
        }], {
            centerzxy: [14,4605,6200]
        }, function(err, res) {
            assert.ifError(err);
            assert.deepEqual(res[0][0].id, 2, 'matches feat 2 (higher score)');
            assert.deepEqual(res[1][0].id, 3, 'matches feat 3');
            assert.deepEqual(res[1][0].distance < res[0][0].distance, true, 'feat 3 is closer than feat2');
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
            relev: 0.8,
            score: 1
        }),
        Grid.encode({
            id: 2,
            x: 1,
            y: 1,
            relev: 1,
            score: 1
        }),
    ]);
    b._set('grid', 0, 1, [
        Grid.encode({
            id: 3,
            x: 2,
            y: 2,
            relev: 1,
            score: 1
        }),
    ]);
    test('coalesceMulti (higher relev wins)', function(assert) {
        coalesce([{
            cache: a,
            mask: 1 << 1,
            idx: 0,
            zoom: 1,
            weight: 0.5,
            phrase: 1
        }, {
            cache: b,
            mask: 1 << 0,
            idx: 1,
            zoom: 2,
            weight: 0.5,
            phrase: 1
        }], {}, function(err, res) {
            assert.ifError(err);
            // sorts by relev, score
            assert.deepEqual(res.length, 1, '1 result');
            assert.deepEqual(res[0].relev, 1, '0.relev');
            assert.deepEqual(res[0].length, 2, '0.length');
            assert.deepEqual(res[0][0], { distance: 0, id: 3, idx: 1, relev: 0.5, score: 1, scoredist: 1, tmpid: 33554435, x: 2, y: 2 }, '0.0');
            assert.deepEqual(res[0][1], { distance: 0, id: 2, idx: 0, relev: 0.5, score: 1, scoredist: 1, tmpid: 2, x: 1, y: 1 }, '0.1');
            assert.end();
        });
    });
})();

// cooalese multi bbox
(function() {
    var a = new Cache('a', 0);
    var b = new Cache('b', 0);
    var c = new Cache('c', 0);
    a._set('grid', 0, 1, [
        Grid.encode({
            id: 1,
            x: 0,
            y: 0,
            relev: 0.8,
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
    b._set('grid', 0, 1, [
        Grid.encode({
            id: 3,
            x: 3,
            y: 0,
            relev: 1,
            score: 1
        }),
        Grid.encode({
            id: 4,
            x: 0,
            y: 3,
            relev: 1,
            score: 1
        })
    ]);
    c._set('grid', 0, 1, [
        Grid.encode({
            id: 5,
            x: 21,
            y: 7,
            relev: 1,
            score: 1
        }),
        Grid.encode({
            id: 6,
            x: 21,
            y: 18,
            relev: 1,
            score: 1
        })
    ]);
    test('coalesceMulti bbox', function(assert) {
        coalesce([{
            cache: a,
            mask: 1 << 1,
            idx: 0,
            zoom: 1,
            weight: 0.5,
            phrase: 1
        }, {
            cache: b,
            mask: 1 << 0,
            idx: 1,
            zoom: 2,
            weight: 0.5,
            phrase: 1
        }], {
            bboxzxy: [1, 0, 0, 1, 0]
        }, function(err, res) {
            assert.ifError(err);
            assert.deepEqual(res.length, 2, '2 results: 1/0/0, 2/3/0');
            assert.end();
        });
    });
    test('coalesceMulti bbox', function(assert) {
        coalesce([{
            cache: a,
            mask: 1 << 1,
            idx: 0,
            zoom: 1,
            weight: 0.5,
            phrase: 1
        }, {
            cache: b,
            mask: 1 << 0,
            idx: 1,
            zoom: 2,
            weight: 0.5,
            phrase: 1
        }], {
            bboxzxy: [2, 0, 0, 1, 3]
        }, function(err, res) {
            assert.ifError(err);
            assert.deepEqual(res.length, 2, '2 results: 1/0/0, 2/0/3');
            assert.end();
        });
    });
    test('coalesceMulti bbox', function(assert) {
        coalesce([{
            cache: a,
            mask: 1 << 1,
            idx: 0,
            zoom: 1,
            weight: 0.5,
            phrase: 1
        }, {
            cache: b,
            mask: 1 << 0,
            idx: 1,
            zoom: 2,
            weight: 0.5,
            phrase: 1
        }], {
            bboxzxy: [6, 14, 30, 15, 64]
        }, function(err, res) {
            assert.ifError(err);
            assert.deepEqual(res.length, 2, '2 results: 1/0/0, 2/0/3');
            assert.end();
        });
    });
    test('coalesceMulti bbox', function(assert) {
        coalesce([{
            cache: b,
            mask: 1 << 1,
            idx: 0,
            zoom: 2,
            weight: 0.5,
            phrase: 1
        }, {
            cache: c,
            mask: 1 << 0,
            idx: 1,
            zoom: 5,
            weight: 0.5,
            phrase: 1
        }], {
            bboxzxy: [1, 0, 0, 1, 0]
        }, function(err, res) {
            assert.ifError(err);
            console.log("res", res);
            assert.deepEqual(res.length, 2, '2 results: 5/20/7, 2/3/0');
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
            mask: 1 << 1,
            idx: 0,
            zoom: 0,
            weight: 0.5,
            phrase: 1
        }, {
            cache: b,
            mask: 1 << 0,
            idx: 1,
            zoom: 1,
            weight: 0.5,
            phrase: 1
        }], {}, function(err, res) {
            assert.ifError(err);
            assert.equal(res.length, 2, 'res length = 2');
            // sorts by relev, score
            assert.deepEqual(res[0].map(function(f) { return f.id; }), [1,3], '0.relev = 1');
            assert.deepEqual(res[1].map(function(f) { return f.id; }), [2,3], '0.relev = 1');
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
            x: 0,
            y: 0,
            relev: 1,
            score: 1
        })
    ]);
    test('coalesceMulti sandwich', function(assert) {
        coalesce([{
            cache: a,
            mask: 1 << 1,
            idx: 25,
            zoom: 0,
            weight: 0.5,
            phrase: 1
        }, {
            cache: b,
            mask: 1 << 0,
            idx: 20,
            zoom: 0,
            weight: 0.5,
            phrase: 1
        }], {}, function(err, res) {
            assert.ifError(err);
            assert.equal(res.length, 2, 'res length = 2');
            assert.deepEqual(res[0].map(function(f) { return f.id; }), [3,1], '0.relev = 1');
            assert.deepEqual(res[1].map(function(f) { return f.id; }), [4,1], '0.relev = 1');
            assert.end();
        });
    });
})();
