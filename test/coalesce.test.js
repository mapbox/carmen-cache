var Cache = require('../index.js').Cache;
var Grid = require('./grid.js');
var coalesce = require('../index.js').Cache.coalesce;
var test = require('tape');

test('coalesce args', function(assert) {
    assert.throws(function() {
        coalesce();
    }, /Expects 3 arguments/, 'throws');

    assert.throws(function() {
        coalesce([]);
    }, /Expects 3 arguments/, 'throws');

    assert.throws(function() {
        coalesce([], {} );
    }, /Expects 3 arguments/, 'throws');

    assert.throws(function() {
        coalesce([{}], {}, function() {} );
    }, /missing idx property/, 'throws');

    assert.throws(function() {
        coalesce([-1], {}, function() {} );
    }, /All items in array must be valid PhrasematchSubq objects/, 'throws');

    assert.throws(function() {
        coalesce(undefined, {}, function() {} );
    }, /Arg 1 must be a PhrasematchSubq array/, 'throws');

    assert.throws(function() {
        coalesce([], {}, function() {} );
    }, /Arg 1 must be an array with one or more/, 'throws');

    assert.throws(function() {
        coalesce([undefined], {}, function() {} );
    }, /All items in array must be valid PhrasematchSubq objects/, 'throws');

    assert.throws(function() {
        coalesce([null], {}, function() {} );
    }, /All items in array must be valid PhrasematchSubq objects/, 'throws');

    var valid_subq = {
        cache: new Cache('a'),
        mask: 1 << 0,
        idx: 0,
        zoom: 2,
        weight: 1,
        phrase: 'a',
        prefix: false
    };

    assert.throws(function() {
        coalesce([valid_subq],undefined,function(){});
    }, /Arg 2 must be an options object/, 'throws');

    assert.throws(function() {
        coalesce([valid_subq],undefined,function(){});
    }, /Arg 2 must be an options object/, 'throws');

    if (process.versions.node[0] != '0') {
        assert.throws(function() {
            coalesce([Object.assign({},valid_subq,{idx:-1})],{},function(){});
        }, /encountered idx value too large to fit/, 'throws');

        assert.throws(function() {
            coalesce([Object.assign({},valid_subq,{idx:null})],{},function(){});
        }, /value must be a number/, 'throws');

        assert.throws(function() {
            coalesce([Object.assign({},valid_subq,{zoom:-1})],{},function(){});
        }, /encountered zoom value too large to fit/, 'throws');

        assert.throws(function() {
            coalesce([Object.assign({},valid_subq,{zoom:null})],{},function(){});
        }, /value must be a number/, 'throws');

        assert.throws(function() {
            coalesce([Object.assign({},valid_subq,{zoom:-1})],{},function(){});
        }, /encountered zoom value too large to fit/, 'throws');

        assert.throws(function() {
            coalesce([Object.assign({},valid_subq,{mask:null})],{},function(){});
        }, /value must be a number/, 'throws');

        assert.throws(function() {
            coalesce([Object.assign({},valid_subq,{mask:-1})],{},function(){});
        }, /encountered mask value too large to fit/, 'throws');

        assert.throws(function() {
            coalesce([Object.assign({},valid_subq,{weight:null})],{},function(){});
        }, /weight value must be a number/, 'throws');

        assert.throws(function() {
            coalesce([Object.assign({},valid_subq,{weight:-1})],{},function(){});
        }, /encountered weight value too large to fit in double/, 'throws');

        assert.throws(function() {
            coalesce([Object.assign({},valid_subq,{phrase:null})],{},function(){});
        }, /phrase value must be a string/, 'throws');

        assert.throws(function() {
            coalesce([Object.assign({},valid_subq,{phrase:''})],{},function(){});
        }, /encountered invalid phrase/, 'throws');

        assert.throws(function() {
            coalesce([Object.assign({},valid_subq,{cache:null})],{},function(){});
        }, /cache value must be a Cache object/, 'throws');

        assert.throws(function() {
            coalesce([Object.assign({},valid_subq,{cache:{}})],{},function(){});
        }, /cache value must be a Cache object/, 'throws');

        var valid_stack = [
            {
                cache: new Cache('a'),
                mask: 1 << 0,
                idx: 0,
                zoom: 0,
                weight: 0.5,
                phrase: '1',
                prefix: false,
            },
            {
                cache: new Cache('b'),
                mask: 1 << 1,
                idx: 1,
                zoom: 1,
                weight: 0.5,
                phrase: '1',
                prefix: false,
            }
        ];

        assert.throws(function() {
            coalesce(valid_stack.concat([Object.assign({},valid_subq,{cache:null})]),{},function(){});
        }, /cache value must be a Cache object/, 'throws');

    }

    assert.throws(function() {
        coalesce([valid_subq], { bboxzxy:null },function(){} );
    }, /bboxzxy must be an array/, 'throws');

    assert.throws(function() {
        coalesce([valid_subq], { bboxzxy:[0,0,0,0] },function(){} );
    }, /bboxzxy must be an array of 5 numbers/, 'throws');

    assert.throws(function() {
        coalesce([valid_subq], { bboxzxy:['',0,0,0,0] },function(){} );
    }, /bboxzxy values must be number/, 'throws');

    assert.throws(function() {
        coalesce([valid_subq], { bboxzxy:[-1,0,0,0,0] },function(){} );
    }, /encountered bboxzxy value too large to fit in uint32_t/, 'throws');

    assert.throws(function() {
        coalesce([valid_subq], { bboxzxy:[4294967296,0,0,0,0] },function(){} );
    }, /encountered bboxzxy value too large to fit in uint32_t/, 'throws');

    assert.throws(function() {
        coalesce([valid_subq], { centerzxy:null },function(){} );
    }, /centerzxy must be an array/, 'throws');

    assert.throws(function() {
        coalesce([valid_subq], { centerzxy:['',0,0] },function(){} );
    }, /centerzxy values must be number/, 'throws');

    assert.throws(function() {
        coalesce([valid_subq], { centerzxy:[0,0] },function(){} );
    }, /centerzxy must be an array of 3 numbers/, 'throws');

    assert.throws(function() {
        coalesce([valid_subq], { centerzxy:[-1,0,0] },function(){} );
    }, /encountered centerzxy value too large to fit in uint32_t/, 'throws');

    assert.throws(function() {
        coalesce([valid_subq], { centerzxy:[4294967296,0,0] },function(){} );
    }, /encountered centerzxy value too large to fit in uint32_t/, 'throws');

    assert.throws(function() {
        coalesce([valid_subq], { centerzxy:[0,0,0] }, 5 );
    }, /Arg 3 must be a callback/, 'throws');

    assert.throws(function() {
        coalesce([{mask: 1 << 0, idx: 1, zoom: 1, weight: .5, phrase: '1'}],{},function(){});
    }, /missing/, 'throws');

    assert.throws(function() {
        coalesce([{cache: new Cache('b'), idx: 1, zoom: 1, weight: .5, phrase: '1'}],{},function(){});
    }, /missing/, 'throws');

    assert.throws(function() {
        coalesce([{cache: new Cache('b'), mask: 1 << 0, zoom: 1, weight: .5, phrase: 1}],{},function(){});
    }, /missing/, 'throws');

    assert.throws(function() {
        coalesce([{cache: new Cache('b'), mask: 1 << 0, idx: 1, weight: .5, phrase: 1}],{},function(){});
    }, /missing/, 'throws');

    assert.throws(function() {
        coalesce([{cache: new Cache('b'), mask: 1 << 0, idx: 1, zoom: 1, phrase: 1}],{},function(){});
    }, /missing/, 'throws');

    assert.throws(function() {
        coalesce([{cache: new Cache('b'), mask: 1 << 0, idx: 1, zoom: 1, weight: .5}],{},function(){});
    }, /missing/, 'throws');

    assert.throws(function() {
        coalesce([{cache: '', mask: 1 << 0, idx: 1, weight: .5,  zoom: 1, phrase: '1', prefix: false}],{},function(){});
    }, /cache value must be a Cache object/, 'throws');

    assert.throws(function() {
        coalesce([{cache: new Cache('b'), mask: '', idx: 1, zoom: 1, weight: .5, phrase: '1', prefix: false}],{},function(){});
    }, /mask value must be a number/, 'throws');

    assert.throws(function() {
        coalesce([{cache: new Cache('b'), mask: 1 << 0, idx: '', weight: .5, zoom: 1, phrase: '1', prefix: false}],{},function(){});
    }, /idx value must be a number/, 'throws');

    assert.throws(function() {
        coalesce([{cache: new Cache('b'), mask: 1 << 0, idx: 1, weight: .5, zoom: '', phrase: '1', prefix: false}],{},function(){});
    }, /zoom value must be a number/, 'throws');

    assert.throws(function() {
        coalesce([{cache: new Cache('b'), mask: 1 << 0, idx: 1, weight: '', zoom: 1, phrase: '1', prefix: false}],{},function(){});
    }, /weight value must be a number/, 'throws');

    assert.throws(function() {
        coalesce([{cache: new Cache('b'), mask: 1 << 0, idx: 1, weight: .5, zoom: 1, phrase: ''}],{},function(){});
    }, /encountered invalid phrase/, 'throws');

    assert.end();
});

(function() {
    var cache = new Cache('a', 0);
    cache._set('grid', '1', [
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
            phrase: '1',
            prefix: false,
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
            phrase: '1',
            prefix: false,
        }], {
            centerzxy: [3,3,3]
        }, function(err, res) {
            assert.ifError(err);
            assert.deepEqual(res[0].relev, 1, '0.relev');
            assert.deepEqual(res[0][0], { distance: 0, id: 3, idx: 0, relev: 1.0, score: 1, scoredist: 112.5, tmpid: 3, x: 3, y: 3 }, '0.0');
            assert.deepEqual(res[1].relev, 1, '1.relev');
            assert.deepEqual(res[1][0], { distance: 8, id: 1, idx: 0, relev: 1.0, score: 7, scoredist: 7, tmpid: 1, x: 1, y: 1 }, '1.0');
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
            phrase: '1',
            prefix: false,
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

    cache._set('grid', '1', grids);
    test('coalesceSingle', function(assert) {
        coalesce([{
            cache: cache,
            mask: 1 << 0,
            idx: 0,
            zoom: 2,
            weight: 1,
            phrase: '1',
            prefix: false,
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
    a._set('grid', '1', [
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
    b._set('grid', '1', [
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
            phrase: '1',
            prefix: false,
        }, {
            cache: b,
            mask: 1 << 0,
            idx: 1,
            zoom: 2,
            weight: 0.5,
            phrase: '1',
            prefix: false,
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
            phrase: '1',
            prefix: false,
        }, {
            cache: b,
            mask: 1 << 0,
            idx: 1,
            zoom: 2,
            weight: 0.5,
            phrase: '1',
            prefix: false,
        }], {
            centerzxy: [2,3,3]
        }, function(err, res) {
            assert.ifError(err);
            // sorts by relev, score
            assert.deepEqual(res[0].relev, 1, '0.relev');
            assert.deepEqual(res[0][0], { distance: 0, id: 3, idx: 1, relev: 0.5, score: 1, scoredist: 112.5, tmpid: 33554435, x: 3, y: 3 }, '0.0');
            assert.deepEqual(res[0][1], { distance: 8, id: 1, idx: 0, relev: 0.5, score: 1, scoredist: 1, tmpid: 1, x: 1, y: 1 }, '0.1');
            assert.deepEqual(res[1].relev, 1, '1.relev');
            assert.deepEqual(res[1][0], { distance: 2, id: 2, idx: 1, relev: 0.5, score: 7, scoredist: 7, tmpid: 33554434, x: 2, y: 2 }, '1.0');
            assert.deepEqual(res[1][1], { distance: 8, id: 1, idx: 0, relev: 0.5, score: 1, scoredist: 1, tmpid: 1, x: 1, y: 1 }, '1.1');
            assert.end();
        });
    });
})();

(function() {
    var a = new Cache('a', 0);
    var b = new Cache('b', 0);
    a._set('grid', '1', [
        Grid.encode({
            id: 1,
            x: 0,
            y: 0,
            relev: 1,
            score: 1
        }),
    ]);
    b._set('grid', '1', [
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
            phrase: '1',
            prefix: false,
        }, {
            cache: b,
            mask: 1 << 0,
            idx: 1,
            zoom: 14,
            weight: 0.5,
            phrase: '1',
            prefix: false,
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
            phrase: '1',
            prefix: false,
        }, {
            cache: b,
            mask: 1 << 0,
            idx: 1,
            zoom: 14,
            weight: 0.5,
            phrase: '1',
            prefix: false,
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
    a._set('grid', '1', [
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
    b._set('grid', '1', [
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
            phrase: '1',
            prefix: false,
        }, {
            cache: b,
            mask: 1 << 0,
            idx: 1,
            zoom: 2,
            weight: 0.5,
            phrase: '1',
            prefix: false,
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
    a._set('grid', '1', [
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
    b._set('grid', '1', [
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
    c._set('grid', '1', [
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
            phrase: '1',
            prefix: false,
        }, {
            cache: b,
            mask: 1 << 0,
            idx: 1,
            zoom: 2,
            weight: 0.5,
            phrase: '1',
            prefix: false,
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
            phrase: '1',
            prefix: false,
        }, {
            cache: b,
            mask: 1 << 0,
            idx: 1,
            zoom: 2,
            weight: 0.5,
            phrase: '1',
            prefix: false,
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
            phrase: '1',
            prefix: false,
        }, {
            cache: b,
            mask: 1 << 0,
            idx: 1,
            zoom: 2,
            weight: 0.5,
            phrase: '1',
            prefix: false,
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
            phrase: '1',
            prefix: false,
        }, {
            cache: c,
            mask: 1 << 0,
            idx: 1,
            zoom: 5,
            weight: 0.5,
            phrase: '1',
            prefix: false,
        }], {
            bboxzxy: [1, 0, 0, 1, 0]
        }, function(err, res) {
            assert.ifError(err);
            assert.deepEqual(res.length, 2, '2 results: 5/20/7, 2/3/0');
            assert.end();
        });
    });
})();

// Multi sandwich scenario
(function() {
    var a = new Cache('a', 0);
    var b = new Cache('b', 0);
    a._set('grid', '1', [
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
    b._set('grid', '1', [
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
            phrase: '1',
            prefix: false,
        }, {
            cache: b,
            mask: 1 << 0,
            idx: 1,
            zoom: 1,
            weight: 0.5,
            phrase: '1',
            prefix: false,
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
    a._set('grid', '1', [
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
    b._set('grid', '1', [
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
            phrase: '1',
            prefix: false,
        }, {
            cache: b,
            mask: 1 << 0,
            idx: 20,
            zoom: 0,
            weight: 0.5,
            phrase: '1',
            prefix: false,
        }], {}, function(err, res) {
            assert.ifError(err);
            assert.equal(res.length, 2, 'res length = 2');
            assert.deepEqual(res[0].map(function(f) { return f.id; }), [3,1], '0.relev = 1');
            assert.deepEqual(res[1].map(function(f) { return f.id; }), [4,1], '0.relev = 1');
            assert.end();
        });
    });
})();

// Mask overflow
(function() {
    var a = new Cache('a', 0);
    var b = new Cache('b', 0);
    var c = new Cache('c', 0);

    var grids = [];
    for (var i = 1; i < 10e3; i++) grids.push(Grid.encode({
        id: i,
        x: 0,
        y: 0,
        relev: 1,
        score: 1
    }));
    a._set('grid', '1', grids);

    b._set('grid', '1', [
        Grid.encode({
            id: 1,
            x: 0,
            y: 0,
            relev: 1,
            score: 1
        })
    ]);
    c._set('grid', '1', [
        Grid.encode({
            id: 1,
            x: 0,
            y: 0,
            relev: 1,
            score: 1
        })
    ]);

    test('coalesceMulti mask safe', function(assert) {
        assert.comment('start coalesce (mask: 2)');
        coalesce([{
            cache: a,
            mask: 1 << 2,
            idx: 0,
            zoom: 0,
            weight: 0.33,
            phrase: '1',
            prefix: false
        }, {
            cache: b,
            mask: 1 << 0,
            idx: 1,
            zoom: 0,
            weight: 0.33,
            phrase: '1',
            prefix: false
        }, {
            cache: c,
            mask: 1 << 1,
            idx: 2,
            zoom: 0,
            weight: 0.33,
            phrase: '1',
            prefix: false
        }], {}, function(err, res) {
            assert.ifError(err);
            assert.equal(res.length, 1, 'res length = 1');
            assert.deepEqual(res[0].map(function(f) { return f.id; }), [1, 1, 1], '0.relev = 0.99');
            assert.end();
        });
    });

    test('coalesceMulti mask overflow', function(assert) {
        assert.comment('start coalesce (mask: 18)');
        coalesce([{
            cache: a,
            mask: 1 << 18,
            idx: 0,
            zoom: 0,
            weight: 0.33,
            phrase: '1',
            prefix: false
        }, {
            cache: b,
            mask: 1 << 0,
            idx: 1,
            zoom: 0,
            weight: 0.33,
            phrase: '1',
            prefix: false
        }, {
            cache: c,
            mask: 1 << 1,
            idx: 2,
            zoom: 0,
            weight: 0.33,
            phrase: '1',
            prefix: false
        }], {}, function(err, res) {
            assert.ifError(err);
            assert.equal(res.length, 1, 'res length = 1');
            assert.deepEqual(res[0].map(function(f) { return f.id; }), [1, 1, 1], '0.relev = 0.99');
            assert.end();
        });
    });
})();

