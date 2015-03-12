var spatialMatch = require('../index.js').Cache.spatialMatch;
var Relev = require('./relev');
var Cover = require('./cover');
var test = require('tape');
var mp39 = Math.pow(2,39);
var mp25 = Math.pow(2,25);

test('zero case', function(q) {
    spatialMatch(1, {}, [], [], [], function(err, res) {
        q.deepEqual(res, {
            coalesced:{},
            results:[],
            sets:{}
        });
        q.end();
    });
});

test('unit', function(assert) {
    var queryLength = 4;
    var feat = {
        1: Relev.encode({
            id: 1,
            idx: 0,
            tmpid: 1,
            reason: 8,
            count: 1,
            relev: 1,
            check: true
        }),
        100000001: Relev.encode({
            id: 1,
            idx: 1,
            tmpid: 100000001,
            reason: 8,
            count: 1,
            relev: 1,
            check: true
        }),
        100000002: Relev.encode({
            id: 2,
            idx: 1,
            tmpid: 100000002,
            reason: 4,
            count: 1,
            relev: 1,
            check: true
        }),
        200000001: Relev.encode({
            id: 1,
            idx: 2,
            tmpid: 200000001,
            reason: 3,
            count: 2,
            relev: 1,
            check: true
        }),
        200000002: Relev.encode({
            id: 2,
            idx: 2,
            tmpid: 200000002,
            reason: 3,
            count: 2,
            relev: 1,
            check: true
        })
    };
    var cover = [
        [
            Cover.encode({ x: 32, y: 32, id: 1 }),
            Cover.encode({ x: 33, y: 32, id: 1 })
        ],
        [
            Cover.encode({ x: 32, y: 32, id: 1 }),
            Cover.encode({ x: 33, y: 32, id: 2 })
        ],
        [
            Cover.encode({ x: 32, y: 32, id: 1 }),
            Cover.encode({ x: 33, y: 32, id: 2 })
        ]
    ];
    var zooms = [6,6,6];
    var groups = [0,1,2];

    spatialMatch(queryLength, feat, cover, zooms, groups, function(err, ret) {
        assert.ifError(err);
        assert.deepEqual(ret.sets, {
            1: feat['1'],
            100000001: feat['100000001'],
            100000002: feat['100000002'],
            200000001: feat['200000001'],
            200000002: feat['200000002']
        });
        assert.deepEqual(ret.results, [ feat['200000002'] ]);
        assert.deepEqual(ret.coalesced, {
            1611137056: [
                1,
                100000001,
                200000001
            ],
            1611153440: [
                1,
                100000002,
                200000002
            ]
        });
        assert.end();
    });
});

test('tied-top', function(assert) {
    var queryLength = 2;
    var feat = {
        100000001: Relev.encode({
            id: 1,
            idx: 1,
            tmpid: 100000001,
            reason: 1,
            count: 1,
            relev: 1,
            check: true
        }),
        100000002: Relev.encode({
            id: 2,
            idx: 1,
            tmpid: 100000002,
            reason: 1,
            count: 1,
            relev: 1,
            check: true
        }),
        1: Relev.encode({
            id: 1,
            idx: 0,
            tmpid: 1,
            reason: 2,
            count: 1,
            relev: 1,
            check: true
        }),
    };
    var cover = [
        [
            Cover.encode({ x: 32, y: 32, id: 1 })
        ],
        [
            Cover.encode({ x: 32, y: 32, id: 1 }),
            Cover.encode({ x: 32, y: 32, id: 2 })
        ],
    ];
    var zooms = [6,6];
    var groups = [0,1,2];

    spatialMatch(queryLength, feat, cover, zooms, groups, function(err, ret) {
        assert.ifError(err);
        assert.deepEqual(ret.sets, {
            1: feat['1'],
            100000001: feat['100000001'],
            100000002: feat['100000002']
        });
        assert.deepEqual(ret.results, [ feat['100000001'], feat['100000002'] ]);
        assert.deepEqual(ret.coalesced, {
            1611137056: [
                1,
                100000001,
                100000002
            ]
        });
        assert.end();
    });
});

test('tied-multitop', function(assert) {
    var queryLength = 2;
    var feat = {
        100000001: Relev.encode({
            id: 1,
            idx: 1,
            tmpid: 100000001,
            reason: 1,
            count: 1,
            relev: 1,
            check: true
        }),
        200000001: Relev.encode({
            id: 1,
            idx: 2,
            tmpid: 200000002,
            reason: 1,
            count: 1,
            relev: 1,
            check: true
        }),
        1: Relev.encode({
            id: 1,
            idx: 0,
            tmpid: 1,
            reason: 2,
            count: 1,
            relev: 1,
            check: true
        }),
    };
    var cover = [
        [
            Cover.encode({ x: 32, y: 32, id: 1 })
        ],
        [
            Cover.encode({ x: 32, y: 32, id: 1 })
        ],
        [
            Cover.encode({ x: 32, y: 32, id: 1 })
        ],
    ];
    var zooms = [6,6,6];
    var groups = [0,1,2];

    spatialMatch(queryLength, feat, cover, zooms, groups, function(err, ret) {
        assert.ifError(err);
        assert.deepEqual(ret.sets, {
            1: feat['1'],
            100000001: feat['100000001'],
            200000001: feat['200000001']
        });
        // Ideally should surface features from both idx 1 + idx 2 as
        // they have maximum relevance score for the same reason/count.
        assert.deepEqual(ret.results, [ feat['100000001'], feat['200000001'] ]);
        assert.deepEqual(ret.coalesced, {
            1611137056: [
                1,
                100000001,
                200000001
            ]
        });
        assert.end();
    });
});

test('real', function(assert) {
    var args = require('./fixtures/spatialMatch-real-args.json');
    spatialMatch(args[0], args[1], args[2], args[3], [0,1,2,3,4,5], function(err, ret) {
        assert.ifError(err);
        if (process.env.UPDATE) {
            require('fs').writeFileSync(__dirname + '/fixtures/spatialMatch-real-ret.json', JSON.stringify(ret, null, 2));
        }
        var expected = require('./fixtures/spatialMatch-real-ret.json');
        assert.deepEqual(ret.coalesced, expected.coalesced);
        assert.deepEqual(ret.sets, expected.sets);
        assert.deepEqual(ret.results.length, expected.results.length);
        assert.end();
    });
});

