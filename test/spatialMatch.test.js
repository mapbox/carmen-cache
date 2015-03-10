var spatialMatch = require('../index.js').Cache.spatialMatch;
var Relev = require('./relev');
var Cover = require('./cover');
var test = require('tape');
var mp39 = Math.pow(2,39);
var mp25 = Math.pow(2,25);

test('zero case', function(q) {
    spatialMatch(1, {}, [], [], function(err, res) {
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

    spatialMatch(queryLength, feat, cover, zooms, function(err, ret) {
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
        1: Relev.encode({
            id: 1,
            idx: 0,
            tmpid: 1,
            reason: 1,
            count: 1,
            relev: 1,
            check: true
        }),
        100000001: Relev.encode({
            id: 1,
            idx: 1,
            tmpid: 100000001,
            reason: 2,
            count: 1,
            relev: 1,
            check: true
        }),
        100000002: Relev.encode({
            id: 2,
            idx: 1,
            tmpid: 100000002,
            reason: 2,
            count: 1,
            relev: 1,
            check: true
        })
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

    spatialMatch(queryLength, feat, cover, zooms, function(err, ret) {
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

test('real', function(assert) {
    var args = require('./fixtures/spatialMatch-real-args.json');
    spatialMatch(args[0], args[1], args[2], args[3], function(err, ret) {
        assert.ifError(err);
        var testRet = require('./fixtures/spatialMatch-real-ret.json');
        assert.deepEqual(ret.coalesced, testRet[1].coalesced);
        assert.deepEqual(ret.sets, testRet[1].sets);
        assert.deepEqual(ret.results, testRet[1].results);
        assert.end();
    });
});

