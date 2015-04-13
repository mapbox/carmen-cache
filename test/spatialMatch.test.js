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
            reason: 8,
            count: 1,
            relev: 1,
            check: true
        }),
        33554433: Relev.encode({
            id: 1,
            idx: 1,
            reason: 8,
            count: 1,
            relev: 1,
            check: true
        }),
        33554434: Relev.encode({
            id: 2,
            idx: 1,
            reason: 4,
            count: 1,
            relev: 1,
            check: true
        }),
        67108865: Relev.encode({
            id: 1,
            idx: 2,
            reason: 3,
            count: 2,
            relev: 1,
            check: true
        }),
        67108866: Relev.encode({
            id: 2,
            idx: 2,
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
            33554433: feat['33554433'],
            33554434: feat['33554434'],
            67108865: feat['67108865'],
            67108866: feat['67108866']
        });
        assert.deepEqual(ret.results, [ feat['67108866'] ]);
        assert.deepEqual(ret.coalesced, {
            1611137056: [
                1,
                33554433,
                67108865
            ],
            1611153440: [
                1,
                33554434,
                67108866
            ]
        });
        assert.end();
    });
});

test('tied-top', function(assert) {
    var queryLength = 2;
    var feat = {
        33554433: Relev.encode({
            id: 1,
            idx: 1,
            reason: 1,
            count: 1,
            relev: 1,
            check: true
        }),
        33554434: Relev.encode({
            id: 2,
            idx: 1,
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
            33554433: feat['33554433'],
            33554434: feat['33554434']
        });
        assert.deepEqual(ret.results, [ feat['33554433'], feat['33554434'] ]);
        assert.deepEqual(ret.coalesced, {
            1611137056: [
                1,
                33554433,
                33554434
            ]
        });
        assert.end();
    });
});

test('tied-multitop', function(assert) {
    var queryLength = 2;
    var feat = {
        33554433: Relev.encode({
            id: 1,
            idx: 1,
            reason: 1,
            count: 1,
            relev: 1,
            check: true
        }),
        67108865: Relev.encode({
            id: 1,
            idx: 2,
            reason: 1,
            count: 1,
            relev: 1,
            check: true
        }),
        1: Relev.encode({
            id: 1,
            idx: 0,
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
    var groups = [0,1,1];

    spatialMatch(queryLength, feat, cover, zooms, groups, function(err, ret) {
        assert.ifError(err);
        assert.deepEqual(ret.sets, {
            1: feat['1'],
            33554433: feat['33554433'],
            67108865: feat['67108865']
        });
        // Ideally should surface features from both idx 1 + idx 2 as
        // they have maximum relevance score for the same reason/count.
        assert.deepEqual(ret.results, [ feat['33554433'], feat['67108865'] ]);
        assert.deepEqual(ret.coalesced, {
            1611137056: [
                1,
                33554433,
                67108865
            ]
        });
        assert.end();
    });
});

