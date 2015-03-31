var Cache = require('../index.js').Cache;
var Relev = require('./relev');
var tape = require('tape');
var fs = require('fs');

tape('#phrasematchPhraseRelev', function(assert) {
    var cache = new Cache('a', 0);
    var phrases = [ 1, 2 ];
    var queryidx = {
        10000: 0,
        20000: 1
    };
    var querymask = {
        10000: 1 << 0,
        20000: 1 << 1
    };
    var querydist = {
        10000: 0,
        20000: 0
    };
    cache._set('phrase', 0, 1, [10009,20006]);
    cache._set('phrase', 0, 2, [20013,30002]);
    cache.phrasematchPhraseRelev(phrases, queryidx, querymask, querydist, function(err, result) {
        assert.ifError(err);
        assert.deepEqual(result, {
            result: [ 1, 2 ],
            relevs: { '1': 8796118792011777, '2': 7353550946435074 }
        });
        var r1 = new Relev(result.relevs[1]);
        var r2 = new Relev(result.relevs[2]);
        assert.equal(r1.relev, 1, '1.relev');
        assert.equal(r1.count, 2, '1.count');
        assert.equal(r1.reason, 3, '1.reason');
        assert.equal(r1.id, 1, '1.id');
        assert.equal(r2.relev, 0.8387096774193549, '2.relev');
        assert.equal(r2.count, 1, '2.count');
        assert.equal(r2.reason, 2, '2.reason');
        assert.equal(r2.id, 2, '2.id');
        assert.end();
    });
});

tape('#phrasematchPhraseRelev', function(assert) {
    var cache = new Cache('a', 0);
    var phrases = [ 1, 2 ];
    var queryidx = {
        10000: 0,
        20000: 1
    };
    var querymask = {
        10000: 1 << 0,
        20000: 1 << 1
    };
    var querydist = {
        10000: 1,
        20000: 1
    };
    cache._set('phrase', 0, 1, [10009,20006]);
    cache._set('phrase', 0, 2, [20013,30002]);
    cache.phrasematchPhraseRelev(phrases, queryidx, querymask, querydist, function(err, result) {
        assert.ifError(err);
        assert.deepEqual(result, {
            result: [ 1, 2 ],
            relevs: { '1': 8514643815301121, '2': 7353550946435074 }
        });
        var r1 = new Relev(result.relevs[1]);
        var r2 = new Relev(result.relevs[2]);
        assert.equal(r1.relev, 0.967741935483871, '1.relev');
        assert.equal(r1.count, 2, '1.count');
        assert.equal(r1.reason, 3, '1.reason');
        assert.equal(r1.id, 1, '1.id');
        assert.equal(r2.relev, 0.8387096774193549, '2.relev');
        assert.equal(r2.count, 1, '2.count');
        assert.equal(r2.reason, 2, '2.reason');
        assert.equal(r2.id, 2, '2.id');
        assert.end();
    });
});

tape('#phrasematchPhraseRelev (query: "a a b", phrase: "a b")', function(assert) {
    var cache = new Cache('a', 0);
    var phrases = [ 1 ];
    var queryidx = { 10000: 0, 20000: 2 };
    var querymask = { 10000: (1 << 0) + (1 << 1), 20000: 1 << 2 };
    var querydist = { 10000: 0, 20000: 0 };
    cache._set('phrase', 0, 1, [10009,20006]);
    cache.phrasematchPhraseRelev(phrases, queryidx, querymask, querydist, function(err, result) {
        assert.ifError(err);
        assert.deepEqual(result.result, [1]);
        assert.deepEqual(new Relev(result.relevs['1']), {
            id: 1,
            idx: 0,
            tmpid: 1,
            reason: 7,
            count: 2,
            relev: 1,
            check: true
        });
        assert.end();
    });
});

tape('#phrasematchPhraseRelev (query: "a a c b", phrase: "a b")', function(assert) {
    var cache = new Cache('a', 0);
    var phrases = [ 1 ];
    var queryidx = { 10000: 0, 20000: 3 };
    var querymask = { 10000: (1 << 0) + (1 << 1), 20000: 1 << 3 };
    var querydist = { 10000: 0, 20000: 0 };
    cache._set('phrase', 0, 1, [10009,20006]);
    cache.phrasematchPhraseRelev(phrases, queryidx, querymask, querydist, function(err, result) {
        assert.ifError(err);
        assert.deepEqual(result.result, [1]);
        assert.deepEqual(new Relev(result.relevs['1']), {
            id: 1,
            idx: 0,
            tmpid: 1,
            reason: 11,
            count: 2,
            relev: 0.8709677419354839,
            check: true
        });
        assert.end();
    });
});

tape('#phrasematchPhraseRelev (query: "a c", phrase: "a b c")', function(assert) {
    var cache = new Cache('a', 0);
    var phrases = [ 1 ];

    //Position of terms in index
    var queryidx = { 10000: 0, 30000: 1 };
    var querymask = { 10000: 1, 2000: 0, 30000: 1 << 2 };
    var querydist = { 10000: 0, 30000: 0 };
    cache._set('phrase', 0, 1, [10009,20009,30009]);
    cache.phrasematchPhraseRelev(phrases, queryidx, querymask, querydist, function(err, result) {
        assert.ifError(err);
        assert.deepEqual(result.result, [1]);
        assert.deepEqual(new Relev(result.relevs['1']), {
            id: 1,
            idx: 0,
            tmpid: 1,
            reason: 5,
            count: 2,
            relev: 0.6451612903225806,
            check: true
        });
        assert.end();
    });
});

// Queries that have extraneous terms are penalized 0.10 for each term
tape('#phrasematchPhraseRelev (query: "a c d e f g b", phrase: "a b")', function(assert) {
    var cache = new Cache('a', 0);
    var phrases = [ 1 ];

    //Position of terms in index
    var queryidx = { 10000: 0, 20000: 6 };
    var querymask = { 10000: 1, 20000: 1 << 6, 30000: 0, 40000: 0, 50000: 0, 60000: 0, 70000: 0 };
    var querydist = { 10000: 0, 20000: 0 };
    cache._set('phrase', 0, 1, [10009,20009]);
    cache.phrasematchPhraseRelev(phrases, queryidx, querymask, querydist, function(err, result) {
        assert.ifError(err);
        assert.deepEqual(result.result, [1]);
        assert.deepEqual(new Relev(result.relevs['1']), {
            id: 1,
            idx: 0,
            tmpid: 1,
            reason: 65,
            count: 2,
            relev: 0.4838709677419355,
            check: true
        });
        assert.end();
    });
});

// Negative relevs should fail gracefully (This produces a relev of -415.250000)
tape('#phrasematchPhraseRelev (query: "a c", phrase: "a b c d e")', function(assert) {
    var cache = new Cache('a', 0);
    var phrases = [ 1 ];

    //Position of terms in index
    var queryidx = { 10000: 0, 30000: 2 };
    var querymask = { 10000: 1, 20000: 0, 30000: 1 << 2, 40000: 0, 50000: 0 };
    var querydist = { 10000: 0, 30000: 0 };
    cache._set('phrase', 0, 1, [10009,20009,30009,40009,50009]);
    cache.phrasematchPhraseRelev(phrases, queryidx, querymask, querydist, function(err, result) {
        assert.ifError(err);
        assert.deepEqual(result.result, []);
        assert.end();
    });
});
