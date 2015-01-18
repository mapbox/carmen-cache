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


