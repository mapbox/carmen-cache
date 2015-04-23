var Cache = require('../index.js').Cache;
var Relev = require('./relev');
var tape = require('tape');
var dataterm = require('./dataterm');
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
    cache.phrasematchPhraseRelev(2, phrases, queryidx, querymask, querydist, function(err, result) {
        assert.ifError(err);
        assert.deepEqual(result, {
            result: [ 1, 2 ],
            relevs: {
                1: Relev.encode({
                    id: 1,
                    idx: 0,
                    reason: parseInt('11',2),
                    count: 2,
                    relev: 1
                }),
                2: Relev.encode({
                    id: 2,
                    idx: 0,
                    reason: parseInt('10',2),
                    count: 1,
                    relev: 0.7741935483870968
                })
            }
        });
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
    cache.phrasematchPhraseRelev(2, phrases, queryidx, querymask, querydist, function(err, result) {
        assert.ifError(err);
        assert.deepEqual(result, {
            result: [ 1, 2 ],
            relevs: {
                1: Relev.encode({
                    id: 1,
                    idx: 0,
                    reason: parseInt('11',2),
                    count: 2,
                    relev: 1
                }),
                2: Relev.encode({
                    id: 2,
                    idx: 0,
                    reason: parseInt('10',2),
                    count: 1,
                    relev: 0.7741935483870968
                })
            }
        });
        assert.end();
    });
});

tape('#phrasematchPhraseRelev (query: "100 a b", phrase: "[1-100] a b")', function(assert) {
    var cache = new Cache('a', 0);
    var phrases = [ 1 ];
    var queryidx = { 1600: 0, 10000: 1, 20000: 2 };
    var querymask = { 1600: 1 << 0, 10000: 1 << 1, 20000: 1 << 2 };
    var querydist = { 1600: 0, 10000: 0, 20000: 0 };
    cache._set('phrase', 0, 1, [
        dataterm.encodeData({type:'range',min:1,max:100}),
        10009,
        20006
    ]);
    cache.phrasematchPhraseRelev(3, phrases, queryidx, querymask, querydist, function(err, result) {
        assert.ifError(err);
        assert.deepEqual(result.result, [1]);
        assert.deepEqual(new Relev(result.relevs['1']), {
            id: 1,
            idx: 0,
            tmpid: 1,
            reason: parseInt('111', 2),
            count: 3,
            relev: 1,
            check: true
        });
        assert.end();
    });
});

tape('#phrasematchPhraseRelev (query: "100 a", phrase: "[1-100] a b")', function(assert) {
    var cache = new Cache('a', 0);
    var phrases = [ 1 ];
    var queryidx = { 1600: 0, 10000: 1 };
    var querymask = { 1600: 1 << 0, 10000: 1 << 1 };
    var querydist = { 1600: 0, 10000: 0 };
    cache._set('phrase', 0, 1, [
        dataterm.encodeData({type:'range',min:1,max:100}),
        10009,
        20006
    ]);
    cache.phrasematchPhraseRelev(2, phrases, queryidx, querymask, querydist, function(err, result) {
        assert.ifError(err);
        assert.deepEqual(result.result, [1]);
        assert.deepEqual(new Relev(result.relevs['1']), {
            id: 1,
            idx: 0,
            tmpid: 1,
            reason: parseInt('11', 2),
            count: 2,
            relev: 1,
            check: true
        });
        assert.end();
    });
});

tape('#phrasematchPhraseRelev (query: "100 a", phrase: "[1-100] 100")', function(assert) {
    var cache = new Cache('a', 0);
    var phrases = [ 1 ];
    var queryidx = { 1600: 0 };
    var querymask = { 1600: parseInt('11',2) };
    var querydist = { 1600: 0 };
    cache._set('phrase', 0, 1, [
        dataterm.encodeData({type:'range',min:1,max:100}),
        1615
    ]);
    cache.phrasematchPhraseRelev(2, phrases, queryidx, querymask, querydist, function(err, result) {
        assert.ifError(err);
        assert.deepEqual(result.result, [1]);
        assert.deepEqual(new Relev(result.relevs['1']), {
            id: 1,
            idx: 0,
            tmpid: 1,
            reason: parseInt('11', 2),
            count: 1,
            relev: 1,
            check: true
        });
        assert.end();
    });
});


tape('#phrasematchPhraseRelev (query: "110 a b", phrase: "[1-100] a b")', function(assert) {
    var cache = new Cache('a', 0);
    var phrases = [ 1 ];
    var queryidx = { 1760: 0, 10000: 1, 20000: 2 };
    var querymask = { 1760: 1 << 0, 10000: 1 << 1, 20000: 1 << 2 };
    var querydist = { 1760: 0, 10000: 0, 20000: 0 };
    cache._set('phrase', 0, 1, [
        dataterm.encodeData({type:'range',min:1,max:100}),
        10009,
        20006
    ]);
    cache.phrasematchPhraseRelev(3, phrases, queryidx, querymask, querydist, function(err, result) {
        assert.ifError(err);
        assert.deepEqual(result.result, [1]);
        assert.deepEqual(new Relev(result.relevs['1']), {
            id: 1,
            idx: 0,
            tmpid: 1,
            reason: parseInt('110', 2),
            count: 2,
            relev: 0.8064516129032258,
            check: true
        });
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
    cache.phrasematchPhraseRelev(3, phrases, queryidx, querymask, querydist, function(err, result) {
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
    cache.phrasematchPhraseRelev(4, phrases, queryidx, querymask, querydist, function(err, result) {
        assert.ifError(err);
        assert.deepEqual(result.result, [1]);
        assert.deepEqual(new Relev(result.relevs['1']), {
            id: 1,
            idx: 0,
            tmpid: 1,
            reason: 3,
            count: 1,
            relev: 0.5806451612903226,
            check: true
        });
        assert.end();
    });
});


