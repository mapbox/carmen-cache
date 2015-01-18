var Cache = require('../index.js').Cache;
var tape = require('tape');
var fs = require('fs');

tape('#phrasematchDegens', function(assert) {
    var cache = new Cache('a', 1);
    cache.phrasematchDegens([
        [ 681154064 ],
        [ 3790501696 ]
    ], function(err, result) {
        assert.ifError(err);
        assert.deepEqual(result, {
            terms: [ 681154064, 3790501696 ],
            queryidx: { '681154064': 0, '3790501696': 1 },
            querymask: { '681154064': 1, '3790501696': 2 },
            querydist: { '681154064': 0, '3790501696': 0 }
        });
        assert.end();
    });
});

tape('#phrasematchDegens + dist', function(assert) {
    var cache = new Cache('a', 1);
    cache.phrasematchDegens([
        [ 681154065 ],
        [ 3790501696 ]
    ], function(err, result) {
        assert.ifError(err);
        assert.deepEqual(result, {
            terms: [ 681154064, 3790501696 ],
            queryidx: { '681154064': 0, '3790501696': 1 },
            querymask: { '681154064': 1, '3790501696': 2 },
            querydist: { '681154064': 1, '3790501696': 0 }
        });
        assert.end();
    });
});

// Edgecase where there are two degen candidates for the same term.
// The last one currently wins the querydist conflict.
tape('#phrasematchDegens + dist overwrite', function(assert) {
    var cache = new Cache('a', 1);
    cache.phrasematchDegens([
        [ 681154065 ],
        [ 681154064 ]
    ], function(err, result) {
        assert.ifError(err);
        assert.deepEqual(result, {
            terms: [ 681154064, 681154064 ],
            queryidx: { '681154064': 0 },
            querymask: { '681154064': 3 },
            querydist: { '681154064': 0 }
        });
        assert.end();
    });
});


