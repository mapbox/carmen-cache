var Cache = require('../index.js').Cache;
var tape = require('tape');
var fs = require('fs');

tape('#phrasematchDegens', function(assert) {
    var cache = new Cache('a', 1);
    var result = cache.phrasematchDegens([
        [ 681154064 ],
        [ 3790501696 ]
    ]);
    assert.deepEqual(result, {
        terms: [ 681154064, 3790501696 ],
        queryidx: { '681154064': 0, '3790501696': 1 },
        querymask: { '681154064': 1, '3790501696': 2 },
        querydist: { '681154064': 0, '3790501696': 0 }
    });
    assert.end();
});


