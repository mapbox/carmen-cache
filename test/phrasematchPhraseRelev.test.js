var Cache = require('../index.js').Cache;
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
            relevs: {
                '1': {
                    count: 2,
                    reason: 3,
                    relev: 1,
                    tmprelev: 1000002
                },
                '2': {
                    count: 1,
                    reason: 2,
                    relev: 0.8666666666666667,
                    tmprelev: 866667.6666666667
                }
            }
        });
        assert.end();
    });
});


