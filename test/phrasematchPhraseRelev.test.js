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
            relevs: { '1': 8796118792011777, '2': 7353550946435074 }
        });
        assert.equal((result.relevs[1]/Math.pow(2,48)|0) % Math.pow(2,5), 31, '1.relev');
        assert.equal((result.relevs[1]/Math.pow(2,45)|0) % Math.pow(2,3), 2, '1.count');
        assert.equal((result.relevs[1]/Math.pow(2,33)|0) % Math.pow(2,12), 3, '1.reason');
        assert.equal((result.relevs[1]/Math.pow(2,0)|0) % Math.pow(2,32), 1, '1.id');
        assert.equal((result.relevs[2]/Math.pow(2,48)|0) % Math.pow(2,5), 26, '2.relev');
        assert.equal((result.relevs[2]/Math.pow(2,45)|0) % Math.pow(2,3), 1, '2.count');
        assert.equal((result.relevs[2]/Math.pow(2,33)|0) % Math.pow(2,12), 2, '2.reason');
        assert.equal((result.relevs[2]/Math.pow(2,0)|0) % Math.pow(2,32), 2, '2.id');
        assert.end();
    });
});


