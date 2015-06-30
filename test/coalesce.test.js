var Cache = require('../index.js').Cache;
var coalesce = require('../index.js').Cache.coalesce;
var test = require('tape');

test('coalesce args', function(assert) {
    assert.throws(function() {
        coalesce();
    }, /Arg 1 must be a PhrasematchSubq array/, 'throws');

    assert.throws(function() {
        coalesce([
            {
                cache: new Cache('a', 1),
                idx: 0,
                zoom: 0,
                weight: 0.5,
                shardlevel: 1,
                phrase: 1
            },
            {
                cache: new Cache('b', 1),
                idx: 1,
                zoom: 1,
                weight: 0.5,
                shardlevel: 1,
                phrase: 1
            },
        ]);
    }, /Arg 2 must be an options object/, 'throws');

    assert.throws(function() {
        coalesce([], { centerzxy:[0,0,0] } );
    }, /Arg 3 must be a callback/, 'throws');

    assert.throws(function() {
        coalesce([], { centerzxy:[0,0,0] }, 5 );
    }, /Arg 3 must be a callback/, 'throws');

    assert.end();
});

test('coalesce', function(assert) {
    coalesce([{
        cache: new Cache('a', 1),
        idx: 0,
        zoom: 0,
        weight: 0.5,
        shardlevel: 1,
        phrase: 1
    }, {
        cache: new Cache('b', 1),
        idx: 1,
        zoom: 1,
        weight: 0.5,
        shardlevel: 1,
        phrase: 1
    }], {}, function(err, res) {
        assert.ifError(err);
        assert.equal(typeof res, 'object');
        assert.end();
    });
});
