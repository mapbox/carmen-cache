var spatialMatch = require('../index.js').Cache.spatialMatch;
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
    var args = require('./fixtures/spatialMatch-args.json');
    spatialMatch(args[0], args[1], args[2], args[3], function(err, ret) {
        assert.ifError(err);
        assert.deepEqual(ret, require('./fixtures/spatialMatch-ret.json'));
        assert.end();
    });
});

test.skip('real', function(assert) {
    var args = require('./fixtures/spatialMatch-real-args.json');
    var testRet = require('./fixtures/spatialMatch-real-ret.json');
    spatialMatch(args[0], args[1], args[2], args[3], function(err, ret) {
        assert.ifError(err);
        assert.deepEqual(ret, testRet[1]);
        assert.end();
    });
});

