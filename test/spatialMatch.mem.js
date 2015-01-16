var spatialMatch = require('../index.js').Cache.spatialMatch;
var test = require('tape');
var mp39 = Math.pow(2,39);
var mp25 = Math.pow(2,25);

var firstRun;

var args = require('./fixtures/spatialMatch-real-args.json');
var testRet = require('./fixtures/spatialMatch-real-ret.json');

for (var i = 0; i < 1000; i++) test('mem ' + i, function(assert) {
    spatialMatch(args[0], args[1], args[2], args[3], function(err, ret) {
        firstRun = firstRun || process.memoryUsage();
        assert.ifError(err);
        assert.deepEqual(ret, testRet[1]);
        assert.end();
    });
});

test('wait', function(assert) {
    console.log('firstRun', firstRun);
    console.log('endMem', process.memoryUsage());
    setTimeout(function() {
        console.log('end2Mem', process.memoryUsage());
        assert.end();
    }, 10e3);
});

