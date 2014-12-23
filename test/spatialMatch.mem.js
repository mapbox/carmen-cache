var spatialMatch = require('../index.js').Cache.spatialMatch;
var test = require('tape');
var mp39 = Math.pow(2,39);
var mp25 = Math.pow(2,25);

var firstRun;

for (var i = 0; i < 1000; i++) test('mem ' + i, function(assert) {
    var args = require('./fixtures/spatialMatch-args.json');
    spatialMatch(args[0], args[1], args[2], args[3], function(err, ret) {
        firstRun = firstRun || process.memoryUsage();
        assert.ifError(err);
        // require('fs').writeFileSync('/tmp/res.json', JSON.stringify(ret, null, 2));
        assert.deepEqual(ret, require('./fixtures/spatialMatch-ret.json'));
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

