var Cache = require('../index.js').Cache;
var tape = require('tape');
var fs = require('fs');
var zlib = require('zlib');
var data;

tape('setup', function(assert) {
    zlib.inflate(fs.readFileSync(__dirname + '/fixtures/bench.pbf'), function(err, d) {
        assert.ifError(err);
        assert.equal(d.length, 1143925, 'pbf is 1143925 bytes');
        data = d;
        assert.end();
    });
});

tape('bench load', function(assert) {
    var cache = new Cache('a');
    var time = +new Date;
    // for (var i = 0; i < 256; i++) cache.loadSync(data, 'stat', i);
    for (var i = 0; i < 256; i++) cache.loadAsDict(data, 'stat', i);
    time = (+new Date - time);
    assert.equal(time < 80e3, true, 'loadAsDict x256 took ' + time + 'ms');
    console.log(process.memoryUsage());
    assert.end();
});

