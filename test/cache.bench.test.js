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

tape('bench loadSync', function(assert) {
    var cache = new Cache('a');
    var time = +new Date;
    var mem = process.memoryUsage().rss;
    for (var i = 0; i < 256; i++) cache.loadSync(data, 'stat', i);
    time = (+new Date - time);
    mem = process.memoryUsage().rss - mem;
    assert.equal(time < 80e3, true, 'loadSync x256 took ' + time + 'ms');
    assert.equal(mem < 2e9, true, 'loadSync rss + ' + mem + ' bytes');
    assert.end();
});

(function() {
var cache = new Cache('a', 256);

[1,2,3,4].forEach(function(j) {
    tape('bench loadSync (LRU) x' + j, function(assert) {
        var mem = process.memoryUsage().rss;
        for (var i = 0 * j; i < 1024 * j; i++) cache.loadSync(data, 'stat', i);
        setTimeout(function() {
            mem = process.memoryUsage().rss - mem;
            if (j === 1) {
                assert.equal(mem < 2e9, true, 'loadSync rss + ' + mem + ' bytes');
            } else {
                assert.equal(mem <= 5e6, true, 'loadSync rss + ' + mem + ' bytes');
            }
            assert.end();
        }, 2000);
    });
});

})();

