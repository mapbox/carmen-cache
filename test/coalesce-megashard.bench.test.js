var Cache = require('../index.js').Cache;
var Grid = require('./grid.js');
var coalesce = require('../index.js').Cache.coalesce;
var test = require('tape');
var mp36 = Math.pow(2,36);
var fs = require('fs');

var tmpdir = "/tmp/temp." + Math.random().toString(36).substr(2, 5);
fs.mkdirSync(tmpdir);
var tmpidx = 0;
var tmpfile = function() { return tmpdir + "/" + (tmpidx++) + ".dat"; };

var megashard = tmpfile();

if (!fs.existsSync(megashard)) {
    var writer = new Cache('writer');
    var ids = [];
    for (var i = 0; i < 100; i++) ids.push(i);
    for (var i = 0; i < Math.pow(2,18); i++) writer._set('grid', Cache.shard('....'), "...." + i, ids);
    writer.pack('grid', +Cache.shard('....'), megashard);
}

var c = new Cache('cache');
c.loadSync(megashard, 'grid', +Cache.shard('....'));

// Benchmark loading many phrases
var phrases = [];
for (var i = 1; i < 100; i++) phrases.push("...." + (Math.pow(2,18)-i));

var runs = 50;
var stacks = [{
    cache: c,
    idx: 0,
    zoom: 14,
    weight: 1,
    phrases: phrases
}];
test('coalesceSingle', function(assert) {
    // skip this one for now until I figure out how it maps to the dropping 'phrases'
    assert.end(); return;
    var time = +new Date;
    function run(remaining) {
        if (!remaining) {
            var ops = (+new Date-time)/runs;
            assert.equal(true, true, 'coalesceSingle @ ' + ops + 'ms');
            assert.end();
            return;
        }
        coalesce(stacks, {}, function(err, res) {
            var checks = true;
            checks = checks && res.length === 40;
            if (!checks) {

                assert.fail('Failed checks');
                assert.end();
            } else {
                run(--remaining);
            }
        });
    }
    run(runs);
});

