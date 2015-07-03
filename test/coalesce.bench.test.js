var Cache = require('../index.js').Cache;
var Grid = require('./grid.js');
var coalesce = require('../index.js').Cache.coalesce;
var test = require('tape');

(function() {
    var runs = 50;
    var b = new Cache('b', 0);
    b._set('grid', 0, 3848571113, require('./fixtures/coalesce-bench-single-3848571113.json'));
    var stacks = [{
        cache: b,
        idx: 0,
        zoom: 14,
        weight: 1,
        shardlevel: 0,
        phrase: 3848571113
    }];
    test('coalesceSingle', function(assert) {
        var time = +new Date;
        function run(remaining) {
            if (!remaining) {
                var ops = (+new Date-time)/runs;
                assert.equal(ops < 20, true, 'coalesceSingle @ ' + ops + 'ms');
                assert.end();
                return;
            }
            coalesce(stacks, {}, function(err, res) {
                var checks = true;
                checks = checks && res.length === 40;
                checks = checks && res[0][0].tmpid === 129900;
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
    test('coalesceSingle proximity', function(assert) {
        var time = +new Date;
        function run(remaining) {
            if (!remaining) {
                var ops = (+new Date-time)/runs;
                assert.equal(ops < 20, true, 'coalesceSingle + proximity @ ' + ops + 'ms');
                assert.end();
                return;
            }
            coalesce(stacks, { centerzxy: [14,4893,6001] }, function(err, res) {
                var checks = true;
                checks = checks && res.length === 23;
                checks = checks && res[0][0].x === 4893;
                checks = checks && res[0][0].y === 6001;
                checks = checks && res[0][0].tmpid === 446213;
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
})();

(function() {
    var runs = 50;
    var a = new Cache('a', 0);
    var b = new Cache('b', 0);
    a._set('grid', 0, 1965155344, require('./fixtures/coalesce-bench-multi-1965155344.json'));
    b._set('grid', 0, 3848571113, require('./fixtures/coalesce-bench-multi-3848571113.json'));
    var stacks = [{
        cache: a,
        idx: 0,
        zoom: 12,
        weight: 0.25,
        shardlevel: 0,
        phrase: 1965155344
    }, {
        cache: b,
        idx: 1,
        zoom: 14,
        weight: 0.75,
        shardlevel: 0,
        phrase: 3848571113
    }];
    test('coalesceMulti', function(assert) {
        var time = +new Date;
        function run(remaining) {
            if (!remaining) {
                var ops = (+new Date-time)/runs;
                assert.equal(ops < 40, true, 'coalesceMulti @ ' + ops + 'ms');
                assert.end();
                return;
            }
            coalesce(stacks, {}, function(err, res) {
                var checks = true;
                checks = checks && res.length === 40;
                checks = checks && res[0][0].tmpid === 33593999;
                checks = checks && res[0][1].tmpid === 514584;
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
    test('coalesceMulti proximity', function(assert) {
        var time = +new Date;
        function run(remaining) {
            if (!remaining) {
                var ops = (+new Date-time)/runs;
                assert.equal(ops < 40, true, 'coalesceMulti + proximity @ ' + ops + 'ms');
                assert.end();
                return;
            }
            coalesce(stacks, { centerzxy: [14,4893,6001] }, function(err, res) {
                var checks = true;
                checks = checks && res.length === 40;
                checks = checks && res[0][0].x === 4893;
                checks = checks && res[0][0].y === 6001;
                checks = checks && res[0][0].tmpid === 34000645;
                checks = checks && res[0][1].tmpid === 5156;
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
})();

