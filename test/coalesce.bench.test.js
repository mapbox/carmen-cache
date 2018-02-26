'use strict';
const Cache = require('../index.js').MemoryCache;
const coalesce = require('../index.js').coalesce;
const test = require('tape');

(function() {
    // asan makes everything slow, so skip benching
    if (process.env.ASAN_OPTIONS) return;

    const runs = 50;
    const b = new Cache('b');
    b._set('3848571113', require('./fixtures/coalesce-bench-single-3848571113.json'));
    // console.log('# pack size', b.pack('grid', '3848571113').length);
    const stacks = [{
        cache: b,
        idx: 0,
        zoom: 14,
        weight: 1,
        phrase: '3848571113',
        prefix: false,
        mask: 1 << 0
    }];
    test('coalesceSingle', (assert) => {
        const time = +new Date;
        function run(remaining) {
            if (!remaining) {
                const ops = (+new Date - time) / runs;
                let expected_ops = 30;
                if (process.env.BUILDTYPE === 'debug') {
                    expected_ops = 500;
                }
                assert.equal(ops < expected_ops, true, 'coalesceSingle @ ' + ops + 'ms < ' + expected_ops + 'ms');
                assert.end();
                return;
            }
            coalesce(stacks, {}, (err, res) => {
                let checks = true;
                checks = checks && res.length === 37;
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
    // Coalesce can optionally take a proximity parameter, and use this as a parameter.
    // This can be more computationally expensive which is why it is a part of the benchmark test.
    test('coalesceSingle proximity', (assert) => {
        const time = +new Date;
        function run(remaining) {
            if (!remaining) {
                const ops = (+new Date - time) / runs;
                let expected_ops = 30;
                if (process.env.BUILDTYPE === 'debug') {
                    expected_ops = 500;
                }
                assert.equal(ops < expected_ops, true, 'coalesceSingle + proximity @ ' + ops + 'ms < ' + expected_ops + 'ms');
                assert.end();
                return;
            }
            coalesce(stacks, { centerzxy: [14,4893,6001] }, (err, res) => {
                let checks = true;
                checks = checks && res.length === 38;
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
    // asan makes everything slow, so skip benching for the sake of letting Travis build
    if (process.env.ASAN_OPTIONS) return;

    const runs = 50;
    const a = new Cache('a', 0);
    const b = new Cache('b', 0);
    a._set('1965155344', require('./fixtures/coalesce-bench-multi-1965155344.json'));
    b._set('3848571113', require('./fixtures/coalesce-bench-multi-3848571113.json'));
    const stacks = [{
        cache: a,
        mask: 1 << 0,
        idx: 0,
        zoom: 12,
        weight: 0.25,
        phrase: '1965155344',
        prefix: false
    }, {
        cache: b,
        mask: 1 << 1,
        idx: 1,
        zoom: 14,
        weight: 0.75,
        phrase: '3848571113',
        prefix: false
    }];
    test('coalesceMulti', (assert) => {
        const time = +new Date;
        function run(remaining) {
            if (!remaining) {
                const ops = (+new Date - time) / runs;
                let expected_ops = 60;
                if (process.env.BUILDTYPE === 'debug') {
                    expected_ops = 1000;
                }
                assert.equal(ops < expected_ops, true, 'coalesceMulti @ ' + ops + 'ms < ' + expected_ops + 'ms');
                assert.end();
                return;
            }
            coalesce(stacks, {}, (err, res) => {
                let checks = true;
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
    test('coalesceMulti proximity', (assert) => {
        const time = +new Date;
        function run(remaining) {
            if (!remaining) {
                const ops = (+new Date - time) / runs;
                let expected_ops = 60;
                if (process.env.BUILDTYPE === 'debug') {
                    expected_ops = 1000;
                }
                assert.equal(ops < expected_ops, true, 'coalesceMulti + proximity @' + ops + 'ms < ' + expected_ops + 'ms');
                assert.end();
                return;
            }
            coalesce(stacks, { centerzxy: [14,4893,6001] }, (err, res) => {
                let checks = true;
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
