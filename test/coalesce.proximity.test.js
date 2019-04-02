'use strict';
// This test positions grids in four quadrant directions (ne, se, sw, nw)
// and then confirms that proximity results,  scoredist and distance are
// calculated correctly when the proximity point is set slightly more toward
// each different quadrant.

const MemoryCache = require('../index.js').MemoryCache;
const Grid = require('./grid.js');
const coalesce = require('../index.js').coalesce;
const scan = require('../index.js').PREFIX_SCAN;
const test = require('tape');

(function() {
    const cache = new MemoryCache('a', 0);
    const ne = {
        id: 1,
        x: 200,
        y: 200,
        relev: 1,
        score: 1
    };
    const se = {
        id: 2,
        x: 200,
        y: 0,
        relev: 1,
        score: 1
    };
    const sw = {
        id: 3,
        x: 0,
        y: 0,
        relev: 1,
        score: 1
    };
    const nw = {
        id: 4,
        x: 0,
        y: 200,
        relev: 1,
        score: 1
    };
    cache._set('1', [
        Grid.encode(ne),
        Grid.encode(se),
        Grid.encode(sw),
        Grid.encode(nw)
    ]);

    // This centerpoint favors the N/S direction over E/W slightly to make
    // expected output order obvious
    test('proximity ne', (t) => {
        coalesce([{
            cache: cache,
            mask: 1 << 0,
            idx: 0,
            zoom: 14,
            weight: 1,
            phrase: '1',
            prefix: scan.disabled
        }], {
            radius: 200,
            centerzxy: [14, 100 + 10, 100 + 15]
        }, (err, res) => {
            t.ifError(err, 'no errors');
            t.deepEqual(res.map((cover) => cover[0].id), [ne.id, nw.id, se.id, sw.id], 'right order');
            t.deepEqual(res.map((cover) => Math.floor(cover[0].distance)), [123, 139, 146, 159], 'distances check out');
            t.end();
        });
    });

    test('proximity se', (t) => {
        coalesce([{
            cache: cache,
            mask: 1 << 0,
            idx: 0,
            zoom: 14,
            weight: 1,
            phrase: '1',
            prefix: scan.disabled
        }], {
            radius: 200,
            centerzxy: [14, 100 + 10, 100 - 15]
        }, (err, res) => {
            t.ifError(err, 'no errors');
            t.deepEqual(res.map((cover) => cover[0].id), [se.id, sw.id, ne.id, nw.id], 'right order');
            t.deepEqual(res.map((cover) => Math.floor(cover[0].distance)), [123, 139, 146, 159], 'distances check out');
            t.end();
        });
    });

    test('proximity sw', (t) => {
        coalesce([{
            cache: cache,
            mask: 1 << 0,
            idx: 0,
            zoom: 14,
            weight: 1,
            phrase: '1',
            prefix: scan.disabled
        }], {
            radius: 200,
            centerzxy: [14, 100 - 10, 100 - 15]
        }, (err, res) => {
            t.ifError(err, 'no errors');
            t.deepEqual(res.map((cover) => cover[0].id), [sw.id, se.id, nw.id, ne.id], 'right order');
            t.deepEqual(res.map((cover) => Math.floor(cover[0].distance)), [123, 139, 146, 159], 'distances check out');
            t.end();
        });
    });

    test('proximity nw', (t) => {
        coalesce([{
            cache: cache,
            mask: 1 << 0,
            idx: 0,
            zoom: 14,
            weight: 1,
            phrase: '1',
            prefix: scan.disabled
        }], {
            radius: 200,
            centerzxy: [14, 100 - 10, 100 + 15]
        }, (err, res) => {
            t.ifError(err, 'no errors');
            t.deepEqual(res.map((cover) => cover[0].id), [nw.id, ne.id, sw.id, se.id], 'right order');
            t.deepEqual(res.map((cover) => Math.floor(cover[0].distance)), [123, 139, 146, 159], 'distances check out');
            t.end();
        });
    });
})();

(function() {
    const cache = new MemoryCache('a', 0);
    const cov = {
        id: 1,
        x: 1,
        y: 1,
        relev: 1,
        score: 0
    };
    cache._set('1', [Grid.encode(cov)]);


    test('Calculates distance correctly for features on same tile as proximity', (t) => {
        coalesce([{
            cache: cache,
            mask: 1 << 0,
            idx: 0,
            zoom: 14,
            weight: 1,
            phrase: '1',
            prefix: scan.disabled
        }], {
            radius: 400,
            centerzxy: [14, 1, 1]
        }, (err, res) => {
            t.ifError(err, 'no errors');
            t.equal(res[0][0].distance, 0, 'Distance for a feature on the same cover as the proximity point should be 0');
            t.equal(res[0][0].scoredist, 643.5016267477292, 'Scoredist shoud be 643.5016267477292');
            t.end();
        });
    });

    test('Calculates distance correctly for features 1 tile away from proximity', (t) => {
        coalesce([{
            cache: cache,
            mask: 1 << 0,
            idx: 0,
            zoom: 14,
            weight: 1,
            phrase: '1',
            prefix: scan.disabled
        }], {
            radius: 400,
            centerzxy: [14, 1, 2]
        }, (err, res) => {
            t.ifError(err, 'no errors');
            t.equal(res[0][0].distance, 1, 'Distance for a feature 1 tile away from proximity point should be 1');
            t.equal(res[0][0].scoredist, 321.7508133738646, 'Scoredist shoud be 321.7508133738646');
            t.end();
        });
    });
})();


(function() {
    const cache = new MemoryCache('a', 0);
    const cov = {
        id: 1,
        x: 0,
        y: 0,
        relev: 1,
        score: 1
    };
    cache._set('1', [Grid.encode(cov)]);

    test('Calculates non-cachec proximity', (t) => {
        coalesce([{
            cache: cache,
            mask: 1 << 0,
            idx: 0,
            zoom: 15,
            weight: 1,
            phrase: '1',
            prefix: scan.disabled
        }], {
            radius: 200,
            centerzxy: [15, 160, 0]
        }, (err, res) => {
            t.ifError(err, 'no errors');
            t.equal(res[0][0].scoredist, 1.5223087695899975, 'Calculate z15 scoredist');
            t.end();
        });
    });
})();

(function() {
    const cachea = new MemoryCache('a', 0);
    const cacheb = new MemoryCache('b', 0);
    const cov = {
        id: 1,
        x: 0,
        y: 0,
        relev: 1,
        score: 1
    };
    cachea._set('1', [Grid.encode(cov)], [0]);
    cacheb._set('2', [Grid.encode(cov)], [0]);

    test('Coalesce single should not penalize nearby language mismatches', (t) => {
        coalesce([{
            cache: cachea,
            mask: 1 << 0,
            idx: 0,
            zoom: 14,
            weight: 1,
            phrase: '1',
            prefix: scan.disabled,
            languages:[1]
        }], {
            radius: 200,
            centerzxy: [14, 0, 0]
        }, (err, res) => {
            t.ifError(err, 'no errors');
            t.equal(res[0][0].relev, 1, 'relev is not penalized');
            t.end();
        });
    });

    test('Coalesce single should penalize distant language mismatches', (t) => {
        coalesce([{
            cache: cachea,
            mask: 1 << 0,
            idx: 0,
            zoom: 14,
            weight: 1,
            phrase: '1',
            prefix: scan.disabled,
            languages:[1]
        }], {
            radius: 200,
            centerzxy: [14, 200, 0]
        }, (err, res) => {
            t.ifError(err, 'no errors');
            t.equal(res[0][0].relev, 0.96, 'relev is not penalized');
            t.end();
        });
    });

    test('Coalesce multi should not penalize nearby language mismatches', (t) => {
        coalesce([{
            cache: cachea,
            mask: 1 << 0,
            idx: 0,
            zoom: 14,
            weight: 1,
            phrase: '1',
            prefix: scan.disabled,
            languages:[1]
        }, {
            cache: cacheb,
            mask: 1 << 1,
            idx: 1,
            zoom: 12,
            weight: 1,
            phrase: '2',
            prefix: scan.disabled,
            languages:[1]
        }], {
            radius: 200,
            centerzxy: [14, 0, 0]
        }, (err, res) => {
            t.ifError(err, 'no errors');
            t.equal(res[0][0].relev, 1, 'relev is not penalized');
            t.equal(res[0][1].relev, 1, 'relev is not penalized');
            t.end();
        });
    });

    test('Coalesce multi should not penalize nearby language mismatches', (t) => {
        coalesce([{
            cache: cachea,
            mask: 1 << 0,
            idx: 0,
            zoom: 14,
            weight: 1,
            phrase: '1',
            prefix: scan.disabled,
            languages:[1]
        }, {
            cache: cacheb,
            mask: 1 << 1,
            idx: 1,
            zoom: 12,
            weight: 1,
            phrase: '2',
            prefix: scan.disabled,
            languages:[1]
        }], {
            radius: 200,
            centerzxy: [14, 200, 0]
        }, (err, res) => {
            t.ifError(err, 'no errors');
            t.equal(res[0][0].relev, 0.96, 'relev is penalized');
            t.equal(res[0][1].relev, 0.96, 'relev is penalized');
            t.end();
        });
    });
})();
