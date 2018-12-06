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
