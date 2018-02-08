'use strict';
const MemoryCache = require('../index.js').MemoryCache;
const RocksDBCache = require('../index.js').RocksDBCache;
const Grid = require('./grid.js');
const coalesce = require('../index.js').coalesce;
const test = require('tape');
const fs = require('fs');

const tmpdir = '/tmp/temp.' + Math.random().toString(36).substr(2, 5);
fs.mkdirSync(tmpdir);
let tmpidx = 0;
const tmpfile = function() { return tmpdir + '/' + (tmpidx++) + '.dat'; };

const toRocksCache = function(memcache) {
    const pack = tmpfile();
    memcache.pack(pack);
    return new RocksDBCache(memcache.id + '.rocks', pack);
};

test('coalesce args', (assert) => {
    assert.throws(() => {
        coalesce();
    }, /Expects 3 arguments/, 'throws');

    assert.throws(() => {
        coalesce([]);
    }, /Expects 3 arguments/, 'throws');

    assert.throws(() => {
        coalesce([], {} );
    }, /Expects 3 arguments/, 'throws');

    assert.throws(() => {
        coalesce([{}], {}, () => {} );
    }, /missing idx property/, 'throws');

    assert.throws(() => {
        coalesce([-1], {}, () => {} );
    }, /All items in array must be valid PhrasematchSubq objects/, 'throws');

    assert.throws(() => {
        coalesce(undefined, {}, () => {} );
    }, /Arg 1 must be a PhrasematchSubq array/, 'throws');

    assert.throws(() => {
        coalesce([], {}, () => {} );
    }, /Arg 1 must be an array with one or more/, 'throws');

    assert.throws(() => {
        coalesce([undefined], {}, () => {} );
    }, /All items in array must be valid PhrasematchSubq objects/, 'throws');

    assert.throws(() => {
        coalesce([null], {}, () => {} );
    }, /All items in array must be valid PhrasematchSubq objects/, 'throws');

    const valid_subq = {
        cache: new MemoryCache('a'),
        mask: 1 << 0,
        idx: 0,
        zoom: 2,
        weight: 1,
        phrase: 'a',
        prefix: false
    };

    assert.throws(() => {
        coalesce([valid_subq],undefined,() => {});
    }, /Arg 2 must be an options object/, 'throws');

    assert.throws(() => {
        coalesce([valid_subq],undefined,() => {});
    }, /Arg 2 must be an options object/, 'throws');

    if (process.versions.node[0] !== '0') {
        assert.throws(() => {
            coalesce([Object.assign({},valid_subq,{ idx:-1 })],{},() => {});
        }, /encountered idx value too large to fit/, 'throws');

        assert.throws(() => {
            coalesce([Object.assign({},valid_subq,{ idx:null })],{},() => {});
        }, /value must be a number/, 'throws');

        assert.throws(() => {
            coalesce([Object.assign({},valid_subq,{ zoom:-1 })],{},() => {});
        }, /encountered zoom value too large to fit/, 'throws');

        assert.throws(() => {
            coalesce([Object.assign({},valid_subq,{ zoom:null })],{},() => {});
        }, /value must be a number/, 'throws');

        assert.throws(() => {
            coalesce([Object.assign({},valid_subq,{ zoom:-1 })],{},() => {});
        }, /encountered zoom value too large to fit/, 'throws');

        assert.throws(() => {
            coalesce([Object.assign({},valid_subq,{ mask:null })],{},() => {});
        }, /value must be a number/, 'throws');

        assert.throws(() => {
            coalesce([Object.assign({},valid_subq,{ mask:-1 })],{},() => {});
        }, /encountered mask value too large to fit/, 'throws');

        assert.throws(() => {
            coalesce([Object.assign({},valid_subq,{ weight:null })],{},() => {});
        }, /weight value must be a number/, 'throws');

        assert.throws(() => {
            coalesce([Object.assign({},valid_subq,{ weight:-1 })],{},() => {});
        }, /encountered weight value too large to fit in double/, 'throws');

        assert.throws(() => {
            coalesce([Object.assign({},valid_subq,{ phrase:null })],{},() => {});
        }, /phrase value must be a string/, 'throws');

        assert.throws(() => {
            coalesce([Object.assign({},valid_subq,{ phrase:'' })],{},() => {});
        }, /encountered invalid phrase/, 'throws');

        assert.throws(() => {
            coalesce([Object.assign({},valid_subq,{ cache:null })],{},() => {});
        }, /cache value must be a Cache object/, 'throws');

        assert.throws(() => {
            coalesce([Object.assign({},valid_subq,{ cache:{} })],{},() => {});
        }, /cache value must be/, 'throws');

        const valid_stack = [
            {
                cache: new MemoryCache('a'),
                mask: 1 << 0,
                idx: 0,
                zoom: 0,
                weight: 0.5,
                phrase: '1',
                prefix: false
            },
            {
                cache: new MemoryCache('b'),
                mask: 1 << 1,
                idx: 1,
                zoom: 1,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }
        ];

        assert.throws(() => {
            coalesce(valid_stack.concat([Object.assign({},valid_subq,{ cache:null })]),{},() => {});
        }, /cache value must be a Cache object/, 'throws');

    }

    assert.throws(() => {
        coalesce([valid_subq], { radius:null },() => {} );
    }, /radius must be a number/, 'throws');

    assert.throws(() => {
        coalesce([valid_subq], { radius:5e9 },() => {} );
    }, /encountered radius too large to fit in unsigned/, 'throws');

    assert.throws(() => {
        coalesce([valid_subq], { bboxzxy:null },() => {} );
    }, /bboxzxy must be an array/, 'throws');

    assert.throws(() => {
        coalesce([valid_subq], { bboxzxy:[0,0,0,0] },() => {} );
    }, /bboxzxy must be an array of 5 numbers/, 'throws');

    assert.throws(() => {
        coalesce([valid_subq], { bboxzxy:['',0,0,0,0] },() => {} );
    }, /bboxzxy values must be number/, 'throws');

    assert.throws(() => {
        coalesce([valid_subq], { bboxzxy:[-1,0,0,0,0] },() => {} );
    }, /encountered bboxzxy value too large to fit in uint32_t/, 'throws');

    assert.throws(() => {
        coalesce([valid_subq], { bboxzxy:[4294967296,0,0,0,0] },() => {} );
    }, /encountered bboxzxy value too large to fit in uint32_t/, 'throws');

    assert.throws(() => {
        coalesce([valid_subq], { centerzxy:null },() => {} );
    }, /centerzxy must be an array/, 'throws');

    assert.throws(() => {
        coalesce([valid_subq], { centerzxy:['',0,0] },() => {} );
    }, /centerzxy values must be number/, 'throws');

    assert.throws(() => {
        coalesce([valid_subq], { centerzxy:[0,0] },() => {} );
    }, /centerzxy must be an array of 3 numbers/, 'throws');

    assert.throws(() => {
        coalesce([valid_subq], { centerzxy:[-1,0,0] },() => {} );
    }, /encountered centerzxy value too large to fit in uint32_t/, 'throws');

    assert.throws(() => {
        coalesce([valid_subq], { centerzxy:[4294967296,0,0] },() => {} );
    }, /encountered centerzxy value too large to fit in uint32_t/, 'throws');

    assert.throws(() => {
        coalesce([valid_subq], { centerzxy:[0,0,0] }, 5 );
    }, /Arg 3 must be a callback/, 'throws');

    assert.throws(() => {
        coalesce([{ mask: 1 << 0, idx: 1, zoom: 1, weight: .5, phrase: '1', prefix: false }],{},() => {});
    }, /missing/, 'throws');

    assert.throws(() => {
        coalesce([{ cache: new MemoryCache('b'), idx: 1, zoom: 1, weight: .5, phrase: '1', prefix: false }],{},() => {});
    }, /missing/, 'throws');

    assert.throws(() => {
        coalesce([{ cache: new MemoryCache('b'), mask: 1 << 0, zoom: 1, weight: .5, phrase: '1', prefix: false }],{},() => {});
    }, /missing/, 'throws');

    assert.throws(() => {
        coalesce([{ cache: new MemoryCache('b'), mask: 1 << 0, idx: 1, weight: .5, phrase: '1', prefix: false }],{},() => {});
    }, /missing/, 'throws');

    assert.throws(() => {
        coalesce([{ cache: new MemoryCache('b'), mask: 1 << 0, idx: 1, zoom: 1, phrase: '1', prefix: false }],{},() => {});
    }, /missing/, 'throws');

    assert.throws(() => {
        coalesce([{ cache: new MemoryCache('b'), mask: 1 << 0, idx: 1, zoom: 1, weight: .5, prefix: false }],{},() => {});
    }, /missing/, 'throws');

    assert.throws(() => {
        coalesce([{ cache: '', mask: 1 << 0, idx: 1, weight: .5,  zoom: 1, phrase: '1', prefix: false }],{},() => {});
    }, /cache value must be a Cache object/, 'throws');

    assert.throws(() => {
        coalesce([{ cache: new MemoryCache('b'), mask: '', idx: 1, zoom: 1, weight: .5, phrase: '1', prefix: false }],{},() => {});
    }, /mask value must be a number/, 'throws');

    assert.throws(() => {
        coalesce([{ cache: new MemoryCache('b'), mask: 1 << 0, idx: '', weight: .5, zoom: 1, phrase: '1', prefix: false }],{},() => {});
    }, /idx value must be a number/, 'throws');

    assert.throws(() => {
        coalesce([{ cache: new MemoryCache('b'), mask: 1 << 0, idx: 1, weight: .5, zoom: '', phrase: '1', prefix: false }],{},() => {});
    }, /zoom value must be a number/, 'throws');

    assert.throws(() => {
        coalesce([{ cache: new MemoryCache('b'), mask: 1 << 0, idx: 1, weight: '', zoom: 1, phrase: '1', prefix: false }],{},() => {});
    }, /weight value must be a number/, 'throws');

    assert.throws(() => {
        coalesce([{ cache: new MemoryCache('b'), mask: 1 << 0, idx: 1, weight: .5, zoom: 1, phrase: '' }],{},() => {});
    }, /encountered invalid phrase/, 'throws');

    assert.end();
});

(function() {
    const memcache = new MemoryCache('a', 0);
    memcache._set('1', [
        Grid.encode({
            id: 2,
            x: 2,
            y: 2,
            relev: 0.8,
            score: 3
        }),
        Grid.encode({
            id: 3,
            x: 3,
            y: 3,
            relev: 1,
            score: 1
        }),
        Grid.encode({
            id: 1,
            x: 1,
            y: 1,
            relev: 1,
            score: 7
        })
    ]);
    const rockscache = toRocksCache(memcache);

    [memcache, rockscache].forEach((cache) => {
        test('coalesceSingle: ' + cache.id, (assert) => {
            coalesce([{
                cache: cache,
                mask: 1 << 0,
                idx: 0,
                zoom: 2,
                weight: 1,
                phrase: '1',
                prefix: false
            }], {}, (err, res) => {
                assert.ifError(err);
                assert.deepEqual(res[0].relev, 1, '0.relev');
                assert.deepEqual(res[0][0], { matches_language: true, distance: 0, id: 1, idx: 0, relev: 1.0, score: 7, scoredist: 7, tmpid: 1, x: 1, y: 1 }, '0.0');
                assert.deepEqual(res[1].relev, 1, '1.relev');
                assert.deepEqual(res[1][0], { matches_language: true, distance: 0, id: 3, idx: 0, relev: 1.0, score: 1, scoredist: 1, tmpid: 3, x: 3, y: 3 }, '1.0');
                assert.deepEqual(res[2].relev, 0.8, '2.relev');
                assert.deepEqual(res[2][0], { matches_language: true, distance: 0, id: 2, idx: 0, relev: 0.8, score: 3, scoredist: 3, tmpid: 2, x: 2, y: 2 }, '2.0');
                assert.end();
            });
        });
        test('coalesceSingle proximity: ' + cache.id, (assert) => {
            coalesce([{
                cache: cache,
                mask: 1 << 0,
                idx: 0,
                zoom: 2,
                weight: 1,
                phrase: '1',
                prefix: false
            }], {
                centerzxy: [3,3,3]
            }, (err, res) => {
                assert.ifError(err);
                assert.deepEqual(res[0].relev, 1, '0.relev');
                assert.deepEqual(res[0][0], { matches_language: true, distance: 0, id: 3, idx: 0, relev: 1.0, score: 1, scoredist: 124.85901539399482, tmpid: 3, x: 3, y: 3 }, '0.0');
                assert.deepEqual(res[1].relev, 1, '1.relev');
                assert.deepEqual(res[1][0], { matches_language: true, distance: 2.8284271247461903, id: 1, idx: 0, relev: 1.0, score: 7, scoredist: 7, tmpid: 1, x: 1, y: 1 }, '1.0');
                assert.deepEqual(res[2].relev, 0.8, '2.relev');
                assert.deepEqual(res[2][0], { matches_language: true, distance: 1.4142135623730951, id: 2, idx: 0, relev: 0.8, score: 3, scoredist: 3, tmpid: 2, x: 2, y: 2 }, '2.0');
                assert.end();
            });
        });
        test('coalesceSingle bbox: ' + cache.id, (assert) => {
            coalesce([{
                cache: cache,
                mask: 1 << 0,
                idx: 0,
                zoom: 2,
                weight: 1,
                phrase: '1',
                prefix: false
            }], {
                bboxzxy: [2, 1, 1, 1, 1]
            }, (err, res) => {
                assert.ifError(err);
                assert.deepEqual(res[0].relev, 1, '1.relev');
                assert.deepEqual(res.length, 1);
                assert.deepEqual(res[0][0], { matches_language: true, distance: 0, id: 1, idx: 0, relev: 1.0, score: 7, scoredist: 7, tmpid: 1, x: 1, y: 1 }, '1.0');
                assert.end();
            });
        });
    });
})();

// exercise the sort function
(function() {
    const caches = {
        'x': new MemoryCache('x', 0),
        'y': new MemoryCache('y', 0)
    };
    for (const toIncrement of ['x', 'y']) {
        const entries = [];
        for (const incVal of [0, 1]) {
            for (const id of [1, 2, 3]) {
                const item = {
                    id: id,
                    x: 1,
                    y: 1,
                    relev: 1,
                    score: 1
                };
                item[toIncrement] += incVal;
                entries.push(item);
            }
        }
        caches[toIncrement]._set('1', entries.map(Grid.encode));

        test('coalesceSingle sort tests: ' + toIncrement, (assert) => {
            coalesce([{
                cache: caches[toIncrement],
                mask: 1 << 0,
                idx: 0,
                zoom: 2,
                weight: 1,
                phrase: '1',
                prefix: false
            }], {}, (err, res) => {
                assert.ifError(err);
                assert.ok(res);
                assert.end();
            });
        });
    }
})();

(function() {
    const memcache = new MemoryCache('a', 0);
    const grids = [];
    const encoded1 = Grid.encode({
        id: 1,
        x: 1,
        y: 1,
        relev: 1,
        score: 0
    });
    for (let i = 0; i < 80; i++) grids.push(encoded1);
    grids.push(Grid.encode({
        id: 2,
        x: 1,
        y: 1,
        relev: 1,
        score: 0
    }));

    memcache._set('1', grids);
    const rockscache = toRocksCache(memcache);
    [memcache, rockscache].forEach((cache) => {
        test('coalesceSingle: ' + cache.id, (assert) => {
            coalesce([{
                cache: cache,
                mask: 1 << 0,
                idx: 0,
                zoom: 2,
                weight: 1,
                phrase: '1',
                prefix: false
            }], {}, (err, res) => {
                assert.ifError(err);
                assert.equal(res.length, 2);
                assert.deepEqual(res[0].relev, 1, '0.relev');
                assert.deepEqual(res[0][0], { matches_language: true, distance: 0, id: 1, idx: 0, relev: 1.0, score: 0, scoredist: 0, tmpid: 1, x: 1, y: 1 }, '0.0');
                assert.deepEqual(res[1].relev, 1, '1.relev');
                assert.deepEqual(res[1][0], { matches_language: true, distance: 0, id: 2, idx: 0, relev: 1.0, score: 0, scoredist: 0, tmpid: 2, x: 1, y: 1 }, '0.0');
                assert.end();
            });
        });
    });
})();

(function() {
    const memcache = new MemoryCache('a', 0);

    memcache._set('1', [Grid.encode({
        id: 1,
        x: 1,
        y: 1,
        relev: 1,
        score: 0
    })], [0]);
    memcache._set('1', [Grid.encode({
        id: 2,
        x: 1,
        y: 1,
        relev: 1,
        score: 0
    })], [1]);
    memcache._set('1', [Grid.encode({
        id: 3,
        x: 1,
        y: 1,
        relev: 1,
        score: 0
    })], [0,1]);
    memcache._set('1', [Grid.encode({
        id: 4,
        x: 1,
        y: 1,
        relev: 1,
        score: 0
    })], [2]);
    const rockscache = toRocksCache(memcache);
    [memcache, rockscache].forEach((cache) => {
        test('coalesceSingle, ALL_LANGUAGES: ' + cache.id, (assert) => {
            coalesce([{
                cache: cache,
                mask: 1 << 0,
                idx: 0,
                zoom: 2,
                weight: 1,
                phrase: '1',
                prefix: false
            }], {}, (err, res) => {
                assert.ifError(err);
                assert.equal(res.length, 4);
                assert.deepEqual(res[0].relev, 1, '0.relev');
                assert.deepEqual(res[0][0], { matches_language: true, distance: 0, id: 1, idx: 0, relev: 1.0, score: 0, scoredist: 0, tmpid: 1, x: 1, y: 1 }, '0.0');
                assert.deepEqual(res[1].relev, 1, '1.relev');
                assert.deepEqual(res[1][0], { matches_language: true, distance: 0, id: 2, idx: 0, relev: 1.0, score: 0, scoredist: 0, tmpid: 2, x: 1, y: 1 }, '0.0');
                assert.deepEqual(res[2].relev, 1, '2.relev');
                assert.deepEqual(res[2][0], { matches_language: true, distance: 0, id: 3, idx: 0, relev: 1.0, score: 0, scoredist: 0, tmpid: 3, x: 1, y: 1 }, '0.0');
                assert.deepEqual(res[3].relev, 1, '3.relev');
                assert.deepEqual(res[3][0], { matches_language: true, distance: 0, id: 4, idx: 0, relev: 1.0, score: 0, scoredist: 0, tmpid: 4, x: 1, y: 1 }, '0.0');
                assert.end();
            });
        });
        test('coalesceSingle, [0]: ' + cache.id, (assert) => {
            coalesce([{
                cache: cache,
                mask: 1 << 0,
                idx: 0,
                zoom: 2,
                weight: 1,
                phrase: '1',
                prefix: false,
                languages: [0]
            }], {}, (err, res) => {
                assert.ifError(err);
                assert.equal(res.length, 4);
                assert.deepEqual(res[0].relev, 1, '0.relev');
                assert.deepEqual(res[0][0], { matches_language: true, distance: 0, id: 1, idx: 0, relev: 1.0, score: 0, scoredist: 0, tmpid: 1, x: 1, y: 1 }, '0.0');
                assert.deepEqual(res[1].relev, 1, '1.relev');
                assert.deepEqual(res[1][0], { matches_language: true, distance: 0, id: 3, idx: 0, relev: 1.0, score: 0, scoredist: 0, tmpid: 3, x: 1, y: 1 }, '0.0');
                assert.deepEqual(res[2].relev, 0.9, '2.relev');
                assert.deepEqual(res[2][0], { matches_language: false, distance: 0, id: 2, idx: 0, relev: 0.9, score: 0, scoredist: 0, tmpid: 2, x: 1, y: 1 }, '0.0');
                assert.deepEqual(res[3].relev, 0.9, '3.relev');
                assert.deepEqual(res[3][0], { matches_language: false, distance: 0, id: 4, idx: 0, relev: 0.9, score: 0, scoredist: 0, tmpid: 4, x: 1, y: 1 }, '0.0');
                assert.end();
            });
        });
        test('coalesceSingle, [3]: ' + cache.id, (assert) => {
            coalesce([{
                cache: cache,
                mask: 1 << 0,
                idx: 0,
                zoom: 2,
                weight: 1,
                phrase: '1',
                prefix: false,
                languages: [3]
            }], {}, (err, res) => {
                assert.ifError(err);
                assert.equal(res.length, 4);
                assert.deepEqual(res[0].relev, 0.9, '0.relev');
                assert.deepEqual(res[0][0], { matches_language: false, distance: 0, id: 1, idx: 0, relev: 0.9, score: 0, scoredist: 0, tmpid: 1, x: 1, y: 1 }, '0.0');
                assert.deepEqual(res[1].relev, 0.9, '1.relev');
                assert.deepEqual(res[1][0], { matches_language: false, distance: 0, id: 2, idx: 0, relev: 0.9, score: 0, scoredist: 0, tmpid: 2, x: 1, y: 1 }, '0.0');
                assert.deepEqual(res[2].relev, 0.9, '2.relev');
                assert.deepEqual(res[2][0], { matches_language: false, distance: 0, id: 3, idx: 0, relev: 0.9, score: 0, scoredist: 0, tmpid: 3, x: 1, y: 1 }, '0.0');
                assert.deepEqual(res[3].relev, 0.9, '3.relev');
                assert.deepEqual(res[3][0], { matches_language: false, distance: 0, id: 4, idx: 0, relev: 0.9, score: 0, scoredist: 0, tmpid: 4, x: 1, y: 1 }, '0.0');
                assert.end();
            });
        });
    });
})();

(function() {
    const memA = new MemoryCache('a', 0);
    const memB = new MemoryCache('b', 0);
    memA._set('1', [
        Grid.encode({
            id: 1,
            x: 1,
            y: 1,
            relev: 1,
            score: 1
        }),
        Grid.encode({
            id: 2,
            x: 2,
            y: 2,
            relev: 1,
            score: 1
        })
    ]);
    memB._set('1', [
        Grid.encode({
            id: 2,
            x: 2,
            y: 2,
            relev: 1,
            score: 7
        }),
        Grid.encode({
            id: 3,
            x: 3,
            y: 3,
            relev: 1,
            score: 1
        }),
        Grid.encode({
            id: 1,
            x: 1,
            y: 1,
            relev: 1,
            score: 3
        })
    ]);
    const rocksA = toRocksCache(memA);
    const rocksB = toRocksCache(memB);

    [[memA, memB], [rocksA, rocksB], [memA, rocksB]].forEach((caches) => {
        const a = caches[0],
            b = caches[1];

        test('coalesceUV: ' + a.id + ', ' + b.id, (assert) => {
            coalesce([{
                cache: a,
                mask: 1 << 1,
                idx: 0,
                zoom: 1,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }, {
                cache: b,
                mask: 1 << 0,
                idx: 1,
                zoom: 2,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }], {}, (err, res) => {
                assert.ifError(err);
                // sorts by relev, score
                assert.deepEqual(res[0].relev, 1, '0.relev');
                assert.deepEqual(res[0][0], { matches_language: true, distance: 0, id: 2, idx: 1, relev: 0.5, score: 7, scoredist: 7, tmpid: 33554434, x: 2, y: 2 }, '0.0');
                assert.deepEqual(res[0][1], { matches_language: true, distance: 0, id: 1, idx: 0, relev: 0.5, score: 1, scoredist: 1, tmpid: 1, x: 1, y: 1 }, '0.1');
                assert.deepEqual(res[1].relev, 1, '1.relev');
                assert.deepEqual(res[1][0], { matches_language: true, distance: 0, id: 3, idx: 1, relev: 0.5, score: 1, scoredist: 1, tmpid: 33554435, x: 3, y: 3 }, '1.0');
                assert.deepEqual(res[1][1], { matches_language: true, distance: 0, id: 1, idx: 0, relev: 0.5, score: 1, scoredist: 1, tmpid: 1, x: 1, y: 1 }, '1.1');
                assert.end();
            });
        });
        test('coalesceUV proximity: ' + a.id + ', ' + b.id, (assert) => {
            coalesce([{
                cache: a,
                mask: 1 << 1,
                idx: 0,
                zoom: 1,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }, {
                cache: b,
                mask: 1 << 0,
                idx: 1,
                zoom: 2,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }], {
                centerzxy: [2,3,3]
            }, (err, res) => {
                assert.ifError(err);
                // sorts by relev, score
                assert.deepEqual(res[0].relev, 1, '0.relev');
                assert.deepEqual(res[0][0], { matches_language: true, distance: 0, id: 3, idx: 1, relev: 0.5, score: 1, scoredist: 124.85901539399482, tmpid: 33554435, x: 3, y: 3 }, '0.0');
                assert.deepEqual(res[0][1], { matches_language: true, distance: 2.8284271247461903, id: 1, idx: 0, relev: 0.5, score: 1, scoredist: 1, tmpid: 1, x: 1, y: 1 }, '0.1');
                assert.deepEqual(res[1].relev, 1, '1.relev');
                assert.deepEqual(res[1][0], { matches_language: true, distance: 1.4142135623730951, id: 2, idx: 1, relev: 0.5, score: 7, scoredist: 7, tmpid: 33554434, x: 2, y: 2 }, '1.0');
                assert.deepEqual(res[1][1], { matches_language: true, distance: 2.8284271247461903, id: 1, idx: 0, relev: 0.5, score: 1, scoredist: 1, tmpid: 1, x: 1, y: 1 }, '1.1');
                assert.end();
            });
        });
    });
})();

(function() {
    const memA = new MemoryCache('a', 0);
    const memB = new MemoryCache('b', 0);
    memA._set('1', [
        Grid.encode({
            id: 1,
            x: 1,
            y: 1,
            relev: 1,
            score: 1
        })
    ]);
    memB._set('1', [
        Grid.encode({
            id: 2,
            x: 1,
            y: 1,
            relev: 1,
            score: 1
        })
    ], [0]);
    memB._set('1', [
        Grid.encode({
            id: 3,
            x: 1,
            y: 1,
            relev: 1,
            score: 1
        })
    ], [1]);
    const rocksA = toRocksCache(memA);
    const rocksB = toRocksCache(memB);

    [[memA, memB], [rocksA, rocksB], [memA, rocksB]].forEach((caches) => {
        const a = caches[0],
            b = caches[1];

        test('coalesceMulti, ALL_LANGUAGES: ' + a.id + ', ' + b.id, (assert) => {
            coalesce([{
                cache: a,
                mask: 1 << 1,
                idx: 0,
                zoom: 1,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }, {
                cache: b,
                mask: 1 << 0,
                idx: 1,
                zoom: 1,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }], {}, (err, res) => {
                assert.ifError(err);
                // sorts by relev, score
                assert.deepEqual(res[0].relev, 1, '0.relev');
                assert.deepEqual(res[0][0], { matches_language: true, distance: 0, id: 2, idx: 1, relev: 0.5, score: 1, scoredist: 1, tmpid: 33554434, x: 1, y: 1 }, '0.0');
                assert.deepEqual(res[0][1], { matches_language: true, distance: 0, id: 1, idx: 0, relev: 0.5, score: 1, scoredist: 1, tmpid: 1, x: 1, y: 1 }, '0.1');
                assert.deepEqual(res[1].relev, 1, '1.relev');
                assert.deepEqual(res[1][0], { matches_language: true, distance: 0, id: 3, idx: 1, relev: 0.5, score: 1, scoredist: 1, tmpid: 33554435, x: 1, y: 1 }, '1.0');
                assert.deepEqual(res[1][1], { matches_language: true, distance: 0, id: 1, idx: 0, relev: 0.5, score: 1, scoredist: 1, tmpid: 1, x: 1, y: 1 }, '1.1');
                assert.end();
            });
        });
        test('coalesceMulti, [0]: ' + a.id + ', ' + b.id, (assert) => {
            coalesce([{
                cache: a,
                mask: 1 << 1,
                idx: 0,
                zoom: 1,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }, {
                cache: b,
                mask: 1 << 0,
                idx: 1,
                zoom: 1,
                weight: 0.5,
                phrase: '1',
                prefix: false,
                languages: [0]
            }], {}, (err, res) => {
                assert.ifError(err);
                // sorts by relev, score
                assert.deepEqual(res[0].relev, 1, '0.relev');
                assert.deepEqual(res[0][0], { matches_language: true, distance: 0, id: 2, idx: 1, relev: 0.5, score: 1, scoredist: 1, tmpid: 33554434, x: 1, y: 1 }, '0.0');
                assert.deepEqual(res[0][1], { matches_language: true, distance: 0, id: 1, idx: 0, relev: 0.5, score: 1, scoredist: 1, tmpid: 1, x: 1, y: 1 }, '0.1');
                // one of our indexes has languages and the other does not, so relev will be 0.95 because it's (.5 + .9*.5)
                assert.deepEqual(res[1].relev, 0.95, '1.relev');
                assert.deepEqual(res[1][0], { matches_language: false, distance: 0, id: 3, idx: 1, relev: 0.45, score: 1, scoredist: 1, tmpid: 33554435, x: 1, y: 1 }, '1.0');
                assert.deepEqual(res[1][1], { matches_language: true, distance: 0, id: 1, idx: 0, relev: 0.5, score: 1, scoredist: 1, tmpid: 1, x: 1, y: 1 }, '1.1');
                assert.end();
            });
        });
        test('coalesceMulti, [3]: ' + a.id + ', ' + b.id, (assert) => {
            coalesce([{
                cache: a,
                mask: 1 << 1,
                idx: 0,
                zoom: 1,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }, {
                cache: b,
                mask: 1 << 0,
                idx: 1,
                zoom: 1,
                weight: 0.5,
                phrase: '1',
                prefix: false,
                languages: [3]
            }], {}, (err, res) => {
                assert.ifError(err);
                // sorts by relev, score
                assert.deepEqual(res[0].relev, 0.95, '0.relev');
                assert.deepEqual(res[0][0], { matches_language: false, distance: 0, id: 2, idx: 1, relev: 0.45, score: 1, scoredist: 1, tmpid: 33554434, x: 1, y: 1 }, '0.0');
                assert.deepEqual(res[0][1], { matches_language: true, distance: 0, id: 1, idx: 0, relev: 0.5, score: 1, scoredist: 1, tmpid: 1, x: 1, y: 1 }, '0.1');
                // one of our indexes has languages and the other does not, so relev will be 0.9 because it's (.5 + .8*.5)
                assert.deepEqual(res[1].relev, 0.95, '1.relev');
                assert.deepEqual(res[1][0], { matches_language: false, distance: 0, id: 3, idx: 1, relev: 0.45, score: 1, scoredist: 1, tmpid: 33554435, x: 1, y: 1 }, '1.0');
                assert.deepEqual(res[1][1], { matches_language: true, distance: 0, id: 1, idx: 0, relev: 0.5, score: 1, scoredist: 1, tmpid: 1, x: 1, y: 1 }, '1.1');
                assert.end();
            });
        });
    });
})();

(function() {
    const memA = new MemoryCache('a', 0);
    const memB = new MemoryCache('b', 0);
    memA._set('1', [
        Grid.encode({
            id: 1,
            x: 0,
            y: 0,
            relev: 1,
            score: 1
        })
    ]);
    memB._set('1', [
        Grid.encode({
            id: 2,
            x: 4800,
            y: 6200,
            relev: 1,
            score: 7
        }),
        Grid.encode({
            id: 3,
            x: 4600,
            y: 6200,
            relev: 1,
            score: 1
        })
    ]);
    const rocksA = toRocksCache(memA);
    const rocksB = toRocksCache(memB);

    [[memA, memB], [rocksA, rocksB], [memA, rocksB]].forEach((caches) => {
        const a = caches[0],
            b = caches[1];

        test('coalesce scoredist (close proximity): ' + a.id + ', ' + b.id, (assert) => {
            coalesce([{
                cache: a,
                mask: 1 << 1,
                idx: 0,
                zoom: 0,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }, {
                cache: b,
                mask: 1 << 0,
                idx: 1,
                zoom: 14,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }], {
                centerzxy: [14,4601,6200]
            }, (err, res) => {
                assert.ifError(err);
                assert.deepEqual(res[0][0].id, 3, 'matches feat 3');
                assert.deepEqual(res[1][0].id, 2, 'matches feat 2');
                assert.deepEqual(res[0][0].distance < res[1][0].distance, true, 'feat 3 is closer than feat2');
                assert.end();
            });
        });
        test('coalesce scoredist (far proximity): ' + a.id + ', ' + b.id, (assert) => {
            coalesce([{
                cache: a,
                mask: 1 << 1,
                idx: 0,
                zoom: 0,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }, {
                cache: b,
                mask: 1 << 0,
                idx: 1,
                zoom: 14,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }], {
                centerzxy: [14,4610,6200]
            }, (err, res) => {
                assert.ifError(err);
                assert.deepEqual(res[0][0].id, 2, 'matches feat 2 (higher score)');
                assert.deepEqual(res[1][0].id, 3, 'matches feat 3');
                assert.deepEqual(res[1][0].distance < res[0][0].distance, true, 'feat 3 is closer than feat2');
                assert.end();
            });
        });
    });
})();

(function() {
    const memA = new MemoryCache('a', 0);
    const memB = new MemoryCache('b', 0);
    memA._set('1', [
        Grid.encode({
            id: 1,
            x: 1,
            y: 1,
            relev: 0.8,
            score: 1
        }),
        Grid.encode({
            id: 2,
            x: 1,
            y: 1,
            relev: 1,
            score: 1
        })
    ]);
    memB._set('1', [
        Grid.encode({
            id: 3,
            x: 2,
            y: 2,
            relev: 1,
            score: 1
        })
    ]);

    const rocksA = toRocksCache(memA);
    const rocksB = toRocksCache(memB);

    [[memA, memB], [rocksA, rocksB], [memA, rocksB]].forEach((caches) => {
        const a = caches[0],
            b = caches[1];

        test('coalesceMulti (higher relev wins): ' + a.id + ', ' + b.id, (assert) => {
            coalesce([{
                cache: a,
                mask: 1 << 1,
                idx: 0,
                zoom: 1,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }, {
                cache: b,
                mask: 1 << 0,
                idx: 1,
                zoom: 2,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }], {}, (err, res) => {
                assert.ifError(err);
                // sorts by relev, score
                assert.deepEqual(res.length, 1, '1 result');
                assert.deepEqual(res[0].relev, 1, '0.relev');
                assert.deepEqual(res[0].length, 2, '0.length');
                assert.deepEqual(res[0][0], { matches_language: true, distance: 0, id: 3, idx: 1, relev: 0.5, score: 1, scoredist: 1, tmpid: 33554435, x: 2, y: 2 }, '0.0');
                assert.deepEqual(res[0][1], { matches_language: true, distance: 0, id: 2, idx: 0, relev: 0.5, score: 1, scoredist: 1, tmpid: 2, x: 1, y: 1 }, '0.1');
                assert.end();
            });
        });
    });
})();

// cooalese multi bbox
(function() {
    const memA = new MemoryCache('a', 0);
    const memB = new MemoryCache('b', 0);
    const memC = new MemoryCache('c', 0);
    memA._set('1', [
        Grid.encode({
            id: 1,
            x: 0,
            y: 0,
            relev: 0.8,
            score: 1
        }),
        Grid.encode({
            id: 2,
            x: 1,
            y: 1,
            relev: 1,
            score: 1
        })
    ]);
    memB._set('1', [
        Grid.encode({
            id: 3,
            x: 3,
            y: 0,
            relev: 1,
            score: 1
        }),
        Grid.encode({
            id: 4,
            x: 0,
            y: 3,
            relev: 1,
            score: 1
        })
    ]);
    memC._set('1', [
        Grid.encode({
            id: 5,
            x: 21,
            y: 7,
            relev: 1,
            score: 1
        }),
        Grid.encode({
            id: 6,
            x: 21,
            y: 18,
            relev: 1,
            score: 1
        })
    ]);

    const rocksA = toRocksCache(memA);
    const rocksB = toRocksCache(memB);
    const rocksC = toRocksCache(memC);

    [[memA, memB, memC], [rocksA, rocksB, rocksC], [memA, rocksB, memC]].forEach((caches) => {
        const a = caches[0],
            b = caches[1],
            c = caches[2];

        test('coalesceMulti bbox: ' + a.id + ', ' + b.id + ', ' + c.id, (assert) => {
            coalesce([{
                cache: a,
                mask: 1 << 1,
                idx: 0,
                zoom: 1,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }, {
                cache: b,
                mask: 1 << 0,
                idx: 1,
                zoom: 2,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }], {
                bboxzxy: [1, 0, 0, 1, 0]
            }, (err, res) => {
                assert.ifError(err);
                assert.deepEqual(res.length, 2, '2 results: 1/0/0, 2/3/0');
                assert.end();
            });
        });
        test('coalesceMulti bbox: ' + a.id + ', ' + b.id + ', ' + c.id, (assert) => {
            coalesce([{
                cache: a,
                mask: 1 << 1,
                idx: 0,
                zoom: 1,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }, {
                cache: b,
                mask: 1 << 0,
                idx: 1,
                zoom: 2,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }], {
                bboxzxy: [2, 0, 0, 1, 3]
            }, (err, res) => {
                assert.ifError(err);
                assert.deepEqual(res.length, 2, '2 results: 1/0/0, 2/0/3');
                assert.end();
            });
        });
        test('coalesceMulti bbox: ' + a.id + ', ' + b.id + ', ' + c.id, (assert) => {
            coalesce([{
                cache: a,
                mask: 1 << 1,
                idx: 0,
                zoom: 1,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }, {
                cache: b,
                mask: 1 << 0,
                idx: 1,
                zoom: 2,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }], {
                bboxzxy: [6, 14, 30, 15, 64]
            }, (err, res) => {
                assert.ifError(err);
                assert.deepEqual(res.length, 2, '2 results: 1/0/0, 2/0/3');
                assert.end();
            });
        });
        test('coalesceMulti bbox: ' + a.id + ', ' + b.id + ', ' + c.id, (assert) => {
            coalesce([{
                cache: b,
                mask: 1 << 1,
                idx: 0,
                zoom: 2,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }, {
                cache: c,
                mask: 1 << 0,
                idx: 1,
                zoom: 5,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }], {
                bboxzxy: [1, 0, 0, 1, 0]
            }, (err, res) => {
                assert.ifError(err);
                assert.deepEqual(res.length, 2, '2 results: 5/20/7, 2/3/0');
                assert.end();
            });
        });
    });
})();

// Multi sandwich scenario
(function() {
    const memA = new MemoryCache('a', 0);
    const memB = new MemoryCache('b', 0);
    memA._set('1', [
        Grid.encode({
            id: 3,
            x: 0,
            y: 0,
            relev: 1,
            score: 1
        }),
        Grid.encode({
            id: 4,
            x: 0,
            y: 0,
            relev: 1,
            score: 1
        })
    ]);
    memB._set('1', [
        Grid.encode({
            id: 1,
            x: 1,
            y: 1,
            relev: 1,
            score: 1
        }),
        Grid.encode({
            id: 2,
            x: 1,
            y: 1,
            relev: 1,
            score: 1
        })
    ]);
    const rocksA = toRocksCache(memA);
    const rocksB = toRocksCache(memB);

    [[memA, memB], [rocksA, rocksB], [memA, rocksB]].forEach((caches) => {
        const a = caches[0],
            b = caches[1];

        test('coalesceMulti sandwich: ' + a.id + ', ' + b.id, (assert) => {
            coalesce([{
                cache: a,
                mask: 1 << 1,
                idx: 0,
                zoom: 0,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }, {
                cache: b,
                mask: 1 << 0,
                idx: 1,
                zoom: 1,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }], {}, (err, res) => {
                assert.ifError(err);
                assert.equal(res.length, 2, 'res length = 2');
                // sorts by relev, score
                assert.deepEqual(res[0].map((f) => { return f.id; }), [1,4], '0.relev = 1');
                assert.deepEqual(res[1].map((f) => { return f.id; }), [2,4], '0.relev = 1');
                assert.end();
            });
        });
    });
})();

// Multi sandwich scenario
(function() {
    const memA = new MemoryCache('a', 0);
    const memB = new MemoryCache('b', 0);
    memA._set('1', [
        Grid.encode({
            id: 3,
            x: 0,
            y: 0,
            relev: 1,
            score: 1
        }),
        Grid.encode({
            id: 4,
            x: 0,
            y: 0,
            relev: 1,
            score: 1
        })
    ]);
    memB._set('1', [
        Grid.encode({
            id: 1,
            x: 0,
            y: 0,
            relev: 1,
            score: 1
        })
    ]);

    const rocksA = toRocksCache(memA);
    const rocksB = toRocksCache(memB);

    [[memA, memB], [rocksA, rocksB], [memA, rocksB]].forEach((caches) => {
        const a = caches[0],
            b = caches[1];

        test('coalesceMulti sandwich: ' + a.id + ', ' + b.id, (assert) => {
            coalesce([{
                cache: a,
                mask: 1 << 1,
                idx: 25,
                zoom: 0,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }, {
                cache: b,
                mask: 1 << 0,
                idx: 20,
                zoom: 0,
                weight: 0.5,
                phrase: '1',
                prefix: false
            }], {}, (err, res) => {
                assert.ifError(err);
                assert.equal(res.length, 2, 'res length = 2');
                assert.deepEqual(res[0].map((f) => { return f.id; }), [3,1], '0.relev = 1');
                assert.deepEqual(res[1].map((f) => { return f.id; }), [4,1], '0.relev = 1');
                assert.end();
            });
        });
    });
})();

// Mask overflow
(function() {
    const memA = new MemoryCache('a', 0);
    const memB = new MemoryCache('b', 0);
    const memC = new MemoryCache('c', 0);

    const grids = [];
    for (let i = 1; i < 10e3; i++) grids.push(Grid.encode({
        id: i,
        x: 0,
        y: 0,
        relev: 1,
        score: 1
    }));
    memA._set('1', grids);

    memB._set('1', [
        Grid.encode({
            id: 1,
            x: 0,
            y: 0,
            relev: 1,
            score: 1
        })
    ]);
    memC._set('1', [
        Grid.encode({
            id: 1,
            x: 0,
            y: 0,
            relev: 1,
            score: 1
        })
    ]);
    const rocksA = toRocksCache(memA);
    const rocksB = toRocksCache(memB);
    const rocksC = toRocksCache(memC);

    [[memA, memB, memC], [rocksA, rocksB, rocksC], [memA, rocksB, memC]].forEach((caches) => {
        const a = caches[0],
            b = caches[1],
            c = caches[2];

        test('coalesceMulti mask safe: ' + a.id + ', ' + b.id + ', ' + c.id, (assert) => {
            assert.comment('start coalesce (mask: 2)');
            coalesce([{
                cache: a,
                mask: 1 << 2,
                idx: 0,
                zoom: 0,
                weight: 0.33,
                phrase: '1',
                prefix: false
            }, {
                cache: b,
                mask: 1 << 0,
                idx: 1,
                zoom: 0,
                weight: 0.33,
                phrase: '1',
                prefix: false
            }, {
                cache: c,
                mask: 1 << 1,
                idx: 2,
                zoom: 0,
                weight: 0.33,
                phrase: '1',
                prefix: false
            }], {}, (err, res) => {
                assert.ifError(err);
                assert.equal(res.length, 1, 'res length = 1');
                assert.deepEqual(res[0].map((f) => { return f.id; }), [1, 9999, 1], '0.relev = 0.99');
                assert.end();
            });
        });

        test('coalesceMulti mask overflow: ' + a.id + ', ' + b.id + ', ' + c.id, (assert) => {
            assert.comment('start coalesce (mask: 18)');
            coalesce([{
                cache: a,
                mask: 1 << 18,
                idx: 0,
                zoom: 0,
                weight: 0.33,
                phrase: '1',
                prefix: false
            }, {
                cache: b,
                mask: 1 << 0,
                idx: 1,
                zoom: 0,
                weight: 0.33,
                phrase: '1',
                prefix: false
            }, {
                cache: c,
                mask: 1 << 1,
                idx: 2,
                zoom: 0,
                weight: 0.33,
                phrase: '1',
                prefix: false
            }], {}, (err, res) => {
                assert.ifError(err);
                assert.equal(res.length, 1, 'res length = 1');
                assert.deepEqual(res[0].map((f) => { return f.id; }), [1, 9999, 1], '0.relev = 0.99');
                assert.end();
            });
        });
    });
})();
