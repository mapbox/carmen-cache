var Cache = require('../index.js').Cache;
var tape = require('tape');
var fs = require('fs');

        tape('#list', function(assert) {
            var cache = new Cache('a', 1);
            cache._set('term', 0, 5, [0,1,2]);
            assert.deepEqual([0], cache.list('term'));
            assert.end();
        });

        tape('#has', function(assert) {
            var cache = new Cache('a', 1);
            cache._set('term', 0, 5, [0,1,2]);
            assert.deepEqual(true, cache.has('term', 0));
            assert.end();
        });

        tape('#get', function(assert) {
            var cache = new Cache('a', 1);
            cache._set('term', 0, 5, [0,1,2]);
            assert.deepEqual([0, 1, 2], cache._get('term', 0, 5));
            assert.equal(undefined, cache._get('term', 5, 9));
            assert.end();
        });

        tape('#set', function(assert) {
            var cache = new Cache('a', 1);
            cache._set('term', 0, 5, [0,1,2]);
            assert.deepEqual(cache._get('term', 0, 5), [0, 1, 2]);
            cache._set('term', 0, 5, [3,4,5]);
            assert.deepEqual(cache._get('term', 0, 5), [3, 4, 5]);
            cache._set('term', 0, 5, [6,7,8], true);
            assert.deepEqual(cache._get('term', 0, 5), [3, 4, 5, 6, 7, 8]);
            assert.end();
        });

        tape('#pack', function(assert) {
            var cache = new Cache('a', 1);
            cache._set('term', 0, 5, [0,1,2]);
            assert.deepEqual(9, cache.pack('term', 0).length);
            // set should replace data
            cache._set('term', 0, 5, [0,1,2,4]);
            assert.deepEqual(10, cache.pack('term', 0).length);
            cache._set('term', 0, 5, []);
            assert.deepEqual(4, cache.pack('term', 0).length);
            // now test packing data created via load
            var packer = new Cache('a', 1);
            var array = [];
            for (var i=0;i<10000;++i) {
                array.push(0);
            }
            packer._set('term', 0, 5, array);
            var loader = new Cache('a', 1);
            loader.load(packer.pack('term',0), 'term', 0);
            // grab data right back out
            assert.deepEqual(10008, loader.pack('term', 0).length);
            // try to grab data that does not exist
            assert.throws(function() { loader.pack('term', 99999999999999) });
            assert.end();
        });

        tape('#load', function(assert) {
            var cache = new Cache('a', 1);
            assert.equal('a', cache.id);

            assert.equal(undefined, cache._get('term', 0, 5));
            assert.deepEqual([], cache.list('term'));

            cache._set('term', 0, 5, [0,1,2]);
            assert.deepEqual([0,1,2], cache._get('term', 0, 5));
            assert.deepEqual([0], cache.list('term'));

            cache._set('term', 0, 21, [5,6]);
            assert.deepEqual([5,6], cache._get('term', 0, 21));
            assert.deepEqual([0], cache.list('term'), 'single shard');
            assert.deepEqual(['5', '21'], cache.list('term', 0), 'keys in shard');

            // cache A serializes data, cache B loads serialized data.
            var pack = cache.pack('term', 0);
            var loader = new Cache('b', 1);
            loader.load(pack, 'term', 0);
            assert.deepEqual([5,6], loader._get('term', 0, 21));
            assert.deepEqual([0], loader.list('term'), 'single shard');
            assert.deepEqual(['5', '21'], loader.list('term', 0), 'keys in shard');
            assert.end();
        });

        tape('#dict', function(assert) {
            var cache = new Cache('a');
            cache._set('term', 0, 5, []);
            cache._set('term', 0, 21, []);
            cache._set('term', 0, 899688358, []);
            cache._set('term', 1, 4, []);
            cache._set('term', 1, 91231, []);
            cache._set('term', 1, 8, []);

            var loader = new Cache('b');

            assert.deepEqual(loader.hasDict('term', 0), false);
            loader.loadAsDict(cache.pack('term', 0), 'term', 0);
            assert.deepEqual(loader.hasDict('term', 0), true);
            assert.deepEqual(loader._dict('term', 0, 5), true);
            assert.deepEqual(loader._dict('term', 0, 21), true);
            assert.deepEqual(loader._dict('term', 0, 22), false);
            assert.deepEqual(loader._dict('term', 0, 899688358), true);

            assert.deepEqual(loader.hasDict('term', 1), false);
            assert.deepEqual(loader._dict('term', 1, 4), false);
            loader.loadAsDict(cache.pack('term', 1), 'term', 1);
            assert.deepEqual(loader.hasDict('term', 1), true);
            assert.deepEqual(loader._dict('term', 1, 4), true);

            assert.end();
        });

        tape('#load (async)', function(assert) {
            var cache = new Cache('a', 1);
            var array = [];
            for (var i=0;i<10000;++i) {
                array.push(0);
            }
            cache._set('term', 0, 0, array);
            var pack = cache.pack('term', 0);
            var loader = new Cache('b', 1);
            // multiple inserts to ensure we are thread safe
            loader.load(pack, 'term', 0,function(err) {
                assert.deepEqual(array, loader._get('term', 0, 0));
                assert.deepEqual([0], loader.list('term'), 'single shard');
            });
            loader.load(pack, 'term', 0,function(err) {
                assert.deepEqual(array, loader._get('term', 0, 0));
                assert.deepEqual([0], loader.list('term'), 'single shard');
            });
            loader.load(pack, 'term', 0,function(err) {
                assert.deepEqual(array, loader._get('term', 0, 0));
                assert.deepEqual([0], loader.list('term'), 'single shard');
            });
            loader.load(pack, 'term', 0,function(err) {
                assert.deepEqual(array, loader._get('term', 0, 0));
                assert.deepEqual([0], loader.list('term'), 'single shard');
            });
            loader.load(pack, 'term', 0,function(err) {
                assert.deepEqual(array, loader._get('term', 0, 0));
                assert.deepEqual([0], loader.list('term'), 'single shard');
            });
            loader.load(pack, 'term', 0,function(err) {
                assert.deepEqual(array, loader._get('term', 0, 0));
                assert.deepEqual([0], loader.list('term'), 'single shard');
            });
            loader.load(pack, 'term', 0,function(err) {
                assert.deepEqual(array, loader._get('term', 0, 0));
                assert.deepEqual([0], loader.list('term'), 'single shard');
                assert.end();
            });
        });

        tape('#unload on empty data', function(assert) {
            var cache = new Cache('a', 1);
            assert.equal(false,cache.unload('term',5));
            assert.deepEqual(false, cache.has('term', 5));
            assert.end();
        });

        tape('#unload after set', function(assert) {
            var cache = new Cache('a', 1);
            cache._set('term', 0, 0, [0,1,2]);
            assert.deepEqual(true, cache.has('term', 0));
            assert.equal(true,cache.unload('term',0));
            assert.deepEqual(false, cache.has('term', 0));
            assert.end();
        });

        tape('#unload after load', function(assert) {
            var cache = new Cache('a', 1);
            var array = [];
            for (var i=0;i<10000;++i) {
                array.push(0);
            }
            cache._set('term', 0, 5, array);
            var pack = cache.pack('term', 0);
            var loader = new Cache('b', 1);
            loader.load(pack, 'term', 0);
            assert.deepEqual(array, loader._get('term', 0, 5));
            assert.deepEqual([0], loader.list('term'), 'single shard');
            assert.deepEqual(true, loader.has('term', 0));
            assert.equal(true,loader.unload('term',0));
            assert.deepEqual(false, loader.has('term', 0));
            assert.end();
        });
