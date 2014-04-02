var Cache = require('../index.js').Cache;
var assert = require('assert');
var fs = require('fs');

describe('Cache', function() {
    describe('c++ functions', function() {
        it('#list', function() {
            var cache = new Cache('a', 1);
            cache._set('term', 0, 5, [0,1,2]);
            assert.deepEqual([0], cache.list('term'));
        });

        it('#has', function() {
            var cache = new Cache('a', 1);
            cache._set('term', 0, 5, [0,1,2]);
            assert.deepEqual(true, cache.has('term', 0));
        });

        it('#get', function() {
            var cache = new Cache('a', 1);
            cache._set('term', 0, 5, [0,1,2]);
            assert.deepEqual([0, 1, 2], cache._get('term', 0, 5));
            assert.equal(undefined, cache._get('term', 5, 9));
        });

        it('#pack', function() {
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
        });

        it('#load', function() {
            var cache = new Cache('a', 1);
            assert.equal('a', cache.id);
            assert.equal(1, cache.shardlevel);

            assert.equal(undefined, cache._get('term', 0, 5));
            assert.deepEqual([], cache.list('term'));

            cache._set('term', 0, 5, [0,1,2]);
            assert.deepEqual([0,1,2], cache._get('term', 0, 5));
            assert.deepEqual([0], cache.list('term'));

            cache._set('term', 0, 21, [5,6]);
            assert.deepEqual([5,6], cache._get('term', 0, 21));
            assert.deepEqual([0], cache.list('term'), 'single shard');
            assert.deepEqual([5, 21], cache.list('term', 0), 'keys in shard');

            // cache A serializes data, cache B loads serialized data.
            var pack = cache.pack('term', 0);
            var loader = new Cache('b', 1);
            loader.load(pack, 'term', 0);
            assert.deepEqual([5,6], loader._get('term', 0, 21));
            assert.deepEqual([0], loader.list('term'), 'single shard');
            assert.deepEqual([5, 21], loader.list('term', 0), 'keys in shard');
        });

        it('#load (async)', function(done) {
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
                done();
            });
        });

        it('#unload on empty data', function() {
            var cache = new Cache('a', 1);
            assert.equal(false,cache.unload('term',5));
            assert.deepEqual(false, cache.has('term', 5));
        });

        it('#unload after set', function() {
            var cache = new Cache('a', 1);
            cache._set('term', 0, 0, [0,1,2]);
            assert.deepEqual(true, cache.has('term', 0));
            assert.equal(true,cache.unload('term',0));
            assert.deepEqual(false, cache.has('term', 0));
        });

        it('#unload after load', function() {
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
        });
    });
});
