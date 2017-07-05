var carmenCache = require('../index.js');
var grids = new carmenCache.MemoryCache('grids');
grids._set('paris', [1,2,3]);
console.log(grids._get('paris'));
var assert = require('assert');
console.log(assert);
assert.deepEqual([1,2,3], [1,2,3]);
var test = require('tape');
test('array test', function(assert) {
	assert.deepEqual([1,2,3],[1,2,3], 'arrays are equal');
	assert.end();
});