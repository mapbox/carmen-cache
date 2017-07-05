var carmenCache = require('../index.js');
var grids = new carmenCache.MemoryCache('grids');
var decoder = require('./grid.js');
var tape_test = require('tape');

tape_test('basic API tests', function(assert) {

	//test 1
	grids._set('paris', [1,2,3]);
	assert.deepEqual(grids._get('paris'), [3,2,1], 'values for paris are equal - set to 1,2,3');

	//test 2
	grids._set('paris', [4,5,6]);
	assert.deepEqual(grids._get('paris'), [6,5,4], 'values for paris are equal - set to 4,5,6');

	//test 3: append values
	grids._set('paris', [7,8,9], null, true);
	assert.deepEqual(grids._get('paris'), [9,8,7,6,5,4], 'values for paris are equal - appended 7,8,9');

	//test 4: don't append values (replace)
	grids._set('paris', [7,8,9], null, false);
	assert.deepEqual(grids._get('paris'), [9,8,7], 'values for paris are equal - set to 7,8,9');

	//test 5
	grids._set('paris', [1,2,3]);
	assert.deepEqual(grids._get('paris'), [3,2,1], 'values for paris are equal - set to 1,2,3');

	assert.end();

});

tape_test('new API tests', function(assert) {

	//test 1
	var a = { x: 3, y: 17, relev: 1.0, score: 5, id: 15 };
	var b = { x: 4, y: 16, relev: 0.8, score: 3, id: 16 };
	var c = { x: 3, y: 10, relev: 0.6, score: 2, id: 17 };

	grids._set('paris', [a,b,c]);
	assert.deepEqual([
		decoder.decode(grids._get('paris')[0]),
		decoder.decode(grids._get('paris')[1]),
		decoder.decode(grids._get('paris')[1])], [c,b,a], 'grid object values are equal - set to a,b,c');

	assert.end();
});