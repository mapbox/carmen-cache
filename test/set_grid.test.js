var carmenCache = require('../index.js');
var grids = new carmenCache.MemoryCache('grids');
var encoder_cpp = new carmenCache.MemoryCache('encodeGrid');
var encoder_js = require('./grid.js');
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

tape_test('cpp encoder tests', function(assert) {

	//test 1
	var a = { x: 3, y: 17, relev: 1.0, score: 5, id: 15 };
	var b = { x: 4, y: 16, relev: 0.8, score: 3, id: 16 };
	var c = { x: 3, y: 10, relev: 0.6, score: 2, id: 17 };

	var a_cpp_encoded = encoder_cpp.encodeGrid(a);
	var b_cpp_encoded = encoder_cpp.encodeGrid(b);
	var c_cpp_encoded = encoder_cpp.encodeGrid(c);

	var a_js_encoded = encoder_js.encode(a);
	var b_js_encoded = encoder_js.encode(b);
	var c_js_encoded = encoder_js.encode(c);

	assert.deepEqual([a_cpp_encoded,b_cpp_encoded,c_cpp_encoded], [a_js_encoded,b_js_encoded,c_js_encoded], 'encoding produces same results in cpp and js');

	assert.end();
});

tape_test('new API tests', function(assert) {
 
 	//test 1
 	var a = { x: 3, y: 17, relev: 1.0, score: 5, id: 15 };
 	var b = { x: 4, y: 16, relev: 0.8, score: 3, id: 16 };
 	var c = { x: 3, y: 10, relev: 0.6, score: 2, id: 17 };
 
 	grids._set('paris', [a,b,c]);

 	a_encoded = encoder_cpp.encodeGrid(a);
 	b_encoded = encoder_cpp.encodeGrid(b);
 	c_encoded = encoder_cpp.encodeGrid(c);

 	assert.deepEqual(grids._get('paris'), [a_encoded, b_encoded, c_encoded], 'grid object values are equal - set to a,b,c');
 
  	assert.end();
 }); 
