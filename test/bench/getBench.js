// sequence is: create a temp cache, load data from JSON into memory cache, dump that
// into perm format, then load it again into another temp cache, then bench that

var readline = require('readline');
var cache = require('../../index.js');
var fs = require('fs');

var c = new cache.Cache('test');

console.log('loading real');
c.loadSync(process.argv[2], 'grid');

// bench
for (var i = 0; i < 10; i++) console.log('xkfc', c._benchGet('grid', 'xkfc'));
for (var i = 0; i < 10; i++) console.log('xstarbucks (t)', c._benchGet('grid', 'xstarbucks', true));
for (var i = 0; i < 10; i++) console.log('xl...', c._benchGetByPrefix('grid', 'xl'));

// verify that we're getting something useful (do after to make sure caches aren't warm before we start)
console.log('xkfc length', c._get('grid', 'xkfc').length);
console.log('xstarbucks length (t)', c._get('grid', 'xstarbucks', true).length);
console.log('xl... length', c._getByPrefix('grid', 'xl').length);

console.log('done');