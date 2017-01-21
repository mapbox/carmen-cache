// sequence is: create a temp cache, load data from JSON into memory cache, dump that
// into perm format, then load it again into another temp cache, then bench that

var readline = require('readline');
var cache = require('../../index.js');
var fs = require('fs');

var c = new cache.Cache('test');

console.log('loading into tmp');
var lineReader = readline.createInterface({
    input: fs.createReadStream(process.argv[2]),
    terminal: false
});

lineReader.on('line', function(line) {
    if (!line.trim().length) return;
    var data = JSON.parse(line);

    c._set('grid', data[0], data[1]);
});

lineReader.on('close', function() {
    console.log('dumping');
    c.pack(process.argv[3], 'grid');

    console.log('done');
});