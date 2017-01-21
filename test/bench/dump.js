var cache = require('../../index.js'),
    readline = require('readline'),
    fs = require('fs');

var c = new cache.Cache('test');
c.loadSync(process.argv[2], 'grid');

var lineReader = readline.createInterface({
    input: fs.createReadStream(process.argv[3]),
    terminal: false
});

lineReader.on('line', function(line) {
    line = line.trim();
    if (!line.length) return;

    console.log(JSON.stringify([line, c._get('grid', line)]));
});

lineReader.on('close', function() {});