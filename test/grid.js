// Caching shows a 6% perf bump
var mp39 = Math.pow(2,39);
var mp25 = Math.pow(2,25);
var mp23 = Math.pow(2,23);
var mp20 = Math.pow(2,20);
var mp14 = Math.pow(2,14);
var mp3 = Math.pow(2,3);
var mp2 = Math.pow(2,2);

module.exports.encode = encode;
module.exports.decode = decode;

function encode(grid) {
    if (grid.id >= mp20) throw new Error('id must be < 2^20');
    if (grid.x >= mp14) throw new Error('x must be < 2^14');
    if (grid.y >= mp14) throw new Error('x must be < 2^14');
    if (grid.x < 0) throw new Error('x must be > 0');
    if (grid.y < 0) throw new Error('y must be > 0');
    if (grid.relev > 1 || grid.relev < 0.4) throw new Error('relev must be between 0.4 and 1');

    var relev = Math.max(0, Math.min(3, Math.round((grid.relev - 0.4) / 0.2)));
    var score = Math.max(0, Math.min(7, grid.score));
    return (grid.x * mp39) + (grid.y * mp25) + (relev * mp23) + (score * mp20) + grid.id;
}

function decode(num) {
    return {
        x: Math.floor(num / mp39),
        y: Math.floor(num % mp39 / mp25),
        relev: parseFloat((0.4 + (Math.floor(num % mp25 / mp23) * 0.2)).toFixed(1)),
        score: Math.floor(num % mp23 / mp20),
        id: Math.floor(num % mp20)
    };
}

