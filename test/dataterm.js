// from carmen...

var mp4 = Math.pow(2,4);
var mp8 = Math.pow(2,8);
var mp20 = Math.pow(2,20);
var mp28 = Math.pow(2,28);
var mp32 = Math.pow(2,32);
var mp52 = Math.pow(2,52);

module.exports = {};
module.exports.encodeData = encodeData;
module.exports.decodeData = decodeData;

function encodeData(obj) {
    var types = { range: 0 };
    var encoded = mp52;
    var type = types[obj.type];
    if (type === undefined) throw new Error('Unknown type ' + obj.type);
    if (obj.max < 0 || obj.max >= mp20) throw new Error('Range max must be between 0-' + (mp20 - 1));
    if (obj.min < 0 || obj.min >= mp20) throw new Error('Range min must be between 0-' + (mp20 - 1));
    encoded += type * mp4;
    encoded += obj.min * mp8;
    encoded += obj.max * mp28;
    return encoded;
}

function decodeData(num) {
    var types = { 0: 'range' };
    var type = Math.floor((num % mp8)/mp4);
    var min = Math.floor((num % mp28)/mp8);
    var max = Math.floor((num % mp52)/mp28);
    return {
        type: types[type],
        min: min,
        max: max
    };
}

