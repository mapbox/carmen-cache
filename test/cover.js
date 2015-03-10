module.exports = Cover;

var mp39 = Math.pow(2,39);
var mp25 = Math.pow(2,25);

function Cover(num) {
    this.x = Math.floor(num / mp39);
    this.y = Math.floor(num % mp39 / mp25);
    this.id = Math.floor(num % mp25);
    return this;
}

Cover.encode = function(obj) {
    return ((obj.x|0) * mp39) + ((obj.y|0) * mp25) + obj.id;
};

