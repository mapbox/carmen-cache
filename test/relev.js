var POW2_48 = Math.pow(2,48);
var POW2_45 = Math.pow(2,45);
var POW2_33 = Math.pow(2,33);
var POW2_32 = Math.pow(2,32);
var POW2_25 = Math.pow(2,25);
var POW2_12 = Math.pow(2,12);
var POW2_8 = Math.pow(2,8);
var POW2_5 = Math.pow(2,5);
var POW2_3 = Math.pow(2,3);

// Prototype for relevance relevd rows of Carmen.search.
// Defined to take advantage of V8 class performance.
module.exports = function Relev(num) {
    this.id = num % POW2_25;
    this.idx = (num / POW2_25 | 0) % POW2_8;
    this.tmpid = (this.idx * 1e8) + this.id;
    this.reason = (num / POW2_33 | 0) % POW2_12;
    this.count = (num / POW2_45 | 0) % POW2_3;
    this.relev = ((num / POW2_48 | 0) % POW2_5) / (POW2_5-1);
    this.check = true;
};

module.exports.encode = function(r) {
    var relev = Math.floor(r.relev * (POW2_5-1));
    return (relev * POW2_48) +
        (r.count * POW2_45) +
        (r.reason * POW2_33) +
        (r.idx * POW2_25) +
        (r.id);
};

