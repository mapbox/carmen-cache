var setRelevance = require('../index.js').Cache.setRelevance;
var test = require('tape');

test('setRelevance', function(t) {
    // No matches.
    t.deepEqual({ relevance:0, sets:[] }, setRelevance(2, []));
    // Relev 1 match for 1 of 2 terms.
    t.deepEqual({ relevance: 0.5, sets:[
        { id: 153, relev: 1, reason: 1, count: 1, idx: 0, tmpid: 153 }
    ]}, setRelevance(2, [
        { id: 153, relev: 1, reason: 1, count: 1, idx: 0, tmpid: 153 }
    ]));
    // Relev 1 match for 2 of 2 terms.
    t.deepEqual({ relevance: 1, sets:[
        { id: 3553, relev: 1, reason: 2, count: 1, idx: 1, tmpid: 100000000003553 },
        { id: 130305, relev: 1, reason: 1, count: 1, idx: 2, tmpid: 200000000130305 }
    ]}, setRelevance(2, [
        { id: 3553, relev: 1, reason: 2, count: 1, idx: 1, tmpid: 100000000003553 },
        { id: 130305, relev: 1, reason: 1, count: 1, idx: 2, tmpid: 200000000130305 }
    ]));
    // Relev penalized for 2 of 2 terms, but with a gap in db index.
    t.deepEqual({ relevance: 0.99, sets:[
        { id: 3553, relev: 1, reason: 2, count: 1, idx: 1, tmpid: 100000000003553 },
        { id: 130305, relev: 1, reason: 1, count: 1, idx: 3, tmpid: 300000000130305 }
    ]}, setRelevance(2, [
        { id: 3553, relev: 1, reason: 2, count: 1, idx: 1, tmpid: 100000000003553 },
        { id: 130305, relev: 1, reason: 1, count: 1, idx: 3, tmpid: 300000000130305 }
    ]));
    // Second match for the same reason does not contribute to final relevance.
    t.deepEqual({ relevance: 0.5, sets:[
        { id: 153, relev: 1, reason: 1, count: 1, idx: 0, tmpid: 153 }
    ]}, setRelevance(2, [
        { id: 153, relev: 1, reason: 1, count: 1, idx: 0, tmpid: 153 },
        { id: 130305, relev: 1, reason: 1, count: 1, idx: 3, tmpid: 300000000130305 }
    ]));
    // Second match with the same DB does not contribute to final relevance.
    t.deepEqual({ relevance: 0.5, sets:[
        { id: 130305, relev: 1, reason: 1, count: 1, idx: 3, tmpid: 300000000130305 },
    ]}, setRelevance(2, [
        { id: 130305, relev: 1, reason: 1, count: 1, idx: 3, tmpid: 300000000130305 },
        { id: 8062, relev: 1, reason: 2, count: 1, idx: 3, tmpid: 300000000008062 }
    ]));
    // Repeated terms with fittable counts/db indexes.
    t.deepEqual({ relevance: 1, sets:[
        { id: 1, relev: 1, reason: 15, count: 2, idx: 2, tmpid: 2e8 + 1 },
        { id: 2, relev: 1, reason: 15, count: 2, idx: 3, tmpid: 3e8 + 2 }
    ]}, setRelevance(4, [
        { id: 1, relev: 1, reason: 15, count: 2, idx: 2, tmpid: 2e8 + 1 },
        { id: 2, relev: 1, reason: 15, count: 2, idx: 3, tmpid: 3e8 + 2 }
    ]));
    // Repeated terms but match counts are exhausted.
    t.deepEqual({ relevance: 0.5, sets:[
        { id: 1, relev: 1, reason: 255, count: 2, idx: 2, tmpid: 2e8+1 },
        { id: 2, relev: 1, reason: 255, count: 2, idx: 3, tmpid: 3e8+2 }
    ]}, setRelevance(8, [
        { id: 1, relev: 1, reason: 255, count: 2, idx: 2, tmpid: 2e8+1 },
        { id: 2, relev: 1, reason: 255, count: 2, idx: 3, tmpid: 3e8+2 }
    ]));
    // Test that elements of the stack without contribution are set to false.
    var stack = [
        { id: 3553, relev: 1, reason: 2, count: 1, idx: 1, tmpid: 100003553 },
        { id: 1, relev: 1, reason: 255, count: 2, idx: 1, tmpid: 100000001 },
        { id: 130305, relev: 1, reason: 1, count: 1, idx: 2, tmpid: 200130305 },
        { id: 2, relev: 1, reason: 255, count: 2, idx: 3, tmpid: 300000002 }
    ];
    var res = setRelevance(2, stack);
    t.equal(res.relevance, 1);
    t.equal(res.sets.length, 3);
    t.deepEqual(res.sets, [
        { id: 3553, relev: 1, reason: 2, count: 1, idx: 1, tmpid: 100003553 },
        { id: 130305, relev: 1, reason: 1, count: 1, idx: 2, tmpid: 200130305 },
        { id: 2, relev: 1, reason: 255, count: 2, idx: 3, tmpid: 300000002 }
    ]);
    t.end();
});
