var setRelevance = require('../index.js').Cache.setRelevance;
var test = require('tape');
var Relev = require('./relev.js');

test('setRelevance', function(t) {
    // No matches.
    t.deepEqual({ relevance:0, sets:[] }, setRelevance(2, []));
    // Relev 1 match for 1 of 2 terms.
    t.deepEqual({ relevance: 0.5, sets:[
        Relev.encode({ id: 153, relev: 1, reason: 1, count: 1, idx: 0 })
    ]}, setRelevance(2, [
        Relev.encode({ id: 153, relev: 1, reason: 1, count: 1, idx: 0 })
    ]));
    // Relev 1 match for 2 of 2 terms.
    t.deepEqual({ relevance: 1, sets:[
        Relev.encode({ id: 3553, relev: 1, reason: 2, count: 1, idx: 1 }),
        Relev.encode({ id: 130305, relev: 1, reason: 1, count: 1, idx: 2 })
    ]}, setRelevance(2, [
        Relev.encode({ id: 3553, relev: 1, reason: 2, count: 1, idx: 1 }),
        Relev.encode({ id: 130305, relev: 1, reason: 1, count: 1, idx: 2 })
    ]));
    // Relev penalized for 2 of 2 terms, but with a gap in db index.
    t.deepEqual({ relevance: 0.99, sets:[
        Relev.encode({ id: 3553, relev: 1, reason: 2, count: 1, idx: 1 }),
        Relev.encode({ id: 130305, relev: 1, reason: 1, count: 1, idx: 3 })
    ]}, setRelevance(2, [
        Relev.encode({ id: 3553, relev: 1, reason: 2, count: 1, idx: 1 }),
        Relev.encode({ id: 130305, relev: 1, reason: 1, count: 1, idx: 3 })
    ]));
    // Second match for the same reason does not contribute to final relevance.
    t.deepEqual({ relevance: 0.5, sets:[
        Relev.encode({ id: 153, relev: 1, reason: 1, count: 1, idx: 0 })
    ]}, setRelevance(2, [
        Relev.encode({ id: 153, relev: 1, reason: 1, count: 1, idx: 0 }),
        Relev.encode({ id: 130305, relev: 1, reason: 1, count: 1, idx: 3 })
    ]));
    // Second match with the same DB does not contribute to final relevance.
    t.deepEqual({ relevance: 0.5, sets:[
        Relev.encode({ id: 130305, relev: 1, reason: 1, count: 1, idx: 3 }),
    ]}, setRelevance(2, [
        Relev.encode({ id: 130305, relev: 1, reason: 1, count: 1, idx: 3 }),
        Relev.encode({ id: 8062, relev: 1, reason: 2, count: 1, idx: 3 })
    ]));
    // Repeated terms with fittable counts/db indexes.
    t.deepEqual({ relevance: 1, sets:[
        Relev.encode({ id: 1, relev: 1, reason: 15, count: 2, idx: 2 }),
        Relev.encode({ id: 2, relev: 1, reason: 15, count: 2, idx: 3 })
    ]}, setRelevance(4, [
        Relev.encode({ id: 1, relev: 1, reason: 15, count: 2, idx: 2 }),
        Relev.encode({ id: 2, relev: 1, reason: 15, count: 2, idx: 3 })
    ]));
    // Repeated terms but match counts are exhausted.
    t.deepEqual({ relevance: 0.5, sets:[
        Relev.encode({ id: 1, relev: 1, reason: 255, count: 2, idx: 2 }),
        Relev.encode({ id: 2, relev: 1, reason: 255, count: 2, idx: 3 })
    ]}, setRelevance(8, [
        Relev.encode({ id: 1, relev: 1, reason: 255, count: 2, idx: 2 }),
        Relev.encode({ id: 2, relev: 1, reason: 255, count: 2, idx: 3 })
    ]));
    // Test that elements of the stack without contribution are set to false.
    var stack = [
        Relev.encode({ id: 3553, relev: 1, reason: 2, count: 1, idx: 1 }),
        Relev.encode({ id: 1, relev: 1, reason: 255, count: 2, idx: 1}),
        Relev.encode({ id: 130305, relev: 1, reason: 1, count: 1, idx: 2 }),
        Relev.encode({ id: 2, relev: 1, reason: 255, count: 2, idx: 3 })
    ];
    var res = setRelevance(2, stack);
    t.equal(res.relevance, 1);
    t.equal(res.sets.length, 3);
    t.deepEqual(res.sets, [
        Relev.encode({ id: 3553, relev: 1, reason: 2, count: 1, idx: 1 }),
        Relev.encode({ id: 130305, relev: 1, reason: 1, count: 1, idx: 2 }),
        Relev.encode({ id: 2, relev: 1, reason: 255, count: 2, idx: 3 })
    ]);
    t.end();
});
