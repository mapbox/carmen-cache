var setRelevance = require('../index.js').Cache.setRelevance;
var test = require('tape');
var Relev = require('./relev.js');

test('setRelevance', function(t) {
    // No matches.
    t.deepEqual({ relevance:0, sets:[] }, setRelevance(2, [], []));
    // Relev 1 match for 1 of 2 terms.
    t.deepEqual({ relevance: 0.49, sets:[
        Relev.encode({ id: 153, relev: 1, reason: 1, count: 1, idx: 0 })
    ]}, setRelevance(2, [
        Relev.encode({ id: 153, relev: 1, reason: 1, count: 1, idx: 0 })
    ], [0]));
    // Relev 1 match for 2 of 2 terms.
    t.deepEqual({ relevance: 1, sets:[
        Relev.encode({ id: 130305, relev: 1, reason: 1, count: 1, idx: 2 }),
        Relev.encode({ id: 3553, relev: 1, reason: 2, count: 1, idx: 1 })
    ]}, setRelevance(2, [
        Relev.encode({ id: 130305, relev: 1, reason: 1, count: 1, idx: 2 }),
        Relev.encode({ id: 3553, relev: 1, reason: 2, count: 1, idx: 1 })
    ], [0,1,2]));
    // Relev penalized for 2 of 2 terms, but with a gap in db index.
    t.deepEqual({ relevance: 0.999, sets:[
        Relev.encode({ id: 130305, relev: 1, reason: 1, count: 1, idx: 3 }),
        Relev.encode({ id: 3553, relev: 1, reason: 2, count: 1, idx: 1 })
    ]}, setRelevance(2, [
        Relev.encode({ id: 130305, relev: 1, reason: 1, count: 1, idx: 3 }),
        Relev.encode({ id: 3553, relev: 1, reason: 2, count: 1, idx: 1 })
    ], [0,1,2,3]));
    // Second match for the same reason does not contribute to final relevance
    // but feature is retained.
    t.deepEqual({ relevance: 0.49, sets:[
        Relev.encode({ id: 153, relev: 1, reason: 1, count: 1, idx: 0 }),
        Relev.encode({ id: 130305, relev: 1, reason: 1, count: 1, idx: 3 })
    ]}, setRelevance(2, [
        Relev.encode({ id: 153, relev: 1, reason: 1, count: 1, idx: 0 }),
        Relev.encode({ id: 130305, relev: 1, reason: 1, count: 1, idx: 3 })
    ], [0,1,2,3]));
    // Second match with the same DB does not contribute to final relevance.
    t.deepEqual({ relevance: 0.49, sets:[
        Relev.encode({ id: 130305, relev: 1, reason: 1, count: 1, idx: 3 }),
    ]}, setRelevance(2, [
        Relev.encode({ id: 130305, relev: 1, reason: 1, count: 1, idx: 3 }),
        Relev.encode({ id: 8062, relev: 1, reason: 2, count: 1, idx: 3 })
    ], [0,1,2,3]));
    // Finds maximal score.
    // In this case the best combination is 2 3
    t.deepEqual({ relevance: 1, sets:[
        Relev.encode({ id: 2, relev: 1.0, reason: 1, count: 1, idx: 2 }),
        Relev.encode({ id: 3, relev: 1.0, reason: 2, count: 1, idx: 1 }),
    ]}, setRelevance(2, [
        Relev.encode({ id: 1, relev: 0.5, reason: 2, count: 1, idx: 2 }),
        Relev.encode({ id: 2, relev: 1.0, reason: 1, count: 1, idx: 2 }),
        Relev.encode({ id: 3, relev: 1.0, reason: 2, count: 1, idx: 1 }),
        Relev.encode({ id: 4, relev: 0.5, reason: 1, count: 1, idx: 1 }),
    ], [0,1,2]));
    // Repeated terms with fittable counts/db indexes.
    t.deepEqual({ relevance: 1, sets:[
        Relev.encode({ id: 1, relev: 1, reason: 15, count: 2, idx: 2 }),
        Relev.encode({ id: 2, relev: 1, reason: 15, count: 2, idx: 3 })
    ]}, setRelevance(4, [
        Relev.encode({ id: 1, relev: 1, reason: 15, count: 2, idx: 2 }),
        Relev.encode({ id: 2, relev: 1, reason: 15, count: 2, idx: 3 })
    ], [0,1,2,3]));
    // Repeated terms but match counts are exhausted.
    t.deepEqual({ relevance: 0.5, sets:[
        Relev.encode({ id: 1, relev: 1, reason: 255, count: 2, idx: 2 }),
        Relev.encode({ id: 2, relev: 1, reason: 255, count: 2, idx: 3 })
    ]}, setRelevance(8, [
        Relev.encode({ id: 1, relev: 1, reason: 255, count: 2, idx: 2 }),
        Relev.encode({ id: 2, relev: 1, reason: 255, count: 2, idx: 3 })
    ], [0,1,2,3]));
    // Terms with low enough relev and high enough gap can result in a negative
    // relevance score. Ensure this is clipped to 0.
    t.deepEqual({ relevance: 0, sets:[
        Relev.encode({ id: 3553, relev: 0.01, reason: 2, count: 1, idx: 1 }),
        Relev.encode({ id: 130305, relev: 0.01, reason: 1, count: 1, idx: 4 })
    ]}, setRelevance(2, [
        Relev.encode({ id: 3553, relev: 0.01, reason: 2, count: 1, idx: 1 }),
        Relev.encode({ id: 130305, relev: 0.01, reason: 1, count: 1, idx: 4 })
    ], [0,1,2,3,4]));
    // Test that elements of the stack without contribution are set to false.
    var stack = [
        Relev.encode({ id: 3553, relev: 1, reason: 1, count: 1, idx: 1 }),
        Relev.encode({ id: 1, relev: 1, reason: 3, count: 2, idx: 1}), // collides with reason 1
        Relev.encode({ id: 130305, relev: 1, reason: 2, count: 1, idx: 2 }),
        Relev.encode({ id: 2, relev: 1, reason: 3, count: 2, idx: 3 }) // collides with reason 1
    ];
    t.deepEqual(setRelevance(2, stack, [0,1,2,3]), {
        relevance: 1,
        sets: [ stack[0], stack[2] ]
    });

    stack = [
        Relev.encode({ id: 1, relev: 1, reason: 3, count: 2, idx: 2 }),
        Relev.encode({ id: 2, relev: 0.9, reason: 3, count: 2, idx: 3 }),
        Relev.encode({ id: 3, relev: 1, reason: 3, count: 2, idx: 3 })
    ];
    t.deepEqual(setRelevance(2, stack, [0,1,2,3]), {
        relevance: 0.99,
        sets: [ stack[0], stack[1], stack[2] ]
    }, 'matching elements with same reason/idx are retained');

    stack = [
        Relev.encode({ id: 1, relev: 1, reason: 3, count: 2, idx: 3 }),
        Relev.encode({ id: 2, relev: 1, reason: 3, count: 2, idx: 3 }),
        Relev.encode({ id: 3, relev: 1, reason: 3, count: 2, idx: 3 })
    ];
    t.deepEqual(setRelevance(2, stack, [0,1,2,3]), {
        relevance: 0.99,
        sets: [ stack[0], stack[1], stack[2] ]
    }, 'matching elements with contiguous reason are retained');

    stack = [
        Relev.encode({ id: 3, relev: 1, reason: 3, count: 2, idx: 3 }),
        Relev.encode({ id: 1, relev: 1, reason: 1, count: 1, idx: 3 }),
        Relev.encode({ id: 2, relev: 1, reason: 1, count: 1, idx: 3 })
    ];
    t.deepEqual(setRelevance(2, stack, [0,1,2,3]), {
        relevance: 0.99,
        sets: [ stack[0] ]
    }, 'elements from the same idx with different reasons are removed');

    // a b a pattern
    // merrick rd merrick
    stack = [
        Relev.encode({ id: 1, relev: 1, reason: 5, count: 1, idx: 1 }),
        Relev.encode({ id: 2, relev: 1, reason: 7, count: 2, idx: 2 }), // merrick rd
    ];
    t.deepEqual(setRelevance(2, stack, [0,1,2]), {
        relevance: 1,
        sets: [ stack[0], stack[1] ]
    }, 'elements from the same idx with different reasons are removed');

    stack = [
        Relev.encode({ id: 31316076, idx: 17, tmpid: 1731316076, reason: 14, count: 2, relev: 1, check: true }),
        Relev.encode({ id: 14563122, idx: 16, tmpid: 1614563122, reason: 14, count: 2, relev: 1, check: true }),
        Relev.encode({ id: 20696639, idx: 4, tmpid: 420696639, reason: 1, count: 1, relev: 1, check: true })
    ];
    t.deepEqual(setRelevance(4, stack, [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]), {
        relevance: 0.75,
        sets: [ stack[0], stack[1] ]
    }, 'elements with an early reason cannot seek backwards to match an earlier term');

    stack = [
        Relev.encode({ id: 2064953, idx: 2, tmpid: 2202064953, reason: 14, count: 2, relev: 1, check: true }),
        Relev.encode({ id: 18537894, idx: 1, tmpid: 2118537894, reason: 14, count: 2, relev: 1, check: true }),
        Relev.encode({ id: 11566, idx: 0, tmpid: 1400011566, reason: 10, count: 1, relev: 1, check: true }),
    ];
    t.deepEqual(setRelevance(4, stack, [0,1,1]), {
        relevance: 0.75,
        sets: stack.slice(0,3)
    }, 'real: merrick rd merrick (with city)');

    stack = [
        Relev.encode({ id: 2064953, idx: 2, tmpid: 2202064953, reason: 14, count: 2, relev: 1, check: true }),
        Relev.encode({ id: 18537894, idx: 1, tmpid: 2118537894, reason: 14, count: 2, relev: 1, check: true }),
    ];
    t.deepEqual(setRelevance(4, stack, [0,1,1]), {
        relevance: 0.49,
        sets: stack.slice(0,2)
    }, 'real: merrick rd merrick (no city)');

    t.end();
});
