var coalesceZooms = require('../index.js').Cache.coalesceZooms;
var test = require('tape');

test('zero case', function(q) {
    var coalesced = coalesceZooms([], []);
    q.deepEqual(coalesced, {});
    q.end();
});

var mp39 = Math.pow(2,39);
var mp25 = Math.pow(2,25);

test('coalesce dupe zooms', function(q) {
    // The data for this test is from the query "holyoke massachusetts"
    // against the province and place indexes.
    var coalesced = coalesceZooms(
        // grids
        [
            [ (0*mp39 + 0*mp25 + 10),  // id:10 @ 1/0/0
            ],
            [ (0*mp39 + 0*mp25 + 11),  // id:10 @ 1/0/0
              (0*mp39 + 0*mp25 + 12),  // id:12 @ 1/0/0
              (1*mp39 + 1*mp25 + 13),  // id:11 @ 1/1/1
            ]
        ],
        // zooms
        [ 1, 1]);
    // Reformat encoded zxy's and map full features to just their IDs for
    // easier debugging/assertion of correct results.
    var z, x, y;
    var coalescedCount = {};
    var coalescedKeys = {};
    for (var zxy in coalesced) {
        z = Math.floor(zxy/Math.pow(2,28));
        x = Math.floor(zxy%Math.pow(2,28)/Math.pow(2,14));
        y = zxy % Math.pow(2,14)
        var key = [z,x,y].join('/');
        coalescedCount[key] = coalesced[zxy].slice(0);
        coalescedKeys[key] = coalesced[zxy].key;
    }
    q.deepEqual(coalescedCount, {
        '1/0/0': [ 10, mp25 + 11, mp25 + 12 ],
        '1/1/1': [ mp25 + 13 ]
    });
    q.deepEqual(coalescedKeys, {
        '1/0/0': [ 10, mp25 + 11, mp25 + 12 ].join('-'),
        '1/1/1': [ mp25 + 13 ].join('-')
    });
    q.end();
});

test('basic coalesce', function(q) {
    // The data for this test is from the query "holyoke massachusetts"
    // against the province and place indexes.
    var coalesced = coalesceZooms(
        // grids
        [
            [ 83019436130799,
              83019436130800,
              83019469685231,
              83569191944687,
              83569225499119,
              83569259053551,
              84118947758575,
              84118981313007,
              84119014867439,
              84668703572463,
              84668737126895,
              84668770681327,
              84668804235759,
              85218459386351,
              85218492940783,
              85218526495215,
              85218560049647,
              85768248754671,
              85768282309103,
              85768315863535 ],
            [ 242468150844959,
              242468184399391,
              243017906658847,
              243017940213279,
              273802688856591,
              335376480745316,
              335376514299748,
              335926236559204 ]
        ],
        // zooms
        [ 9, 11 ]);
    // Reformat encoded zxy's and map full features to just their IDs for
    // easier debugging/assertion of correct results.
    var z, x, y;
    var coalescedCount = {};
    var coalescedKeys = {};
    for (var zxy in coalesced) {
        z = Math.floor(zxy/Math.pow(2,28));
        x = Math.floor(zxy%Math.pow(2,28)/Math.pow(2,14));
        y = zxy % Math.pow(2,14)
        var key = [z,x,y].join('/');
        coalescedCount[key] = coalesced[zxy].slice(0);
        coalescedKeys[key] = coalesced[zxy].key;
    }
    q.deepEqual({
        '9/151/188': [ 495, 496 ],
        '9/151/189': [ 495 ],
        '9/152/188': [ 495 ],
        '9/152/189': [ 495 ],
        '9/152/190': [ 495 ],
        '9/153/188': [ 495 ],
        '9/153/189': [ 495 ],
        '9/153/190': [ 495 ],
        '9/154/188': [ 495 ],
        '9/154/189': [ 495 ],
        '9/154/190': [ 495 ],
        '9/154/191': [ 495 ],
        '9/155/188': [ 495 ],
        '9/155/189': [ 495 ],
        '9/155/190': [ 495 ],
        '9/155/191': [ 495 ],
        '9/156/189': [ 495 ],
        '9/156/190': [ 495 ],
        '9/156/191': [ 495 ],
        '11/441/770': [ mp25 + 7711 ],
        '11/441/771': [ mp25 + 7711 ],
        '11/442/770': [ mp25 + 7711 ],
        '11/442/771': [ mp25 + 7711 ],
        '11/498/724': [ mp25 + 131599 ],
        '11/610/758': [ mp25 + 14180, 495 ],
        '11/610/759': [ mp25 + 14180, 495 ],
        '11/611/758': [ mp25 + 14180, 495 ]
    }, coalescedCount);
    q.deepEqual({
        '11/441/770': (mp25 + 7711) + '',
        '11/441/771': (mp25 + 7711) + '',
        '11/442/770': (mp25 + 7711) + '',
        '11/442/771': (mp25 + 7711) + '',
        '11/498/724': (mp25 + 131599) + '',
        '11/610/758': (mp25 + 14180) + '-495',
        '11/610/759': (mp25 + 14180) + '-495',
        '11/611/758': (mp25 + 14180) + '-495',
        '9/151/188': '495-496',
        '9/151/189': '495',
        '9/152/188': '495',
        '9/152/189': '495',
        '9/152/190': '495',
        '9/153/188': '495',
        '9/153/189': '495',
        '9/153/190': '495',
        '9/154/188': '495',
        '9/154/189': '495',
        '9/154/190': '495',
        '9/154/191': '495',
        '9/155/188': '495',
        '9/155/189': '495',
        '9/155/190': '495',
        '9/155/191': '495',
        '9/156/189': '495',
        '9/156/190': '495',
        '9/156/191': '495'
    }, coalescedKeys);
    q.end();
});

