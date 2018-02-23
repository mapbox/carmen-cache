
#include "cpp_util.hpp"

namespace carmen {

// Converts from the packed integer into (relev, score, x, y, feature_id)
Cover numToCover(uint64_t num) {
    Cover cover;
    assert(((num >> 34) % POW2_14) <= static_cast<double>(std::numeric_limits<unsigned short>::max()));
    assert(((num >> 34) % POW2_14) >= static_cast<double>(std::numeric_limits<unsigned short>::min()));
    unsigned short y = static_cast<unsigned short>((num >> 34) % POW2_14);
    assert(((num >> 20) % POW2_14) <= static_cast<double>(std::numeric_limits<unsigned short>::max()));
    assert(((num >> 20) % POW2_14) >= static_cast<double>(std::numeric_limits<unsigned short>::min()));
    unsigned short x = static_cast<unsigned short>((num >> 20) % POW2_14);
    assert(((num >> 48) % POW2_3) <= static_cast<double>(std::numeric_limits<unsigned short>::max()));
    assert(((num >> 48) % POW2_3) >= static_cast<double>(std::numeric_limits<unsigned short>::min()));
    unsigned short score = static_cast<unsigned short>((num >> 48) % POW2_3);
    uint32_t id = static_cast<uint32_t>(num % POW2_20);
    bool matches_language = static_cast<bool>(num & LANGUAGE_MATCH_BOOST);
    cover.x = x;
    cover.y = y;
    double relev = 0.4 + (0.2 * static_cast<double>((num >> 51) % POW2_2));
    cover.relev = relev;
    cover.score = score;
    cover.id = id;
    cover.matches_language = matches_language;

    // These are not derived from decoding the input num but by
    // external values after initialization.
    cover.idx = 0;
    cover.mask = 0;
    cover.tmpid = 0;
    cover.distance = 0;

    return cover;
}

// Converts the ZXY coordinates of the tile that contains a proximity point to a
// ZXY at the appropriate zoom level necessary for a given coalesce operation
ZXY pxy2zxy(unsigned z, unsigned x, unsigned y, unsigned target_z) {
    ZXY zxy;
    zxy.z = target_z;

    // Interval between parent and target zoom level
    signed zDist = static_cast<signed>(target_z) - static_cast<signed>(z);
    signed zMult = zDist - 1;
    if (zDist == 0) {
        zxy.x = x;
        zxy.y = y;
        return zxy;
    }

    // Midpoint length @ z for a tile at parent zoom level
    double pMid_d = static_cast<double>(std::pow(2, zDist) / 2);
    assert(pMid_d <= static_cast<double>(std::numeric_limits<unsigned>::max()));
    assert(pMid_d >= static_cast<double>(std::numeric_limits<unsigned>::min()));
    signed pMid = static_cast<signed>(pMid_d);
    zxy.x = static_cast<unsigned>((static_cast<signed>(x) * zMult) + pMid);
    zxy.y = static_cast<unsigned>((static_cast<signed>(y) * zMult) + pMid);
    return zxy;
}


// Calculates a ZXY for appropriate for a given coalesce operation out of the supplied bbox ZXY coordinates.
ZXY bxy2zxy(unsigned z, unsigned x, unsigned y, unsigned target_z, bool max) {
    ZXY zxy;
    zxy.z = target_z;

    // Interval between parent and target zoom level
    signed zDist = static_cast<signed>(target_z) - static_cast<signed>(z);
    if (zDist == 0) {
        zxy.x = x;
        zxy.y = y;
        return zxy;
    }

    // zoom conversion multiplier
    float mult = static_cast<float>(std::pow(2, zDist));

    // zoom in min
    if (zDist > 0 && !max) {
        zxy.x = static_cast<unsigned>(static_cast<float>(x) * mult);
        zxy.y = static_cast<unsigned>(static_cast<float>(y) * mult);
        return zxy;
    }
    // zoom in max
    else if (zDist > 0 && max) {
        zxy.x = static_cast<unsigned>(static_cast<float>(x) * mult + (mult - 1));
        zxy.y = static_cast<unsigned>(static_cast<float>(y) * mult + (mult - 1));
        return zxy;
    }
    // zoom out
    else {
        unsigned mod = static_cast<unsigned>(std::pow(2, target_z));
        unsigned xDiff = x % mod;
        unsigned yDiff = y % mod;
        unsigned newX = x - xDiff;
        unsigned newY = y - yDiff;

        zxy.x = static_cast<unsigned>(static_cast<float>(newX) * mult);
        zxy.y = static_cast<unsigned>(static_cast<float>(newY) * mult);
        return zxy;
    }
}

// Equivalent of scoredist() function in carmen
// Combines score and distance into a single score that can be used for sorting.
// Unlike carmen the effect is not scaled by zoom level as regardless of index
// the score value at this stage is a 0-7 scalar (by comparison, in carmen, scores
// for indexes with features covering lower zooms often have exponentially higher
// scores - example: country@z9 vs poi@z14).
double scoredist(unsigned zoom, double distance, double score, double radius) {
    if (zoom < 6) zoom = 6;
    if (distance == 0.0) distance = 0.01;
    double scoredist = 0;

    // Since distance is in tiles we calculate scoredist by converting the miles into
    // a tile unit value at the appropriate zoom first.
    //
    // 32 tiles is about 40 miles at z14, use this as our mile <=> tile conversion.
    scoredist = ((radius * (32.0 / 40.0)) / _pow(1.5, 14 - static_cast<int>(zoom))) / distance;

    return score > scoredist ? score : scoredist;
}

// Open database for read-write availability
rocksdb::Status OpenDB(const rocksdb::Options& options, const std::string& name, std::unique_ptr<rocksdb::DB>& dbptr) {
    rocksdb::DB* db;
    rocksdb::Status status = rocksdb::DB::Open(options, name, &db);
    dbptr = std::unique_ptr<rocksdb::DB>(db);
    return status;
}

// Open database for read-only availability
rocksdb::Status OpenForReadOnlyDB(const rocksdb::Options& options, const std::string& name, std::unique_ptr<rocksdb::DB>& dbptr) {
    rocksdb::DB* db;
    rocksdb::Status status = rocksdb::DB::OpenForReadOnly(options, name, &db);
    dbptr = std::unique_ptr<rocksdb::DB>(db);
    return status;
}

} // namespace carmen
