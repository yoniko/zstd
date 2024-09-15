//
// Created by yoniko on 7/6/22.
//

#include "zstd_entropy.h"

// Table was pre-generated in python using [0x80000 ^ int(math.log2(i)*(1<<16))
// for i in range(1<<8, 1<<9)]
#define LOG2_TABLE_PRECISION_BITS (8)
#define LOG2_TABLE_MULTIPLIER (1 << 16)
const uint16_t LOG2_TABLE[1 << LOG2_TABLE_PRECISION_BITS] = {
        0,     368,   735,   1101,  1465,  1828,  2190,  2550,  2909,  3266,  3622,
        3977,  4331,  4683,  5034,  5383,  5731,  6078,  6424,  6769,  7112,  7454,
        7794,  8134,  8472,  8809,  9145,  9480,  9813,  10146, 10477, 10807, 11136,
        11463, 11790, 12115, 12440, 12763, 13085, 13406, 13726, 14045, 14363, 14680,
        14995, 15310, 15624, 15936, 16248, 16558, 16868, 17176, 17484, 17790, 18096,
        18400, 18704, 19006, 19308, 19608, 19908, 20207, 20505, 20801, 21097, 21392,
        21686, 21980, 22272, 22563, 22854, 23143, 23432, 23720, 24007, 24293, 24578,
        24862, 25146, 25429, 25710, 25991, 26272, 26551, 26829, 27107, 27384, 27660,
        27935, 28210, 28483, 28756, 29028, 29300, 29570, 29840, 30109, 30377, 30644,
        30911, 31177, 31442, 31707, 31971, 32234, 32496, 32757, 33018, 33278, 33538,
        33796, 34054, 34312, 34568, 34824, 35079, 35334, 35588, 35841, 36093, 36345,
        36596, 36847, 37096, 37346, 37594, 37842, 38089, 38336, 38582, 38827, 39071,
        39315, 39559, 39801, 40044, 40285, 40526, 40766, 41006, 41245, 41483, 41721,
        41959, 42195, 42431, 42667, 42902, 43136, 43370, 43603, 43836, 44068, 44299,
        44530, 44760, 44990, 45219, 45448, 45676, 45904, 46131, 46357, 46583, 46808,
        47033, 47257, 47481, 47704, 47927, 48149, 48371, 48592, 48813, 49033, 49253,
        49472, 49690, 49909, 50126, 50343, 50560, 50776, 50992, 51207, 51421, 51635,
        51849, 52062, 52275, 52487, 52699, 52910, 53121, 53331, 53541, 53751, 53960,
        54168, 54376, 54584, 54791, 54998, 55204, 55410, 55615, 55820, 56024, 56228,
        56432, 56635, 56837, 57040, 57242, 57443, 57644, 57844, 58044, 58244, 58443,
        58642, 58841, 59039, 59236, 59433, 59630, 59827, 60023, 60218, 60413, 60608,
        60802, 60996, 61190, 61383, 61576, 61768, 61960, 62152, 62343, 62534, 62724,
        62914, 63104, 63293, 63482, 63671, 63859, 64047, 64234, 64421, 64608, 64794,
        64980, 65165, 65351
};

#if 1

size_t ZSTD_entropyCost(unsigned const* count, unsigned const max, size_t const totalElements)
{
    int64_t intEntropy = 0;
    int64_t normalize = 0;
    if (totalElements == 0) {
        return 0;
    }
    normalize = ((int64_t)1 << 62) / (int64_t)totalElements;
    for (size_t i = 0; i < max; i++) {
        unsigned int const currCount = count[i];
        int64_t logIndex             = (int64_t)(currCount * normalize);
        int const clz                = (int) ZSTD_countLeadingZeros64(
                (uint64_t)logIndex | (1 << (LOG2_TABLE_PRECISION_BITS + 1)));
        const int shift = (62 - LOG2_TABLE_PRECISION_BITS) - clz + 1;
        logIndex >>= shift;
        logIndex = logIndex & ((1 << LOG2_TABLE_PRECISION_BITS) - 1);
        intEntropy += -(int64_t)currCount
                      * (LOG2_TABLE[logIndex] - clz * LOG2_TABLE_MULTIPLIER);
    }
    return (size_t)(((double)intEntropy)
              / ((double)LOG2_TABLE_MULTIPLIER)
              - (double)totalElements);
}

size_t ZSTD_crossEntropyCost(short const* norm, unsigned accuracyLog,
                             unsigned const* count, unsigned const max)
{
    int64_t intEntropy = 0;
    size_t totalElements = 0;
    int64_t normalize;
    for(size_t i = 0; i < max; i++) {
        totalElements += count[i];
    }
    if (totalElements == 0) {
        return 0;
    }
    // Need to make sure that `accuracyLog <= LOG2_TABLE_PRECISION_BITS`
    normalize = (int64_t)1 << (62 - accuracyLog);
    for (size_t i = 0; i < max; i++) {
        unsigned const currCount = (norm[i] != -1) ? (unsigned)norm[i] : 1;
        int64_t logIndex             = (int64_t)(currCount * normalize);
        int const clz                = (int) ZSTD_countLeadingZeros64(
                (uint64_t)logIndex | (1 << (LOG2_TABLE_PRECISION_BITS + 1)));
        const int shift = (62 - LOG2_TABLE_PRECISION_BITS) - clz + 1;
        logIndex >>= shift;
        logIndex = logIndex & ((1 << LOG2_TABLE_PRECISION_BITS) - 1);
        intEntropy += -(int64_t)count[i]
                      * (LOG2_TABLE[logIndex] - clz * LOG2_TABLE_MULTIPLIER);
    }
    return (size_t)(((double)intEntropy)
           / ((double)LOG2_TABLE_MULTIPLIER)
           - (double)totalElements);
}

#else
/**
 * -log2(x / 256) lookup table for x in [0, 256).
 * If x == 0: Return 0
 * Else: Return floor(-log2(x / 256) * 256)
 */
static unsigned const kInverseProbabilityLog256[256] = {
        0,    2048, 1792, 1642, 1536, 1453, 1386, 1329, 1280, 1236, 1197, 1162,
        1130, 1100, 1073, 1047, 1024, 1001, 980,  960,  941,  923,  906,  889,
        874,  859,  844,  830,  817,  804,  791,  779,  768,  756,  745,  734,
        724,  714,  704,  694,  685,  676,  667,  658,  650,  642,  633,  626,
        618,  610,  603,  595,  588,  581,  574,  567,  561,  554,  548,  542,
        535,  529,  523,  517,  512,  506,  500,  495,  489,  484,  478,  473,
        468,  463,  458,  453,  448,  443,  438,  434,  429,  424,  420,  415,
        411,  407,  402,  398,  394,  390,  386,  382,  377,  373,  370,  366,
        362,  358,  354,  350,  347,  343,  339,  336,  332,  329,  325,  322,
        318,  315,  311,  308,  305,  302,  298,  295,  292,  289,  286,  282,
        279,  276,  273,  270,  267,  264,  261,  258,  256,  253,  250,  247,
        244,  241,  239,  236,  233,  230,  228,  225,  222,  220,  217,  215,
        212,  209,  207,  204,  202,  199,  197,  194,  192,  190,  187,  185,
        182,  180,  178,  175,  173,  171,  168,  166,  164,  162,  159,  157,
        155,  153,  151,  149,  146,  144,  142,  140,  138,  136,  134,  132,
        130,  128,  126,  123,  121,  119,  117,  115,  114,  112,  110,  108,
        106,  104,  102,  100,  98,   96,   94,   93,   91,   89,   87,   85,
        83,   82,   80,   78,   76,   74,   73,   71,   69,   67,   66,   64,
        62,   61,   59,   57,   55,   54,   52,   50,   49,   47,   46,   44,
        42,   41,   39,   37,   36,   34,   33,   31,   30,   28,   26,   25,
        23,   22,   20,   19,   17,   16,   14,   13,   11,   10,   8,    7,
        5,    4,    2,    1,
};

/**
 * Returns the cost in bits of encoding the distribution described by count
 * using the entropy bound.
 */
size_t ZSTD_entropyCost(unsigned const* count, unsigned const max, size_t const total)
{
    unsigned cost = 0;
    unsigned s = 0;

    assert(total > 0);
    uint64_t norm_factor = ((uint64_t)1 << 62) / total;
    for (s = 0; s <= max; ++s) {
        unsigned norm = (unsigned)((count[s]*norm_factor)>>(62-8));
        if (count[s] != 0 && norm == 0)
            norm = 1;
        assert(count[s] < total);
        cost += count[s] * kInverseProbabilityLog256[norm];
    }
    return (double)(cost >> 8);
}

/**
 * Returns the cost in bits of encoding the distribution in count using the
 * table described by norm. The max symbol support by norm is assumed >= max.
 * norm must be valid for every symbol with non-zero probability in count.
 */
size_t ZSTD_crossEntropyCost(short const* norm, unsigned accuracyLog,
                             unsigned const* count, unsigned const max)
{
    unsigned const shift = 8 - accuracyLog;
    size_t cost = 0;
    unsigned s;
    assert(accuracyLog <= 8);
    for (s = 0; s <= max; ++s) {
        unsigned const normAcc = (norm[s] != -1) ? (unsigned)norm[s] : 1;
        unsigned const norm256 = normAcc << shift;
        assert(norm256 > 0);
        assert(norm256 < 256);
        cost += count[s] * kInverseProbabilityLog256[norm256];
    }
    return cost >> 8;
}
#endif