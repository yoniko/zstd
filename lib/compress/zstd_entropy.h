//
// Created by yoniko on 7/6/22.
//

#ifndef ZSTD_ZSTD_ENTROPY_H
#define ZSTD_ZSTD_ENTROPY_H

#include "../common/zstd_internal.h"


/**
 * Returns the cost in bits of encoding the distribution described by count
 * using the entropy bound.
 */
size_t ZSTD_entropyCost(unsigned const* count, unsigned const max, size_t const total);


/**
 * Returns the cost in bits of encoding the distribution in count using the
 * table described by norm. The max symbol support by norm is assumed >= max.
 * norm must be valid for every symbol with non-zero probability in count.
 */
size_t ZSTD_crossEntropyCost(short const* norm, unsigned accuracyLog,
                             unsigned const* count, unsigned const max);

#endif //ZSTD_ZSTD_ENTROPY_H
