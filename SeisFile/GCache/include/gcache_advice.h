/*
 * gcache_advice.h
 *
 *  Created on: Dec 7, 2015
 *      Author: wangyida
 */

#ifndef GCACHE_ADVICE_H_
#define GCACHE_ADVICE_H_

#include "gcache_global.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {

#define GCACHE_STORAGE_ARCH_NUM 3
enum StorageArch {
    STORAGE_LOCAL = 0, STORAGE_DIST = 1, STORAGE_NAS = 2
};

enum CacheLevel {
    CACHE_L1 = 0, CACHE_L2 = 1, CACHE_L3 = 2
};
enum RootType {
    META = 1, DATA = 0
};

enum FAdvice {
    FADV_NONE = 0, FADV_RAW = 1, FADV_SEQUENTIAL = 2, FADV_RANDOM = 3, FADV_MMAP = 4
};

enum CompressAdvice {
    CMP_NON = 0, CMP_GZIP = 1
};

typedef unsigned short lifetime_t;
#define LIFETIME_RUNTIME_BOUND         0
#define LIFETIME_DEFAULT                 30

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_ADVICE_H_ */
