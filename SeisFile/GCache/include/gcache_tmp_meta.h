/*
 * gcache_tmp_meta.h
 *
 *  Created on: Jan 21, 2016
 *      Author: zch
 */

#ifndef GCACHE_TMP_META_H_
#define GCACHE_TMP_META_H_

#include <sys/types.h>

#include "gcache_global.h"
#include "seisfs_meta.h"

#define META_VERSION 1

NAMESPACE_BEGIN_SEISFS
namespace tmp {

struct MetaTMP {
    struct MetaHeader header;
    int16_t dirID; // use dirID to choose root dir
    int16_t CompAdivce;
};

enum MetaType {
    META_TMP_FILE = 0, META_TMP_KVTABLE = 1,
};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_TMP_META_H_ */
