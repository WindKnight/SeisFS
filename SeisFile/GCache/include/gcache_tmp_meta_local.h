/*
 * gcache_tmp_meta_local.h
 *
 *  Created on: Jan 21, 2016
 *      Author: zch
 */

#ifndef GCACHE_TMP_META_LOCAL_H_
#define GCACHE_TMP_META_LOCAL_H_

#include "gcache_global.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {
struct MetaTMP;

struct MetaTMPLocal {
    int version;
    struct MetaTMP meta;
    MetaTMPLocal() {
        version = META_VERSION;
    }

    // add context of hostname, pid, creation time
};

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_TMP_META_LOCAL_H_ */
