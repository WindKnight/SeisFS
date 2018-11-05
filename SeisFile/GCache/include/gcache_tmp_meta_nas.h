/*
 * gcache_tmp_meta_nas.h
 *
 *  Created on: Jan 21, 2016
 *      Author: zch
 */

#ifndef GCACHE_TMP_META_NAS_H_
#define GCACHE_TMP_META_NAS_H_

#include "gcache_global.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {
struct MetaTMP;

struct MetaTMPNAS {
    int version;
    struct MetaTMP meta;

    MetaTMPNAS() {
        version = META_VERSION;
    }

};

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_TMP_META_NAS_H_ */
