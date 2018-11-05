/*
 * gcache_meta_dis.h
 *
 *  Created on: Jan 8, 2016
 *      Author: wyd
 */

#ifndef GCACHE_META_DIS_H_
#define GCACHE_META_DIS_H_

#include "gcache_global.h"
#include "gcache_tmp_meta.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {

struct MetaTMPHDFS {
    int version;
    struct MetaTMP meta;

    MetaTMPHDFS() {
        version = META_VERSION;
    }
};

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_META_DIS_H_ */
