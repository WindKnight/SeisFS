/*
 * gcache_tmp_dir.cpp
 *
 *  Created on: Jan 8, 2016
 *      Author: wyd
 */

#include "gcache_tmp_dir.h"

#include <cerrno>
#include <string>

#include "gcache_global.h"
#include "gcache_advice.h"
#include "gcache_tmp_error.h"
#include "gcache_tmp_dir_hdfs.h"
#include "gcache_tmp_dir_local.h"
#include "gcache_tmp_dir_nas.h"

//extern int GCACHE_errno;
NAMESPACE_BEGIN_SEISFS
namespace tmp {
Dir *Dir::New(const std::string &dir_name, StorageArch &s_arch) {
    Dir *dir = NULL;
    switch (s_arch) {
        case STORAGE_LOCAL:
            dir = new DirLocal(dir_name);
            break;
        case STORAGE_DIST:
            dir = new DirHDFS(dir_name);
            break;
        case STORAGE_NAS:
            dir = new DirNAS(dir_name);
            break;
        default:
            GCACHE_tmp_errno = GCACHE_TMP_ERR_NOARCH;
            return NULL;
    }

    if (!dir->Init()) {
        delete dir;
        if (s_arch == STORAGE_DIST) {
            dir = new DirNAS(dir_name);
            if (dir->Init()) {
                s_arch = STORAGE_NAS;
                return dir;
            }
        }
        return NULL;
    }

    return dir;
}

NAMESPACE_END_SEISFS
}

