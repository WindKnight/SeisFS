/*
 * gcache_tmp_file.cpp
 *
 *  Created on: Dec 28, 2015
 *      Author: wyd
 */

#include "gcache_tmp_file.h"

#include <cerrno>
#include <string>

#include "gcache_global.h"
#include "gcache_advice.h"
#include "gcache_tmp_error.h"
#include "gcache_tmp_file_hdfs.h"
#include "gcache_tmp_file_local.h"
#include "gcache_tmp_file_nas.h"

//extern int GCACHE_errno;
NAMESPACE_BEGIN_SEISFS
namespace tmp {
File* File::New(const std::string &file_name, StorageArch &s_arch) {
    File *file = NULL;
    switch (s_arch) {
        case STORAGE_LOCAL:
            file = new FileLocal(file_name);
            break;
        case STORAGE_DIST:
            file = new FileHDFS(file_name);
            break;
        case STORAGE_NAS:
            file = new FileNAS(file_name);
            break;
        default:
            GCACHE_tmp_errno = GCACHE_TMP_ERR_NOARCH;
            return NULL;
    }

    if (!file->Init()) {
        delete file;
        if (s_arch == STORAGE_DIST) {
            file = new FileNAS(file_name);
            if (file->Init()) {
                s_arch = STORAGE_NAS;
                return file;
            }
        }
        return NULL;
    }
    return file;
}

NAMESPACE_END_SEISFS
}
