/*
 * 2/6 2016
 * author: buaa_zhang
 *
 */

#include <pthread.h>
#include <stddef.h>
#include <cstdint>

#include "gcache_global.h"
#include "gcache_advice.h"
#include "gcache_tmp_storage_hdfs.h"
#include "gcache_tmp_storage_local.h"
#include "gcache_tmp_storage_nas.h"
#include "gcache_tmp_storage.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {
Storage* Storage::GetInstance(StorageArch s_arch) {

    if (_storageTable == NULL) {

        pthread_mutex_lock(&_table_mutex);
        if (_storageTable == NULL) {
            _storageTable = new Storage*[GCACHE_STORAGE_ARCH_NUM];
            for (uint32_t i = 0; i < GCACHE_STORAGE_ARCH_NUM; i++) {
                _storageTable[i] = NULL;
            }
        }
        pthread_mutex_unlock(&_table_mutex);

    }

    Storage* storage = NULL;

    switch (s_arch) {
        case STORAGE_LOCAL:
            if (_storageTable[STORAGE_LOCAL] != NULL) storage = _storageTable[STORAGE_LOCAL];
            else {
                pthread_mutex_lock(&_local_mutex);
                if (_storageTable[STORAGE_LOCAL] == NULL) {
                    storage = new StorageLocal();
                    if (storage != NULL) {
                        if (!storage->Init()) {
                            storage = NULL;
                            _storageTable[STORAGE_LOCAL] = NULL;
                        } else {
                            _storageTable[STORAGE_LOCAL] = storage;
                        }
                    }
                }
                pthread_mutex_unlock(&_local_mutex);
            }
            break;
        case STORAGE_DIST:
            if (_storageTable[STORAGE_DIST] != NULL) storage = _storageTable[STORAGE_DIST];
            else {
                pthread_mutex_lock(&_dist_mutex);
                if (_storageTable[STORAGE_DIST] == NULL) {
                    storage = new StorageHDFS();
                    if (storage != NULL) {
                        if (!storage->Init()) {
                            storage = NULL;
                            _storageTable[STORAGE_DIST] = NULL;
                        } else {
                            _storageTable[STORAGE_DIST] = storage;
                        }
                    }
                }
                pthread_mutex_unlock(&_dist_mutex);
            }
            break;
        case STORAGE_NAS:
            if (_storageTable[STORAGE_NAS] != NULL) storage = _storageTable[STORAGE_NAS];
            else {
                pthread_mutex_lock(&_nas_mutex);
                if (_storageTable[STORAGE_NAS] == NULL) {
                    storage = new StorageNAS();
                    if (storage != NULL) {
                        if (!storage->Init()) {
                            storage = NULL;
                            _storageTable[STORAGE_NAS] = NULL;
                        } else {
                            _storageTable[STORAGE_NAS] = storage;
                        }
                    }
                }
                pthread_mutex_unlock(&_nas_mutex);

            }
            break;
    }

    return storage;

}

Storage** Storage::_storageTable = NULL;

pthread_mutex_t Storage::_table_mutex;
pthread_mutex_t Storage::_local_mutex;
pthread_mutex_t Storage::_nas_mutex;
pthread_mutex_t Storage::_dist_mutex;

NAMESPACE_END_SEISFS
}
