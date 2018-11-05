/*
 * gcache_tmp_storage.h
 *
 *  Created on: 2/6, 2016
 *      Author: buaa_zhang
 */

#ifndef GCACHE_TMP_STORAGE_H_
#define GCACHE_TMP_STORAGE_H_

#include <pthread.h>
#include <stdint.h>
#include <sys/types.h>
#include <map>
#include <string>
#include <vector>

#include "gcache_global.h"
#include "gcache_advice.h"
#include "gcache_tmp_meta.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {

class Storage {
public:

    struct DiskInfo {
        int dirID;
        int quota;
        std::string rootDir;
        CacheLevel cache_lv;
        int64_t used_space;

        //Add new Attri
        RootType root_t;

    };

    virtual ~Storage() {
    }

    /* Singleton Pattern.
     * Initialize the  _storageConfig when GetInstance is first invoked.
     */
    static Storage* GetInstance(StorageArch s_arch);
    /**
     * Return the name of the specified local directory.
     */
    virtual std::string GetRootDir(int dirID) = 0;

    virtual int64_t GetRootTotalSpace(int dirID) = 0;

    virtual int64_t GetRootFreeSpace(int dirID) = 0;

    virtual bool PutMeta(const std::string& filename, const char* metaData, int size,
            MetaType meta_type) = 0;

    virtual bool GetMeta(const std::string& filename, char*& metaData, int& size,
            MetaType meta_type) = 0;

    virtual bool DeleteMeta(const std::string& filename, MetaType meta_type) = 0;

    virtual bool RenameMeta(const std::string& old_name, const std::string &new_name,
            MetaType meta_type) = 0;

    virtual bool MakeMetaDir(const std::string& dir_name) = 0;

    virtual uint64_t LastAccessed(const std::string &filename, MetaType meta_type) = 0;

    /**
     * Elect the ID of a root directory who has the most capacity.
     */
    virtual int ElectRootDir(CacheLevel cache_lv) = 0;

    virtual CacheLevel GetRealCacheLevel(CacheLevel cache_lv) = 0;

    virtual CacheLevel GetCacheLevel(int dirID) = 0;

    virtual std::map<int, DiskInfo> *GetAllRootDir() = 0;

    virtual std::vector<struct DiskInfo> *GetCacheInfo() = 0;

protected:
    Storage() {
    }
    ;

private:

    virtual bool Init() = 0;

    static Storage** _storageTable;

    static pthread_mutex_t _table_mutex;
    static pthread_mutex_t _local_mutex;
    static pthread_mutex_t _nas_mutex;
    static pthread_mutex_t _dist_mutex;

};

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_TMP_STORAGE_H_ */
