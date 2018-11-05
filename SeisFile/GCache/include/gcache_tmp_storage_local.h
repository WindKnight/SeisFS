/*
 * gcache_tmp_config_local.h
 *
 *  Created on: Jan 21, 2016
 *      Author: zch
 */

#ifndef GCACHE_TMP_CONFIG_LOCAL_H_
#define GCACHE_TMP_CONFIG_LOCAL_H_

#include <sys/types.h>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "gcache_global.h"
#include "gcache_advice.h"
#include "gcache_tmp_meta.h"
#include "gcache_tmp_storage.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {

#define CACHE_NUM 3

class StorageLocal: public Storage {
public:
    StorageLocal();

    virtual ~StorageLocal();

    virtual bool PutMeta(const std::string& filename, const char* metaData, int size,
            MetaType meta_type);

    virtual bool GetMeta(const std::string& filename, char*& metaData, int& size,
            MetaType meta_type);

    virtual bool DeleteMeta(const std::string& filename, MetaType meta_type);

    virtual bool RenameMeta(const std::string& old_name, const std::string &new_name,
            MetaType meta_type);

    virtual bool MakeMetaDir(const std::string& dir_name);

    virtual uint64_t LastAccessed(const std::string &filename, MetaType meta_type);

    virtual std::string GetRootDir(int dirID);

    virtual int64_t GetRootTotalSpace(int dirID);

    virtual int64_t GetRootFreeSpace(int dirID);

    virtual int ElectRootDir(CacheLevel cache_lv);

    virtual CacheLevel GetRealCacheLevel(CacheLevel cache_lv);

    virtual CacheLevel GetCacheLevel(int dirID);

    virtual std::map<int, DiskInfo> *GetAllRootDir();

    virtual std::vector<Storage::DiskInfo> *GetCacheInfo();

protected:

private:
    virtual bool Init();

    bool ParseParam();

    std::string _metaDirName;

    std::vector<DiskInfo> _cacheInfo[CACHE_NUM];
    std::map<int, DiskInfo> _rootDirs;
    std::map<int, DiskInfo> _metaDirs;
};

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_TMP_CONFIG_LOCAL_H_ */
