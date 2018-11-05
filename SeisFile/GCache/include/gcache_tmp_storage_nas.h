/*
 * gcache_tmp_storage_nas.h
 *
 *  Created on: Jan 21, 2016
 *      Author: zch
 */

#ifndef GCACHE_TMP_STORAGE_NAS_H_
#define GCACHE_TMP_STORAGE_NAS_H_

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

#define CACHE_NUM 3 //Macro of Cache level
/**
 * StorageNAS class is a child of Storage.
 * this class provides some functions of Storage operations in NAS storage.
 */
class StorageNAS: public Storage {
public:
    StorageNAS();
    /**
     * destructor, destruct object.
     */
    virtual ~StorageNAS();
    /**
     * Write meta info for the given filename
     */
    virtual bool PutMeta(const std::string& filename, const char* metaData, int size,
            MetaType meta_type);
    /**
     * Read meta info for the given filename
     */
    virtual bool GetMeta(const std::string& filename, char*& metaData, int& size,
            MetaType meta_type);
    /**
     * Delete meta info for the given filename
     */
    virtual bool DeleteMeta(const std::string& filename, MetaType meta_type);
    /**
     * Rename meta file name for the given filename
     */
    virtual bool RenameMeta(const std::string& old_name, const std::string &new_name,
            MetaType meta_type);
    /**
     * Make meta dir for the given path
     */
    virtual bool MakeMetaDir(const std::string& dir_name);
    /**
     * Get the time last accessed for the given filename
     */
    virtual uint64_t LastAccessed(const std::string &filename, MetaType meta_type);
    /**
     * Get root dir path by dirID
     */
    virtual std::string GetRootDir(int dirID);
    /**
     * Get total space of the root of the given dirID
     */
    virtual int64_t GetRootTotalSpace(int dirID);
    /**
     * Get free space of the root of the given dirID
     */
    virtual int64_t GetRootFreeSpace(int dirID);
    /**
     * Choose a root dir for given cachelevel
     */
    virtual int ElectRootDir(CacheLevel cache_lv);
    /**
     * Get real cachelevel for the given cache level
     */
    virtual CacheLevel GetRealCacheLevel(CacheLevel cache_lv);
    /**
     * Get cache level of a given root dirID
     */
    virtual CacheLevel GetCacheLevel(int dirID);
    /**
     * Get all root dir of the system
     */
    virtual std::map<int, DiskInfo> *GetAllRootDir();
    /**
     * Get all cache info of the system
     */
    virtual std::vector<Storage::DiskInfo> *GetCacheInfo();

protected:

private:
    /**
     * Do some initial process to init the member variables
     */
    virtual bool Init();
    /**
     * Parse Config Files and do some of the process
     */
    bool ParseParam();

    std::string _metaDirName; //meta path of the system

    std::vector<DiskInfo> _cacheInfo[CACHE_NUM]; //_cacheInfo of the system
    std::map<int, DiskInfo> _rootDirs; //all root dirs of the system indexed by dirID
    std::map<int, DiskInfo> _metaDirs; // all meta dirs of the system indexed by dirID
};

NAMESPACE_END_SEISFS
}

#endif
