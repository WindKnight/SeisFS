/*
 * gcache_tmp_storage_hdfs.h
 *
 *  Created on: Jan 21, 2016
 *      Author: zch
 */

#ifndef GCACHE_TMP_STORAGE_HDFS_H_
#define GCACHE_TMP_STORAGE_HDFS_H_

#include <map>
#include <vector>

#include "hdfs.h"
#include "gcache_global.h"
#include "gcache_tmp_storage.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {

#define CACHE_NUM 3 //Macro of Cache level
/**
 * StorageHDFS class is a child of Storage.
 * this class provides some functions of Storage operations in HDFS storage.
 */

class StorageHDFS: public Storage {
public:
    StorageHDFS();
    /**
     * destructor, destruct object.
     */
    virtual ~StorageHDFS();
    /* Singleton Pattern.
     * Initialize the  _storageConfig when GetInstance is first invoked.
     */

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
     * Rename meta file for the given filename
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
    virtual std::map<int, Storage::DiskInfo> *GetAllRootDir();
    /**
     * Get all cache info of the system
     */
    virtual std::vector<Storage::DiskInfo> *GetCacheInfo();
    /**
     * Get  root dir's hdfs link of the given root dir
     */
    virtual hdfsFS getRootFS(std::string path);
    /**
     * Get all root dir.s hdfs link of the system
     */
    virtual std::map<std::string, hdfsFS> *GetAllRootFS();
    //virtual hdfsFS  getHDFS(std::string path);

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

    //char* hdfsFgets(char * buffer,int size,hdfsFS fs,hdfsFile fp);
    //std::string hdfsGetPath(std::string path);
    std::string _metaDirName;  //meta path of the system
    hdfsFS metaFS; //meta hdfs link of the system
    std::vector<DiskInfo> _cacheInfo[CACHE_NUM]; //_cacheInfo of the system

    std::map<int, DiskInfo> _rootDirs; //all root dirs of the system indexed by dirID
    std::map<std::string, hdfsFS> _rootFS; //all root hdfs link of the system indexed by root path
    std::map<std::string, hdfsFS> _metaFS; //all meta hdfs link of the system indexed by meta path
    std::map<int, DiskInfo> _metaDirs; // all meta dirs of the system indexed by dirID
    //static StorageHDFS* _storageHDFS;
};

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_TMP_STORAGE_HDFS_H_ */
