/*
 * 2/6 2016
 * author: buaa_zhang
 *
 */
/*
 * gcache_tmp_dir_nas.h
 *
 *  Created on: Jan 8, 2016
 *      Author: wyd
 */

#ifndef GCACHE_TMP_DIR_CENTRAL_H_
#define GCACHE_TMP_DIR_CENTRAL_H_

#include <sys/types.h>
#include <list>
#include <map>
#include <string>
#include <vector>

#include "gcache_global.h"
#include "gcache_advice.h"
#include "gcache_tmp_dir.h"
#include "gcache_tmp_storage.h"
#include "gcache_tmp_error.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {
/**
 * DirNAS class is a child of Dir.
 * this class provides some functions of Dir operations in NAS storage.
 */
class DirNAS: public Dir {
public:
    /**
     * destructor, destruct objects
     */
    virtual ~DirNAS();
    /*
     Test the existence of the directory
     */
    virtual bool Exist();
    /**
     *Returns the size of the partition named by this pathname
     */
    virtual int64_t GetTotalSpace(CacheLevel level = CACHE_L3);

    /**
     *Returns the unallocated size of the partition named by this pathname
     */
    virtual int64_t GetFreeSpace(CacheLevel level = CACHE_L3);

    /*
     *Creates the directory named by this abstract pathname, include any necessary but nonexistent parent directories.
     */
    virtual bool MakeDirs();

    /**
     * Remove the Directory, according to the param , remove it subdir
     */
    virtual bool Remove(bool recursive = false);
    /**
     * Renames the path name to a new name
     */
    virtual bool Rename(const std::string& new_name);
    /**
     *Move the directory to another directory.
     */
    virtual bool Move(const std::string& dest_dir);
    /**
     *Copy the directory to a new location.
     */
    virtual bool Copy(const std::string& dest_dir);
    /**
     * Return an list of strings naming the files denoting the files in the directory denoted by this path/wildcard
     */
    virtual bool ListFiles(std::list<std::string>& fileList, const std::string& wildcard = "");
    /**
     * Return an list of strings naming the directories denoting the directories in the directory denoted by this path/wildcard
     */
    virtual bool ListDirs(std::list<std::string>& dirList, const std::string& wildcard = "");

    friend class Dir;

private:
    /**
     * constructor
     * set member variables to initial values
     */
    DirNAS(const std::string &path);
    /**
     * Set the member variables to real meaningful values
     */
    virtual bool Init();
    /**
     * Test if the Dir is Empty
     */
    bool IsEmpty();
    /**
     * Move current directory to dest_path
     */
    bool MoveToRealDest(const std::string &real_dest_path);
    /**
     * Copy source directory to destination directory
     */
    bool RecursiveCopy(const std::string &real_src_path, const std::string &real_dest_path);
    /**
     * Test if the directory exists in one of the root dirs
     * */
    bool PartExist();
    std::string _path; //path of the directory objects
    Storage *_storage; // Storage Media info of the System
    std::map<int, Storage::DiskInfo> *_rootDirs; // root Dirs path of the system indexed by dirID
    std::vector<Storage::DiskInfo> *_cacheInfo; // root dirs of the system indexed by cache level

};

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_TMP_DIR_NAS_H_ */
