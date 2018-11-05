/*
 * gcache_tmp_dir_local.h
 *
 *  Created on: Jan 8, 2016
 *      Author: wyd
 */

#ifndef GCACHE_TMP_DIR_LOCAL_H_
#define GCACHE_TMP_DIR_LOCAL_H_

#include <sys/types.h>
#include <list>
#include <map>
#include <string>
#include <vector>

#include "gcache_global.h"
#include "gcache_advice.h"
#include "gcache_tmp_dir.h"
#include "gcache_tmp_storage.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {
class DirLocal: public Dir {
public:
    virtual ~DirLocal();

    /*
     判断目录是否存在
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
     创建目录的方法，调用之后会在所有的存储介质上建立相同的目录
     由于我们在所有的存储介质上都有相同的目录结构，因此没有必要设置cache层级和过期时间的属性。
     如果目录为空的时候，清理进程会自动删除目录。
     Creates the directory named by this abstract pathname, include any necessary but nonexistent parent directories.
     */
    virtual bool MakeDirs();

    /*
     删除目录的方法，调用之后会删除所有存储介质上的目录
     如果recursive为true，则采用递归删除，删除目录下的所有文件。如果为false，则只有目录为空的时候才能删除
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
    DirLocal(const std::string &path);

    virtual bool Init();

    bool IsEmpty();

    bool MoveToRealDest(const std::string &real_dest_path);

    bool RecursiveCopy(const std::string &real_src_path, const std::string &real_dest_path);

    bool PartExist();

    std::string _path;
    Storage *_storage;
    std::map<int, Storage::DiskInfo> *_rootDirs;
    std::vector<Storage::DiskInfo> *_cacheInfo;
};

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_TMP_DIR_LOCAL_H_ */
