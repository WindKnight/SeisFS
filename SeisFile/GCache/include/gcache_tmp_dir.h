/*
 * gcache_tmp_dir.h
 *
 *  Created on: Jan 7, 2016
 *      Author: wyd
 */

#ifndef GCACHE_TMP_DIR_H_
#define GCACHE_TMP_DIR_H_

#include <sys/types.h>
#include <list>
#include <string>

#include "gcache_global.h"
#include "gcache_advice.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {

/*
 Dir是负责目录操作的类型，定义了操作目录的一些方法，包括MakeDir， DeleteDir和判断目录是否存在的Exist。
 Dir是一个抽象的基类，需要为本地盘，集中存储，分布式存储分别定义子类，实现相应的方法。
 由于用户逻辑视角下的每个目录都可能在不同的cache，如果分别在不同的存储介质下建立不同的目录结构，目录管理会相对比较复杂
 为了简化目录管理的操作，我们在实现中，在所有的存储介质上建立与逻辑视角相同的目录结构，在建立目录和删除目录时都在所有的存储介质上进行同样操作。
 */

class Dir {
public:

    virtual ~Dir() {
    }

    /*
     创建目录对象，根据s_arch区分目录是建在本地存储，集中存储还是分布式存储上。
     调用New方法会实例化相应的子类的对象，并返回指针，用户通过该指针进行目录的操作。
     */
    static Dir *New(const std::string &path, StorageArch &s_arch);

    /*
     判断目录是否存在
     */
    virtual bool Exist() = 0;

    /**
     *Returns the size of the partition named by this pathname
     */
    virtual int64_t GetTotalSpace(CacheLevel level = CACHE_L3) = 0;

    /**
     *Returns the unallocated size of the partition named by this pathname
     */
    virtual int64_t GetFreeSpace(CacheLevel level = CACHE_L3) = 0;

    /*
     创建目录的方法，调用之后会在所有的存储介质上建立相同的目录
     由于我们在所有的存储介质上都有相同的目录结构，因此没有必要设置cache层级和过期时间的属性。
     如果目录为空的时候，清理进程会自动删除目录。
     Creates the directory named by this abstract pathname, include any necessary but nonexistent parent directories.
     */
    virtual bool MakeDirs() = 0;

    /*
     删除目录的方法，调用之后会删除所有存储介质上的目录
     如果recursive为true，则采用递归删除，删除目录下的所有文件。如果为false，则只有目录为空的时候才能删除
     */
    virtual bool Remove(bool recursive = false) = 0;

    /**
     * Renames the path name to a new name
     */
    virtual bool Rename(const std::string& new_name) = 0;

    /**
     *Move the directory to another directory.
     */
    virtual bool Move(const std::string& dest_dir) = 0;

    /**
     *Copy the directory to a new location.
     */
    virtual bool Copy(const std::string& dest_dir) = 0;
    /**
     * Return an list of strings naming the files denoting the files in the directory denoted by this path/wildcard
     */
    virtual bool ListFiles(std::list<std::string>& fileList, const std::string& wildcard = "") = 0;

    /**
     * Return an list of strings naming the directories denoting the directories in the directory denoted by this path/wildcard
     */
    virtual bool ListDirs(std::list<std::string>& dirList, const std::string& wildcard = "") = 0;

protected:

    Dir() {
    }
    ;

private:

    virtual bool Init() = 0;

};

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_TMP_DIR_H_ */
