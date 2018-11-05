#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/types.h>
#include <unistd.h>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <iterator>
#include <map>
#include <string>
#include <utility>
#include <vector>
#include <set>
#include <regex>

#include "hdfs.h"
#include "gcache_tmp_storage_hdfs.h"
#include "gcache_io.h"
#include "gcache_string.h"
#include "gcache_log.h"
#include "gcache_advice.h"
#include "gcache_tmp_error.h"
#include "gcache_tmp_meta.h"
#include "gcache_hdfs_utils.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {
/**
 * constructor
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param
 * @func                set member variable to initial values.
 * @return                no return
 */
StorageHDFS::StorageHDFS() {
    metaFS = NULL;
}
/**
 * destructor
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param
 * @func                destruct object()
 * @return      no return
 */
StorageHDFS::~StorageHDFS() {
    for (std::map<std::string, hdfsFS>::iterator it = _rootFS.begin(); it != _rootFS.end(); ++it) {
        hdfsDisconnect(it->second);
    }
}
/**
 * PutMeta
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                 filename filename of the object file
 *                                  metaData content of the file's meta info
 *                                 size size of metaData
 *                                   meta_type type of meta eg. META_FILE OR META_KVTABLE
 *
 * @func            Write Meta data
 * @return                 return true if write meta info succeed
 *                                false if write failed
 *
 */
bool StorageHDFS::PutMeta(const std::string& filename, const char* metaData, int size,
        MetaType meta_type) {
    std::string real_meta_dir_name;
    switch (meta_type) {
        case META_TMP_FILE:
            real_meta_dir_name = _metaDirName + "/";
            break;
        case META_TMP_KVTABLE:
            real_meta_dir_name = _metaDirName + "/GCacheObject/";
            break;
        default:
            errno = GCACHE_TMP_ERR_NOT_SUPPORT;
            return false;
    }
    hdfsFS fs = metaFS;
    std::string full_path = real_meta_dir_name + filename;

    std::string metaFileName = hdfsGetPath(full_path);
    int dirNamePos = metaFileName.find_last_of('/');
    std::string metaDir = metaFileName.substr(0, dirNamePos);
    int bRet = hdfsCreateDirectory(fs, metaDir.c_str());
    if (bRet != 0) {
        //hdfsDisconnect(fs);
        return false;
    }
    hdfsChmod(fs, metaDir.c_str(), 0755);

#if 0
    //When create File ,hdfs mkdir function can Recursively done the create job
    int dirNamePos = full_path.find_last_of('/');
    std::string metaDir = full_path.substr(0, dirNamePos);

    bool bRet = RecursiveMakeDir(metaDir);

    if (!bRet)
    return false;
#endif
    //O_WRONLY mean write,create or truncate
    hdfsFile fd = hdfsOpenFile(fs, metaFileName.c_str(), O_WRONLY, 0, REPLICATION, 0);
    if (NULL == fd) {
        hdfsCloseFile(fs, fd);
        //hdfsDisconnect(fs);
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_PUTMETA, errno);
        return false;
    }
    hdfsChmod(fs, metaFileName.c_str(), 0644);

    int write_len = sizeof(size);
    if (write_len != hdfsWrite(fs, fd, &size, write_len)) {
        hdfsCloseFile(fs, fd);
        //hdfsDisconnect(fs);
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_PUTMETA, errno);
        return false;
    }
    write_len = size;
    if (write_len != hdfsWrite(fs, fd, metaData, write_len)) {
        hdfsCloseFile(fs, fd);
        //hdfsDisconnect(fs);
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_PUTMETA, errno);
        return false;
    }
    hdfsCloseFile(fs, fd);
    //hdfsDisconnect(fs);
    return true;
}
/**
 * GetMeta
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                 filename filename of the object file
 *                                  metaData content of the file's meta info
 *                                 size size of metaData
 *                                   meta_type type of meta eg. META_FILE OR META_KVTABLE
 *
 * @func            Read Meta data
 * @return                 return true if read meta info succeed
 *                                false if write failed
 *
 */
bool StorageHDFS::GetMeta(const std::string& filename, char*& metaData, int& size,
        MetaType meta_type) {
    std::string real_meta_dir_name;
    switch (meta_type) {
        case META_TMP_FILE:
            real_meta_dir_name = _metaDirName + "/";
            break;
        case META_TMP_KVTABLE:
            real_meta_dir_name = _metaDirName + "/GCacheObject/";
            break;
        default:
            errno = GCACHE_TMP_ERR_NOT_SUPPORT;
            return false;
    }
    hdfsFS fs = metaFS;
    std::string metaFileName = hdfsGetPath(real_meta_dir_name + filename);
    hdfsFile fd = hdfsOpenFile(fs, metaFileName.c_str(), O_RDONLY, 0, REPLICATION, 0);
    if (NULL == fd) {
        //hdfsDisconnect(fs);
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_GETMETA, errno);
        return false;
    }
    int read_len = sizeof(size);
    if (read_len != hdfsRead(fs, fd, &size, read_len)) {
        hdfsCloseFile(fs, fd);
        //hdfsDisconnect(fs);
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_GETMETA, errno);
        return false;
    }
    metaData = (char*) malloc(size);
    read_len = size;
    if (read_len != hdfsRead(fs, fd, metaData, read_len)) {
        hdfsCloseFile(fs, fd);
        //hdfsDisconnect(fs);
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_GETMETA, errno);
        return false;
    }
    hdfsCloseFile(fs, fd);
    //hdfsDisconnect(fs);
    return true;

}
/**
 * DeleteMeta
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                 filename filename of the object file/dir
 *                                 meta_type type of meta eg. META_FILE OR META_KVTABLE
 * @func            Delete Meta data

 * @return                 return true if delete succeed
 *                                 false if an error occurred
 */
bool StorageHDFS::DeleteMeta(const std::string& filename, MetaType meta_type) {
    std::string real_meta_dir_name;
    switch (meta_type) {
        case META_TMP_FILE:
            real_meta_dir_name = _metaDirName + "/";
            break;
        case META_TMP_KVTABLE:
            real_meta_dir_name = _metaDirName + "/GCacheObject/";
            break;
        default:
            errno = GCACHE_TMP_ERR_NOT_SUPPORT;
            return false;
    }
    hdfsFS fs = metaFS;
    std::string metaFileName = hdfsGetPath(real_meta_dir_name + filename);
    int acc_ret = hdfsExists(fs, metaFileName.c_str());
    if (acc_ret == -1) {
        //hdfsDisconnect(fs);
        return true;
    }
    hdfsFileInfo *hf_info = hdfsGetPathInfo(fs, metaFileName.c_str());
    if ( NULL == hf_info) {
        hdfsFreeFileInfo(hf_info, 1);
        //hdfsDisconnect(fs);
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
        return false;
    }
    if (hf_info->mKind == 'D') {

        int rm_ret = hdfsDelete(fs, metaFileName.c_str(), 1);
        if (rm_ret != 0) {
            hdfsFreeFileInfo(hf_info, 1);
            //hdfsDisconnect(fs);
            errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_DELMETA, errno);
            return false;
        }
#if 0
        // Modify RecursiveRemove( The lib have such a func)
        bool rm_ret = RecursiveRemove(metaFileName);
        if(!rm_ret) {
            hdfsFreeFileInfo(hf_info,1);
            //hdfsDisconnect(fs);
            errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_DELMETA, errno);
            return false;
        }
#endif
    } else if (hf_info->mKind == 'F') {
        int unlink_ret = hdfsDelete(fs, metaFileName.c_str(), 0);
        if (unlink_ret == -1) {
            hdfsFreeFileInfo(hf_info, 1);
            //hdfsDisconnect(fs);
            errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_DELMETA, errno);
            return false;
        }
    }
    hdfsFreeFileInfo(hf_info, 1);
    //hdfsDisconnect(fs);
    return true;

}
/**
 * RenameMeta
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                old_name filename of the object file/dir
 *                                 new_name new file name of the object file/dir
 *                                 meta_type type of meta eg. META_FILE OR META_KVTABLE
 * @func            Rename a given file path's meta file a new name

 * @return                 return true if Rename meta file succeed
 *                                 false if an error occurred
 *
 */
bool StorageHDFS::RenameMeta(const std::string& old_name, const std::string &new_name,
        MetaType meta_type) {
    std::string real_meta_dir_name;
    std::string s_new_name = new_name;
    switch (meta_type) {
        case META_TMP_FILE:
            real_meta_dir_name = _metaDirName + "/";
            break;
        case META_TMP_KVTABLE:
            real_meta_dir_name = _metaDirName + "/GCacheObject/";
            break;
        default:
            errno = GCACHE_TMP_ERR_NOT_SUPPORT;
            return false;
    }
    int dirNewNamePos = new_name.find_last_not_of('/');

    s_new_name = new_name.substr(0, dirNewNamePos + 1);
    hdfsFS fs = metaFS;
    std::string oldFullPath = real_meta_dir_name + old_name;
    std::string newFullPath = real_meta_dir_name + s_new_name;
    std::string oldMetaFilename = hdfsGetPath(oldFullPath);
    std::string newMetaFilename = hdfsGetPath(newFullPath);

    /*
     * auto make meta dir
     */
    int dirNamePos = newMetaFilename.find_last_of('/');
    std::string metaDir = newMetaFilename.substr(0, dirNamePos);
    // RecursiveMakeDir params hdfs://ip:port/path
    int bRet = hdfsCreateDirectory(fs, metaDir.c_str());

    if (bRet != 0) {
        //hdfsDisconnect(fs);
        return false;
    }
    hdfsChmod(fs, metaDir.c_str(), 0755);

    int iRet = hdfsRename(fs, oldMetaFilename.c_str(), newMetaFilename.c_str());
    if (iRet == -1) {
        //hdfsDisconnect(fs);
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RENMETA, errno);
        return false;
    }
    //hdfsDisconnect(fs);
    return true;
}
/**
 * MakeMetaDir
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                dir_name name of the dir
 * @func            Make meta dir for a given dir path

 * @return                 return true if make the dir
 *                                 false if an error occurred
 *
 */
bool StorageHDFS::MakeMetaDir(const std::string& dir_name) {
    std::string fullMetaFileName = _metaDirName + "/" + dir_name;
    // Modify RecursiveMakeDir For HDFS(hdfs://ip:port/path)
    hdfsFS fs = metaFS;
    std::string metaFileName = hdfsGetPath(fullMetaFileName);
    //int dirNamePos = metaFileName.find_last_of('/');
    //std::string metaDir = metaFileName.substr(0, dirNamePos);
    int bRet = hdfsCreateDirectory(fs, metaFileName.c_str());

    if (bRet != 0) {
        //hdfsDisconnect(fs);
        return false;
    }
    hdfsChmod(fs, metaFileName.c_str(), 0755);

#if 0
    bool mRet = RecursiveMakeDir(metaFileName);
    if (!mRet)
    return false;
#endif
    //hdfsDisconnect(fs);
    return true;

}
/**
 * LastAccessed
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                filename filename of the object file/dir
 *                                meta_type type of meta eg. META_FILE OR META_KVTABLE
 *
 * @func            get the last access time of the file

 * @return                 return  time when last access (s)
 */
uint64_t StorageHDFS::LastAccessed(const std::string &filename, MetaType meta_type) {
    std::string real_meta_dir_name;
    switch (meta_type) {
        case META_TMP_FILE:
            real_meta_dir_name = _metaDirName + "/";
            break;
        case META_TMP_KVTABLE:
            real_meta_dir_name = _metaDirName + "/GCacheObject/";
            break;
        default:
            errno = GCACHE_TMP_ERR_NOT_SUPPORT;
            return false;
    }

    hdfsFS fs = metaFS;
    std::string metaFileName = hdfsGetPath(real_meta_dir_name + filename);
    hdfsFileInfo * hf_info = hdfsGetPathInfo(fs, metaFileName.c_str());
    //hdfsDisconnect(fs);
    if (NULL == hf_info) {
        hdfsFreeFileInfo(hf_info, 1);
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
        return 0;
    }
    uint64_t time_s = hf_info->mLastAccess;
    hdfsFreeFileInfo(hf_info, 1);
    return time_s;
}
/**
 * GetRootDir
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                dirID dirID of the root
 * @func            Get root dir path of the given dirID
 * @return                 return the path of the given root dirID
 *                                 if no such dirID ,return  ""
 */
std::string StorageHDFS::GetRootDir(int dirID) {
    std::map<int, DiskInfo>::iterator it = _rootDirs.find(dirID);
    if (it == _rootDirs.end()) {
        errno = GCACHE_TMP_ERR_NOT_SUPPORT;
        return "";
    }
    return (it->second).rootDir;
}
/**
 * getRootFS
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                path of the root dir
 * @func            Get root dir's hdfs link
 * @return                 return the hdfs link  of the given root path
 *                                 if no such path ,return  NULL;
 */

hdfsFS StorageHDFS::getRootFS(std::string path) {
    std::map<std::string, hdfsFS>::iterator it = _rootFS.find(path);
    if (it == _rootFS.end()) {
        errno = GCACHE_TMP_ERR_NOT_SUPPORT;
        return NULL;
    }
    return it->second;
}
/**
 * GetRootTotalSpace
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                dirID dirID of the root
 * @func            Get the given dirID's root dir's total space
 * @return                 return the total space of the dirID
 */
int64_t StorageHDFS::GetRootTotalSpace(int dirID) {
    std::map<int, DiskInfo>::iterator it = _rootDirs.find(dirID);
    if (it == _rootDirs.end()) {
        errno = GCACHE_TMP_ERR_NOT_SUPPORT;
        return -1;
    }
    int64_t all_space = (int64_t) it->second.quota * 1024 * 1024 * 1024;
    if (all_space <= 0) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STORAGE, errno);
        return -1;
    }
    return all_space;

}
/**
 * GetRootFreeSpace
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                dirID dirID of the root
 * @func            Get the given dirID's root dir's free space
 * @return                 return the free space of the dirID
 */
int64_t StorageHDFS::GetRootFreeSpace(int dirID) {
    std::map<int, DiskInfo>::iterator it = _rootDirs.find(dirID);
    if (it == _rootDirs.end()) {
        errno = GCACHE_TMP_ERR_NOT_SUPPORT;
        return -1;
    }
    hdfsFS fs = getRootFS((it->second).rootDir);

    int64_t all_space = (int64_t) it->second.quota * 1024 * 1024 * 1024;
    if (all_space < 0) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STORAGE, errno);
        return -1;
    }
    if (it->second.used_space <= 0) {
        it->second.used_space = 0;
        std::vector<std::string> dir_vector;
        dir_vector.push_back(hdfsGetPath(it->second.rootDir));
        while (!dir_vector.empty()) {
            int count = 0;
            hdfsFileInfo * listFileInfo = hdfsListDirectory(fs,
                    dir_vector[dir_vector.size() - 1].c_str(), &count);
            dir_vector.pop_back();
            if (listFileInfo == NULL) {
                return -1;
            }

            for (int i = 0; i < count; ++i) {
                if (listFileInfo[i].mKind == kObjectKindDirectory) {
                    dir_vector.push_back(listFileInfo[i].mName);
                    continue;
                }
                it->second.used_space += listFileInfo[i].mSize;
            }
            hdfsFreeFileInfo(listFileInfo, count);

        }
    }
    return all_space - it->second.used_space;

}
/**
 * ElectRootDir
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                cache_lv Level of the cache
 * @func            Select a root dir in a series root path whose cache level are same
 * @return                 return dirID of the voted root dir
 */
int StorageHDFS::ElectRootDir(CacheLevel cache_lv) {
    int ret_dir_id = -1;

    int cache_lv_int = cache_lv;
    if (cache_lv_int < 0 || cache_lv_int > CACHE_NUM) {
        errno = GCACHE_TMP_ERR_NOT_SUPPORT;
        return -1;
    }
    if (_cacheInfo[cache_lv].empty()) {
        errno = GCACHE_TMP_ERR_STORAGE;
        return -1;
    }
    int64_t max_space = -1;
    for (std::vector<DiskInfo>::iterator it = _cacheInfo[cache_lv].begin();
            it != _cacheInfo[cache_lv].end(); ++it) {
        int dirID = it->dirID;
        int64_t free_space = GetRootFreeSpace(dirID);
        if (max_space < free_space) {
            ret_dir_id = dirID;
            max_space = free_space;
        }
    }
    return ret_dir_id;

}
/**
 * GetRealCacheLevel
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                cache_lv Level of the cache
 * @func            Get real cache level of a given cache level
 * @return                 return real cache level
 */
CacheLevel StorageHDFS::GetRealCacheLevel(CacheLevel cache_lv) {
    int cache_lv_int = cache_lv;
    if (!_cacheInfo[cache_lv_int].empty()) {
        return CacheLevel(cache_lv_int);
    } else {
        if (cache_lv_int != CACHE_NUM - 1) {
            int real_cache_lv_int = cache_lv_int + 1;
            while (real_cache_lv_int < CACHE_NUM) {
                if (!_cacheInfo[real_cache_lv_int].empty()) break;
                real_cache_lv_int++;
            }
            if (real_cache_lv_int >= CACHE_NUM) return CacheLevel(cache_lv_int);
            else return CacheLevel(real_cache_lv_int);
        } else {
            int real_cache_lv_int = cache_lv_int - 1;
            while (real_cache_lv_int >= 0) {
                if (!_cacheInfo[real_cache_lv_int].empty()) break;
                real_cache_lv_int--;
            }
            if (real_cache_lv_int < 0) return CacheLevel(cache_lv_int);
            else return CacheLevel(real_cache_lv_int);
        }
    }
}
/**
 * GetCacheLevel
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                dirID dirID of a root
 * @func            Get cache level of a given root dir's dirID
 * @return                 return cache level of the given root path
 */
CacheLevel StorageHDFS::GetCacheLevel(int dirID) {
    std::map<int, DiskInfo>::iterator it = _rootDirs.find(dirID);
    if (it == _rootDirs.end()) {
        errno = GCACHE_TMP_ERR_STORAGE;
        return CACHE_L1;
    }
    return (it->second).cache_lv;
}
/**
 * GetAllRootDir
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param
 * @func            Get all root dirs of the system
 * @return                 return root dir's info indexed by dirID
 *
 */
std::map<int, Storage::DiskInfo> *StorageHDFS::GetAllRootDir() {
    return &_rootDirs;
}
/**
 * GetAllRootFS
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param
 * @func            Get all root hdfs link of the system
 * @return                 return root dir's hdfs link indexed by root path
 *
 */
std::map<std::string, hdfsFS> *StorageHDFS::GetAllRootFS() {
    return &_rootFS;
}
/**
 * GetCacheInfo
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param
 * @func            Get all cacheinfo of the system
 * @return                 return all cache info
 *
 */

std::vector<Storage::DiskInfo> *StorageHDFS::GetCacheInfo() {
    return _cacheInfo;
}
/**
 * Init
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param
 * @func            Do some preprocess,Init member variables
 * @return                 return True if init succeed
 *                                 if an error occurs,return False
 *
 */
bool StorageHDFS::Init() {

    if (!ParseParam()) return false;
#if 0
    for (int i = CACHE_NUM - 2; i >= 0; --i) {
        if (_cacheInfo[i].empty()) {
            _cacheInfo[i] = _cacheInfo[i + 1];
        }
    }

    for (int i = 1; i < CACHE_NUM; ++i) {
        if (_cacheInfo[i].empty()) {
            _cacheInfo[i] = _cacheInfo[i - 1];
        }
    }
#endif
    _metaDirName = ((_metaDirs.begin())->second).rootDir;        //(meta backup)
    metaFS = (_metaFS.begin())->second;
    /*
     * build data root dir
     */
    for (std::map<int, DiskInfo>::iterator it = _rootDirs.begin(); it != _rootDirs.end(); ++it) {
        std::string root_dir = it->second.rootDir;
        hdfsFS fs = getHDFS(root_dir);
        std::string root_dir_path = hdfsGetPath(root_dir);
        hdfsFileInfo *hf_info = hdfsGetPathInfo(fs, root_dir_path.c_str());
        int aRet = hdfsExists(fs, root_dir_path.c_str());
        if (0 != aRet) {
            bool mRet = hdfsCreateDirectory(fs, root_dir_path.c_str());
            if (mRet != 0) {
                hdfsFreeFileInfo(hf_info, 1);
                //hdfsDisconnect(fs);
                errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_MKDIR, errno);
                return false;
            }
        } else {
            hdfsChmod(fs, root_dir_path.c_str(), 0755);

            aRet = hf_info->mPermissions;
            int flag = aRet >> 6;
            //int a = flag & W_OK;
            if ((flag & W_OK) != W_OK) {
                hdfsFreeFileInfo(hf_info, 1);
                //hdfsDisconnect(fs);
                return false;
            }
        }
        hdfsFreeFileInfo(hf_info, 1);
        //hdfsDisconnect(fs);
    }

    /*
     * build meta root dir
     */
    for (std::map<int, DiskInfo>::iterator it = _metaDirs.begin(); it != _metaDirs.end(); ++it) {
        std::string meta_root_dir = it->second.rootDir;
        hdfsFS fs = getHDFS(meta_root_dir);
        std::string meta_root_dir_path = hdfsGetPath(meta_root_dir);
        hdfsFileInfo *hf_info = hdfsGetPathInfo(fs, meta_root_dir_path.c_str());
        int aRet = hdfsExists(fs, meta_root_dir_path.c_str());
        if (0 != aRet) {
            int mRet = hdfsCreateDirectory(fs, meta_root_dir_path.c_str());
            //bool mRet = RecursiveMakeDir(meta_root_dir);
            if (mRet != 0) {
                hdfsFreeFileInfo(hf_info, 1);
                //hdfsDisconnect(fs);
                errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_MKDIR, errno);
                return false;
            }
            hdfsChmod(fs, meta_root_dir_path.c_str(), 0755);

        } else {
            aRet = hf_info->mPermissions;
            int flag = aRet >> 6;
            if ((flag & W_OK) != W_OK) {
                //hdfsDisconnect(fs);
                hdfsFreeFileInfo(hf_info, 1);
                return false;
            }
        }
        hdfsFreeFileInfo(hf_info, 1);
        //hdfsDisconnect(fs);
    }

    return true;
}
/**
 * ParseParam
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param
 * @func            Parse Config Files and do lots of process

 * @return                 return true if parse meta file succeed
 *                                 false if an error occurred
 * Option:
 *                 Config File Address: Get From a SysPath $GCACHE_TMP_HDFS_CONF
 *
 */
bool StorageHDFS::ParseParam() {
//        std::string para_filename = "/u0/data/etc/storage_dist.conf";
    std::string user = getenv("USER");
    char *confile = getenv("GCACHE_TMP_HDFS_CONF");
    if (confile == NULL) {
        Err("environment path GCACHE_TMP_HDFS_CONF doesn't exist\n");
        errno = GCACHE_TMP_ERR_CONF;
        return false;
    }
    std::string para_filename = confile;

    //std::string para_filename = getenv("GCACHE_TMP_HDFS_CONF");
//    Err("para_filename = %s\n", para_filename.c_str());
#if 0
    FILE *fp = fopen(para_filename.c_str(), "r");
    if(NULL == fp) {
        errno = GCACHE_TMP_ERR_STORAGE;
        return false;
    }
#endif

    if (para_filename.find("hdfs://") != std::string::npos)  //configure file in hdfs
            {
        //Get The Pure file Path
        std::string path = hdfsGetPath(para_filename);
        if (!StartsWith(path, "/")) {
            path = "/" + path;
        }
        hdfsFS fs = getHDFS(para_filename);
        if (fs == NULL) {
            errno = GCACHE_TMP_ERR_CONF;
            return false;
        }
        hdfsFile fp = hdfsOpenFile(fs, path.c_str(), O_RDONLY, 0, REPLICATION, 0);
        if (NULL == fp) {
            errno = GCACHE_TMP_ERR_CONF;
            hdfsDisconnect(fs);
            return false;
        }
        char buf[1024];
        std::set<int> dirID_set;

        bool first_round = true;
        bool no_cache_lv = false;

        while (hdfsFgets(buf, 1024, fs, fp) != NULL) {
            int len = strlen(buf);
            if (buf[len - 1] == '\n') buf[len - 1] = '\0';

            std::vector<std::string> params = SplitString(buf);

            if (params.size() == 0) {
                break;
            }

            /*
             * do not use repeated dirID
             */
            int dirID = params[0][0];
            if (dirID_set.count(dirID) == 0) {
                dirID_set.insert(dirID);
            } else {
                errno = GCACHE_TMP_ERR_REPDIR;

                hdfsCloseFile(fs, fp);
                hdfsDisconnect(fs);
                return false;
            }

            DiskInfo disk_info;
            disk_info.dirID = dirID;
            disk_info.used_space = -1;
            disk_info.rootDir = params[1] + "/" + user;

            //Link hdfs Server
            hdfsFS _fs = getHDFS(disk_info.rootDir);

            std::string entry_type = params[2];
            if (entry_type == "meta") {
                _metaFS.insert(std::make_pair(disk_info.rootDir, _fs));
                _metaDirs.insert(std::make_pair(disk_info.dirID, disk_info));
            } else if (entry_type == "data") {

                if (params.size() <= 3) { // no cache_lv and quota
                    if (first_round) {
                        no_cache_lv = true;
                        disk_info.cache_lv = CACHE_L1;
                        disk_info.quota = -1;
                    } else {
                        if (no_cache_lv) {
                            disk_info.cache_lv = CACHE_L1;
                            disk_info.quota = -1;
                        } else {
                            errno = GCACHE_TMP_ERR_STORAGE;
                            hdfsCloseFile(fs, fp);
                            hdfsDisconnect(fs);
                            return false;
                        }
                    }

                } else if (params.size() == 4) { // set cache_lv or quota
                    if (StartsWith(params[3], "cache_l")) { // set cache_lv
                        if (first_round) {
                            no_cache_lv = false;
                            if (params[3] == "cache_l1") disk_info.cache_lv = CACHE_L1;
                            else if (params[3] == "cache_l2") disk_info.cache_lv = CACHE_L2;
                            else if (params[3] == "cache_l3") disk_info.cache_lv = CACHE_L3;
                            else {
                                errno = GCACHE_TMP_ERR_NOT_SUPPORT;
                                hdfsCloseFile(fs, fp);
                                hdfsDisconnect(fs);
                                return false;
                            }
                            disk_info.quota = -1;
                        } else {
                            if (no_cache_lv) {
                                errno = GCACHE_TMP_ERR_STORAGE;
                                hdfsCloseFile(fs, fp);
                                hdfsDisconnect(fs);
                                return false;
                            } else {
                                if (params[3] == "cache_l1") disk_info.cache_lv = CACHE_L1;
                                else if (params[3] == "cache_l2") disk_info.cache_lv = CACHE_L2;
                                else if (params[3] == "cache_l3") disk_info.cache_lv = CACHE_L3;
                                else {
                                    errno = GCACHE_TMP_ERR_NOT_SUPPORT;
                                    hdfsCloseFile(fs, fp);
                                    hdfsDisconnect(fs);
                                    return false;
                                }

                                disk_info.quota = -1;
                            }
                        }
                    } else { // set quota
                        if (first_round) {
                            no_cache_lv = true;
                            disk_info.cache_lv = CACHE_L1;
                            disk_info.quota = ToInt(params[3]);
                        } else {
                            if (no_cache_lv) {
                                disk_info.cache_lv = CACHE_L1;
                                disk_info.quota = ToInt(params[3]);
                            } else {
                                errno = GCACHE_TMP_ERR_STORAGE;
                                hdfsCloseFile(fs, fp);
                                hdfsDisconnect(fs);
                                return false;
                            }
                        }
                    }
                } else { // set all params
                    if (first_round) {
                        no_cache_lv = false;
                        if (params[3] == "cache_l1") disk_info.cache_lv = CACHE_L1;
                        else if (params[3] == "cache_l2") disk_info.cache_lv = CACHE_L2;
                        else if (params[3] == "cache_l3") disk_info.cache_lv = CACHE_L3;
                        else {
                            errno = GCACHE_TMP_ERR_NOT_SUPPORT;
                            hdfsCloseFile(fs, fp);
                            hdfsDisconnect(fs);
                            return false;
                        }

                        disk_info.quota = ToInt(params[4]);
                    } else {
                        if (no_cache_lv) {
                            errno = GCACHE_TMP_ERR_STORAGE;
                            hdfsCloseFile(fs, fp);
                            hdfsDisconnect(fs);
                            return false;
                        } else {
                            if (params[3] == "cache_l1") disk_info.cache_lv = CACHE_L1;
                            else if (params[3] == "cache_l2") disk_info.cache_lv = CACHE_L2;
                            else if (params[3] == "cache_l3") disk_info.cache_lv = CACHE_L3;
                            else {
                                errno = GCACHE_TMP_ERR_NOT_SUPPORT;
                                hdfsCloseFile(fs, fp);
                                hdfsDisconnect(fs);
                                return false;
                            }
                            disk_info.quota = ToInt(params[4]);
                        }
                    }
                }
                if (disk_info.quota <= 0) {
                    disk_info.quota = (int64_t) hdfsGetCapacity(fs) / 1024 / 1024 / 1024;
                }
                _cacheInfo[disk_info.cache_lv].push_back(disk_info);
                _rootDirs.insert(std::make_pair(disk_info.dirID, disk_info));
                _rootFS.insert(std::make_pair(disk_info.rootDir, _fs));
                first_round = false;
            } else {
                errno = GCACHE_TMP_ERR_STORAGE;
                hdfsCloseFile(fs, fp);
                hdfsDisconnect(fs);
                return false;
            }
        }
        hdfsCloseFile(fs, fp);
        hdfsDisconnect(fs);
    } else   //configure file in Nas
    {

        FILE *fp = fopen(para_filename.c_str(), "r");
        if (NULL == fp) {
            errno = GCACHE_TMP_ERR_OPFILE;
            return false;
        }
        char buf[1024];
        std::set<int> dirID_set;

        bool first_round = true;
        bool no_cache_lv = false;

        while (fgets(buf, 1024, fp) != NULL) {
            int len = strlen(buf);
            if (buf[len - 1] == '\n') buf[len - 1] = '\0';

            std::vector<std::string> params = SplitString(buf);

            /*
             * do not use repeated dirID
             */
            int dirID = params[0][0];
            if (dirID_set.count(dirID) == 0) {
                dirID_set.insert(dirID);
            } else {
                errno = GCACHE_TMP_ERR_REPDIR;
                fclose(fp);
                return false;
            }

            DiskInfo disk_info;
            disk_info.dirID = dirID;

            disk_info.rootDir = params[1] + "/" + user;

            //Link hdfs Server
            hdfsFS _fs = getHDFS(disk_info.rootDir);

            std::string entry_type = params[2];
            if (entry_type == "meta") {
                _metaFS.insert(std::make_pair(disk_info.rootDir, _fs));
                _metaDirs.insert(std::make_pair(disk_info.dirID, disk_info));
            } else if (entry_type == "data") {

                if (params.size() <= 3) { // no cache_lv and quota
                    if (first_round) {
                        no_cache_lv = true;
                        disk_info.cache_lv = CACHE_L1;
                        disk_info.quota = -1;
                    } else {
                        if (no_cache_lv) {
                            disk_info.cache_lv = CACHE_L1;
                            disk_info.quota = -1;
                        } else {
                            errno = GCACHE_TMP_ERR_STORAGE;
                            fclose(fp);
                            return false;
                        }
                    }

                } else if (params.size() == 4) { // set cache_lv or quota
                    if (StartsWith(params[3], "cache_l")) { // set cache_lv
                        if (first_round) {
                            no_cache_lv = false;
                            if (params[3] == "cache_l1") disk_info.cache_lv = CACHE_L1;
                            else if (params[3] == "cache_l2") disk_info.cache_lv = CACHE_L2;
                            else if (params[3] == "cache_l3") disk_info.cache_lv = CACHE_L3;
                            else {
                                errno = GCACHE_TMP_ERR_NOT_SUPPORT;
                                fclose(fp);
                                return false;
                            }
                            disk_info.quota = -1;
                        } else {
                            if (no_cache_lv) {
                                errno = GCACHE_TMP_ERR_STORAGE;
                                fclose(fp);
                                return false;
                            } else {
                                if (params[3] == "cache_l1") disk_info.cache_lv = CACHE_L1;
                                else if (params[3] == "cache_l2") disk_info.cache_lv = CACHE_L2;
                                else if (params[3] == "cache_l3") disk_info.cache_lv = CACHE_L3;
                                else {
                                    errno = GCACHE_TMP_ERR_NOT_SUPPORT;
                                    fclose(fp);
                                    return false;
                                }

                                disk_info.quota = -1;
                            }
                        }
                    } else { // set quota
                        if (first_round) {
                            no_cache_lv = true;
                            disk_info.cache_lv = CACHE_L1;
                            disk_info.quota = ToInt(params[3]);
                        } else {
                            if (no_cache_lv) {
                                disk_info.cache_lv = CACHE_L1;
                                disk_info.quota = ToInt(params[3]);
                            } else {
                                errno = GCACHE_TMP_ERR_STORAGE;
                                fclose(fp);
                                return false;
                            }
                        }
                    }
                } else { // set all params
                    if (first_round) {
                        no_cache_lv = false;
                        if (params[3] == "cache_l1") disk_info.cache_lv = CACHE_L1;
                        else if (params[3] == "cache_l2") disk_info.cache_lv = CACHE_L2;
                        else if (params[3] == "cache_l3") disk_info.cache_lv = CACHE_L3;
                        else {
                            errno = GCACHE_TMP_ERR_NOT_SUPPORT;
                            fclose(fp);
                            return false;
                        }

                        disk_info.quota = ToInt(params[4]);
                    } else {
                        if (no_cache_lv) {
                            errno = GCACHE_TMP_ERR_STORAGE;
                            fclose(fp);
                            return false;
                        } else {
                            if (params[3] == "cache_l1") disk_info.cache_lv = CACHE_L1;
                            else if (params[3] == "cache_l2") disk_info.cache_lv = CACHE_L2;
                            else if (params[3] == "cache_l3") disk_info.cache_lv = CACHE_L3;
                            else {
                                errno = GCACHE_TMP_ERR_NOT_SUPPORT;
                                fclose(fp);
                                return false;
                            }
                            disk_info.quota = ToInt(params[4]);
                        }
                    }
                }

                _cacheInfo[disk_info.cache_lv].push_back(disk_info);
                _rootDirs.insert(std::make_pair(disk_info.dirID, disk_info));
                _rootFS.insert(std::make_pair(disk_info.rootDir, _fs));
                first_round = false;
            } else {
                errno = GCACHE_TMP_ERR_STORAGE;
                fclose(fp);
                return false;
            }
        }

    }

    return true;
}
#if 0
//Return a FS for a given path
hdfsFS StorageHDFS::getHDFS(std::string path) {
    std::string uri = path;
    std::string new_uri = "";
    struct hdfsBuilder *builder = hdfsNewBuilder();
    new_uri = Replace(uri, '/', ':');
    std::vector<std::string> uri_split = SplitString(new_uri, ":");
    std::string protocol = uri_split[0];
    std::string address = uri_split[1];
    std::string port = uri_split[2];
    std::string dir_name = uri_split[3];
    hdfsBuilderSetNameNode(builder, address.c_str());
    hdfsBuilderSetNameNodePort(builder, ToInt64(port));
    hdfsFS fs = hdfsBuilderConnect(builder);

    return fs;

}
char* StorageHDFS::hdfsFgets(char* buffer, int size, hdfsFS fs, hdfsFile file) {
    register int c;
    register char *cs;
    cs = buffer;
    register char *read;
    hdfsRead(fs, file, read, 1);
    c = read[0];
    while (--size > 0 && c != EOF) {
        if ((*cs++ = c) == '\n')
        break;
        hdfsRead(fs, file, read, 1);
        c = read[0];

    }
    *cs = '\0';
    return (c == EOF && cs == buffer) ? NULL : buffer;

}

std::string StorageHDFS::hdfsGetPath(std::string path) {
    std::string para_filename = path;
    int pos = para_filename.find("://");
    std::string new_para_filename = para_filename.substr(pos + 3);
    int path_pos = new_para_filename.find("/");
    std::string path_ret = new_para_filename.substr(path_pos);
    return path_ret;

}
#endif

NAMESPACE_END_SEISFS
}
