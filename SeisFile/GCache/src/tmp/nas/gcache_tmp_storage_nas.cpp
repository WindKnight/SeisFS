/*
 * 2/6 2016
 * authour Buaa_zhang
 */

#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <unistd.h>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <iterator>
#include <set>
#include <utility>
#include <dirent.h>

#include "gcache_tmp_storage_nas.h"
#include "gcache_io.h"
#include "gcache_string.h"
#include "gcache_tmp_error.h"
#include "gcache_log.h"

//extern int GCACHE_errno;
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
StorageNAS::StorageNAS() {

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
StorageNAS::~StorageNAS() {

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
bool StorageNAS::PutMeta(const std::string& filename, const char* metaData, int size,
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
            Err("the meta_type is wrong, please input the right value\n");
            GCACHE_tmp_errno = GCACHE_TMP_ERR_NOT_SUPPORT;
            return false;
    }
    std::string metaFileName = real_meta_dir_name + filename;
    int dirNamePos = metaFileName.find_last_of('/');
    std::string metaDir = metaFileName.substr(0, dirNamePos);

    bool bRet = RecursiveMakeDir(metaDir);
    if (!bRet) return false;

    int fd = open(metaFileName.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (0 > fd) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_OPFILE, errno);
        return false;
    }
    int write_len = sizeof(size);
    if (write_len != write(fd, &size, write_len)) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_WRITE, errno);
        return false;
    }
    write_len = size;
    if (write_len != write(fd, metaData, write_len)) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_WRITE, errno);
        return false;
    }
    close(fd);
    return true;
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
 * @func            Read Meta data
 * @return                 return true if read meta info succeed
 *                                false if read failed
 *
 */

bool StorageNAS::GetMeta(const std::string& filename, char*& metaData, int& size,
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
            Err("the meta_type is wrong, please input the right value\n");
            GCACHE_tmp_errno = GCACHE_TMP_ERR_NOT_SUPPORT;
            return false;
    }
    std::string metaFileName = real_meta_dir_name + filename;
    int fd = open(metaFileName.c_str(), O_RDONLY);
    if (-1 == fd) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_OPFILE, errno);
        return false;
    }
    int read_len = sizeof(size);
    if (read_len != read(fd, &size, read_len)) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_READ, errno);
        return false;
    }
    metaData = (char*) malloc(size);
    read_len = size;
    if (read_len != read(fd, metaData, read_len)) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_READ, errno);
        return false;
    }
    close(fd);
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
bool StorageNAS::DeleteMeta(const std::string& filename, MetaType meta_type) {
    std::string real_meta_dir_name;
    switch (meta_type) {
        case META_TMP_FILE:
            real_meta_dir_name = _metaDirName + "/";
            break;
        case META_TMP_KVTABLE:
            real_meta_dir_name = _metaDirName + "/GCacheObject/";
            break;
        default:
            Err("the meta_type is wrong, please input the right value\n");
            GCACHE_tmp_errno = GCACHE_TMP_ERR_NOT_SUPPORT;
            return false;
    }

    std::string metaFileName = real_meta_dir_name + filename;
    int acc_ret = access(metaFileName.c_str(), F_OK);
    if (acc_ret == -1) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_ACCSESS, errno);
        return true;
    }

    struct stat stat_buf;
    int sRet = stat(metaFileName.c_str(), &stat_buf);
    if (0 > sRet) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
        return false;
    }
    if (S_ISDIR(stat_buf.st_mode)) {
        bool rm_ret = RecursiveRemove(metaFileName);
        if (!rm_ret) {
            GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RMFILE, errno);
            return false;
        }
    } else if (S_ISREG(stat_buf.st_mode)) {
        int unlink_ret = unlink(metaFileName.c_str());
        if (unlink_ret == -1) {
            GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_LINK, errno);
            return false;
        }
    }

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
bool StorageNAS::RenameMeta(const std::string& old_name, const std::string &new_name,
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
            Err("the meta_type is wrong, please input the right value\n");
            GCACHE_tmp_errno = GCACHE_TMP_ERR_NOT_SUPPORT;
            return false;
    }

    std::string oldMetaFilename = real_meta_dir_name + old_name;
    std::string newMetaFilename = real_meta_dir_name + new_name;

    /*
     * auto make meta dir
     */
    int dirNamePos = newMetaFilename.find_last_of('/');
    std::string metaDir = newMetaFilename.substr(0, dirNamePos);
    bool bRet = RecursiveMakeDir(metaDir);
    if (!bRet) return false;

    int iRet = rename(oldMetaFilename.c_str(), newMetaFilename.c_str());
    if (iRet == -1) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RENAME, errno);
        return false;
    }
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
bool StorageNAS::MakeMetaDir(const std::string& dir_name) {
    std::string metaFileName = _metaDirName + "/" + dir_name;

    bool mRet = RecursiveMakeDir(metaFileName);
    if (!mRet) return false;
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
uint64_t StorageNAS::LastAccessed(const std::string &filename, MetaType meta_type) {
    std::string real_meta_dir_name;
    switch (meta_type) {
        case META_TMP_FILE:
            real_meta_dir_name = _metaDirName + "/";
            break;
        case META_TMP_KVTABLE:
            real_meta_dir_name = _metaDirName + "/GCacheObject/";
            break;
        default:
            Err("the meta_type is wrong, please input the right value\n");
            GCACHE_tmp_errno = GCACHE_TMP_ERR_NOT_SUPPORT;
            return false;
    }

    std::string metaFileName = real_meta_dir_name + filename;

    struct stat f_stat;
    int iRet = stat(metaFileName.c_str(), &f_stat);
    if (-1 == iRet) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
        return 0;
    }
    uint64_t time_ns = f_stat.st_atim.tv_nsec / 1000000 + f_stat.st_atim.tv_sec * 1000;
    return time_ns;
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

std::string StorageNAS::GetRootDir(int dirID) {
    std::map<int, DiskInfo>::iterator it = _rootDirs.find(dirID);
    if (it == _rootDirs.end()) {
        GCACHE_tmp_errno = GCACHE_TMP_ERR_GETFS;
        return "";
    }
    return (it->second).rootDir;
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

int64_t StorageNAS::GetRootTotalSpace(int dirID) {
//    struct statfs s_fs;
    std::map<int, DiskInfo>::iterator it = _rootDirs.find(dirID);
    if (it == _rootDirs.end()) {
        GCACHE_tmp_errno = GCACHE_TMP_ERR_GETFS;
        return -1;
    }
//    int iRet = statfs((it->second).rootDir.c_str(), &s_fs);
//    if (iRet == -1) {
//        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STORAGE, errno);
//        return -1;
//    }
//
//    int64_t totalSpace = s_fs.f_bsize * s_fs.f_blocks;
    return (int64_t) it->second.quota * 1024 * 1024 * 1024;
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

int64_t StorageNAS::GetRootFreeSpace(int dirID) {
//    struct statfs s_fs;
    std::map<int, DiskInfo>::iterator it = _rootDirs.find(dirID);
    if (it == _rootDirs.end()) {
        GCACHE_tmp_errno = GCACHE_TMP_ERR_GETFS;
        return -1;
    }
    int64_t all_space = (int64_t) it->second.quota * 1024 * 1024 * 1024;
    if (all_space < 0) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_SPACE_FULL, errno);
        return -1;
    }

    if (it->second.used_space <= 0) {
        it->second.used_space = 0;
        std::vector<std::string> dir_vector;
        dir_vector.push_back(it->second.rootDir);
        while (!dir_vector.empty()) {
            struct dirent *dp;
            DIR *dirp = opendir(dir_vector[dir_vector.size() - 1].c_str());
            std::string root_path = dir_vector[dir_vector.size() - 1] + "/";
            dir_vector.pop_back();
            while ((dp = readdir(dirp)) != NULL) {
                if (strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0) {
                    continue;
                }
                struct stat s_fs;
                std::string path = root_path + dp->d_name;
                int iRet = stat(path.c_str(), &s_fs);
                if (iRet == -1) {
                    GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
                    return -1;
                }
                if (S_IFDIR & s_fs.st_mode) {
                    dir_vector.push_back(path);
                    continue;
                }
                it->second.used_space += s_fs.st_size;
            }
            closedir(dirp);
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

int StorageNAS::ElectRootDir(CacheLevel cache_lv) {
    int ret_dir_id = -1;

    //int cache_lv_int = cache_lv;
    if (cache_lv < 0 || cache_lv > CACHE_NUM) {
        GCACHE_tmp_errno = GCACHE_TMP_ERR_CALEV;
        return -1;
    }
    if (_cacheInfo[cache_lv].empty()) {
        GCACHE_tmp_errno = GCACHE_TMP_ERR_CALEV;
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

CacheLevel StorageNAS::GetRealCacheLevel(CacheLevel cache_lv) {
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

CacheLevel StorageNAS::GetCacheLevel(int dirID) {
    std::map<int, DiskInfo>::iterator it = _rootDirs.find(dirID);
    if (it == _rootDirs.end()) {
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

std::map<int, Storage::DiskInfo> *StorageNAS::GetAllRootDir() {
    return &_rootDirs;
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

std::vector<Storage::DiskInfo> *StorageNAS::GetCacheInfo() {
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
bool StorageNAS::Init() {

    if (!ParseParam()) return false;
#if 0
    for(int i = CACHE_NUM - 2; i >= 0; --i) {
        if(_cacheInfo[i].empty()) {
            _cacheInfo[i] = _cacheInfo[i + 1];
        }
    }

    for(int i = 1; i < CACHE_NUM; ++i) {
        if(_cacheInfo[i].empty()) {
            _cacheInfo[i] = _cacheInfo[i - 1];
        }
    }
#endif
    _metaDirName = ((_metaDirs.begin())->second).rootDir; //(meta backup)

    /*
     * build data root dir
     */
    for (std::map<int, DiskInfo>::iterator it = _rootDirs.begin(); it != _rootDirs.end(); ++it) {
        std::string root_dir = it->second.rootDir;
        int aRet = access(root_dir.c_str(), F_OK);
        if (0 != aRet) {
            bool mRet = RecursiveMakeDir(root_dir);
            if (!mRet) {
                GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_MKDIR, errno);
                return false;
            }
        } else {
            aRet = access(root_dir.c_str(), W_OK | F_OK);
            if (0 != aRet) {
                GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_ACCSESS, errno);
                return false;
            }
        }
    }

    /*
     * build meta root dir
     */
    for (std::map<int, DiskInfo>::iterator it = _metaDirs.begin(); it != _metaDirs.end(); ++it) {
        std::string meta_root_dir = it->second.rootDir;
        int aRet = access(meta_root_dir.c_str(), F_OK);
        if (0 != aRet) {
            bool mRet = RecursiveMakeDir(meta_root_dir);
            if (!mRet) {
                GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_MKDIR, errno);
                return false;
            }
        } else {
            aRet = access(meta_root_dir.c_str(), W_OK | F_OK);
            if (0 != aRet) {
                GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_ACCSESS, errno);
                return false;
            }
        }
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
 *                 Config File Address: Get From a SysPath $GCACHE_TMP_NAS_CONF
 *
 */
bool StorageNAS::ParseParam() {
    std::string user = getenv("USER");
    //设置GCACHE_TMP_NAS_CONF环境变量重启电脑
    char *confile = getenv("GCACHE_TMP_NAS_CONF");
    if (confile == NULL) {
        printf("environment path GCACHE_TMP_NAS_CONF doesn't exist\n");
        return false;
    }
    std::string para_filename = confile;
//    Info("para_filename = %s\n", para_filename.c_str());
    FILE *fp = fopen(para_filename.c_str(), "r");
    if (NULL == fp) {
        GCACHE_tmp_errno = GCACHE_TMP_ERR_OPFILE;
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
            GCACHE_tmp_errno = GCACHE_TMP_ERR_REPDIR;
            fclose(fp);
            return false;
        }

        DiskInfo disk_info;
        disk_info.dirID = dirID;
        disk_info.rootDir = params[1] + "/" + user + "/";

        std::string entry_type = params[2];
        if (entry_type == "meta") {
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
                        GCACHE_tmp_errno = GCACHE_TMP_ERR_CALEV;
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
                            GCACHE_tmp_errno = GCACHE_TMP_ERR_CALEV;
                            fclose(fp);
                            return false;
                        }
                        disk_info.quota = -1;
                    } else {
                        if (no_cache_lv) {
                            GCACHE_tmp_errno = GCACHE_TMP_ERR_CALEV;
                            fclose(fp);
                            return false;
                        } else {
                            if (params[3] == "cache_l1") disk_info.cache_lv = CACHE_L1;
                            else if (params[3] == "cache_l2") disk_info.cache_lv = CACHE_L2;
                            else if (params[3] == "cache_l3") disk_info.cache_lv = CACHE_L3;
                            else {
                                GCACHE_tmp_errno = GCACHE_TMP_ERR_CALEV;
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
                            GCACHE_tmp_errno = GCACHE_TMP_ERR_CALEV;
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
                        GCACHE_tmp_errno = GCACHE_TMP_ERR_CALEV;
                        fclose(fp);
                        return false;
                    }

                    disk_info.quota = ToInt(params[4]);
                } else {
                    if (no_cache_lv) {
                        GCACHE_tmp_errno = GCACHE_TMP_ERR_CALEV;
                        fclose(fp);
                        return false;
                    } else {
                        if (params[3] == "cache_l1") disk_info.cache_lv = CACHE_L1;
                        else if (params[3] == "cache_l2") disk_info.cache_lv = CACHE_L2;
                        else if (params[3] == "cache_l3") disk_info.cache_lv = CACHE_L3;
                        else {
                            GCACHE_tmp_errno = GCACHE_TMP_ERR_CALEV;
                            fclose(fp);
                            return false;
                        }
                        disk_info.quota = ToInt(params[4]);
                    }
                }
            }

            if (disk_info.quota <= 0) {
                struct statfs s_fs;
                int iRet = statfs(disk_info.rootDir.c_str(), &s_fs);
                if (iRet == -1) {
                    errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STORAGE, errno);
                    return -1;
                }

                disk_info.quota = (int64_t) s_fs.f_bsize * s_fs.f_blocks / 1024 / 1024 / 1024;
            }
            _cacheInfo[disk_info.cache_lv].push_back(disk_info);
            _rootDirs.insert(std::make_pair(disk_info.dirID, disk_info));

            first_round = false;
        } else {
            GCACHE_tmp_errno = GCACHE_TMP_ERR_STORAGE;
            fclose(fp);
            return false;
        }
    }

    fclose(fp);
    return true;
}

NAMESPACE_END_SEISFS
}
