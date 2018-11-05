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

#include "gcache_tmp_storage_local.h"
#include "gcache_io.h"
#include "gcache_string.h"
#include "gcache_tmp_error.h"
#include "gcache_log.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {

StorageLocal::StorageLocal() {

}

StorageLocal::~StorageLocal() {

}

bool StorageLocal::PutMeta(const std::string& filename, const char* metaData, int size,
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
            errno = GCACHE_TMP_ERR_NOT_SUPPORT;
            return false;
    }
    std::string metaFileName = real_meta_dir_name + filename;
    int dirNamePos = metaFileName.find_last_of('/');
    std::string metaDir = metaFileName.substr(0, dirNamePos);

    bool bRet = RecursiveMakeDir(metaDir);
    if (!bRet) return false;

    int fd = open(metaFileName.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (0 > fd) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_PUTMETA, errno);
        return false;
    }
    int write_len = sizeof(size);
    if (write_len != write(fd, &size, write_len)) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_PUTMETA, errno);
        return false;
    }
    write_len = size;
    if (write_len != write(fd, metaData, write_len)) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_PUTMETA, errno);
        return false;
    }
    close(fd);
    return true;
}

bool StorageLocal::GetMeta(const std::string& filename, char*& metaData, int& size,
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
            errno = GCACHE_TMP_ERR_NOT_SUPPORT;
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

bool StorageLocal::DeleteMeta(const std::string& filename, MetaType meta_type) {
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
            errno = GCACHE_TMP_ERR_NOT_SUPPORT;
            return false;
    }

    std::string metaFileName = real_meta_dir_name + filename;
    int acc_ret = access(metaFileName.c_str(), F_OK);
    if (acc_ret == -1) {
        return true;
    }

    struct stat stat_buf;
    int sRet = stat(metaFileName.c_str(), &stat_buf);
    if (0 > sRet) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
        return false;
    }
    if (S_ISDIR(stat_buf.st_mode)) {
        bool rm_ret = RecursiveRemove(metaFileName);
        if (!rm_ret) {
            errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RMFILE, errno);
            return false;
        }
    } else if (S_ISREG(stat_buf.st_mode)) {
        int unlink_ret = unlink(metaFileName.c_str());
        if (unlink_ret == -1) {
            errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_LINK, errno);
            return false;
        }
    }

    return true;
}

bool StorageLocal::RenameMeta(const std::string& old_name, const std::string &new_name,
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
            errno = GCACHE_TMP_ERR_NOT_SUPPORT;
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
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RENMETA, errno);
        return false;
    }
    return true;
}

bool StorageLocal::MakeMetaDir(const std::string& dir_name) {
    std::string metaFileName = _metaDirName + "/" + dir_name;

    bool mRet = RecursiveMakeDir(metaFileName);
    if (!mRet) return false;
    return true;
}

uint64_t StorageLocal::LastAccessed(const std::string &filename, MetaType meta_type) {
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
            errno = GCACHE_TMP_ERR_NOT_SUPPORT;
            return false;
    }

    std::string metaFileName = real_meta_dir_name + filename;

    struct stat f_stat;
    int iRet = stat(metaFileName.c_str(), &f_stat);
    if (-1 == iRet) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
        return 0;
    }
    uint64_t time_ns = f_stat.st_atim.tv_nsec / 1000000 + f_stat.st_atim.tv_sec * 1000;
    return time_ns;
}

std::string StorageLocal::GetRootDir(int dirID) {
    std::map<int, DiskInfo>::iterator it = _rootDirs.find(dirID);
    if (it == _rootDirs.end()) {
        return "";
    }
    return (it->second).rootDir;
}

int64_t StorageLocal::GetRootTotalSpace(int dirID) {
//    struct statfs s_fs;
    std::map<int, DiskInfo>::iterator it = _rootDirs.find(dirID);
    if (it == _rootDirs.end()) {
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

int64_t StorageLocal::GetRootFreeSpace(int dirID) {
    std::map<int, DiskInfo>::iterator it = _rootDirs.find(dirID);
    if (it == _rootDirs.end()) {
        return -1;
    }
    int64_t all_space = (int64_t) it->second.quota * 1024 * 1024 * 1024;
    if (all_space < 0) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_SPACE_FULL, errno);
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
                    errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STORAGE, errno);
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

int StorageLocal::ElectRootDir(CacheLevel cache_lv) {
    int ret_dir_id = -1;

    if (cache_lv < 0 || cache_lv > CACHE_NUM) {
        errno = GCACHE_TMP_ERR_CALEV;
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

CacheLevel StorageLocal::GetRealCacheLevel(CacheLevel cache_lv) {
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

CacheLevel StorageLocal::GetCacheLevel(int dirID) {
    std::map<int, DiskInfo>::iterator it = _rootDirs.find(dirID);
    if (it == _rootDirs.end()) {
        errno = GCACHE_TMP_ERR_STORAGE;
        return CACHE_L1;
    }
    return (it->second).cache_lv;
}

std::map<int, Storage::DiskInfo> *StorageLocal::GetAllRootDir() {
    return &_rootDirs;
}

std::vector<Storage::DiskInfo> *StorageLocal::GetCacheInfo() {
    return _cacheInfo;
}

bool StorageLocal::Init() {

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
                errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_MKDIR, errno);
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
                errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_MKDIR, errno);
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

bool StorageLocal::ParseParam() {
    std::string user = getenv("USER");
    //设置GCACHE_TMP_LOCAL_CONF环境变量重启电脑
    char *confile = getenv("GCACHE_TMP_LOCAL_CONF");
    if (confile == NULL) {
        Err("environment path GCACHE_TMP_LOCAL_CONF doesn't exist\n");
        return false;
    }
    std::string para_filename = confile;
//    Err("para_filename = %s\n", para_filename.c_str());
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
            fclose(fp);
            return false;
        }

        DiskInfo disk_info;
        disk_info.dirID = dirID;
        disk_info.used_space = -1;
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
                        errno = GCACHE_TMP_ERR_CALEV;
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
                            errno = GCACHE_TMP_ERR_CALEV;
                            fclose(fp);
                            return false;
                        }
                        disk_info.quota = -1;
                    } else {
                        if (no_cache_lv) {
                            errno = GCACHE_TMP_ERR_CALEV;
                            fclose(fp);
                            return false;
                        } else {
                            if (params[3] == "cache_l1") disk_info.cache_lv = CACHE_L1;
                            else if (params[3] == "cache_l2") disk_info.cache_lv = CACHE_L2;
                            else if (params[3] == "cache_l3") disk_info.cache_lv = CACHE_L3;
                            else {
                                errno = GCACHE_TMP_ERR_CALEV;
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
                        errno = GCACHE_TMP_ERR_CALEV;
                        fclose(fp);
                        return false;
                    }

                    disk_info.quota = ToInt(params[4]);
                } else {
                    if (no_cache_lv) {
                        errno = GCACHE_TMP_ERR_CALEV;
                        fclose(fp);
                        return false;
                    } else {
                        if (params[3] == "cache_l1") disk_info.cache_lv = CACHE_L1;
                        else if (params[3] == "cache_l2") disk_info.cache_lv = CACHE_L2;
                        else if (params[3] == "cache_l3") disk_info.cache_lv = CACHE_L3;
                        else {
                            errno = GCACHE_TMP_ERR_CALEV;
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
            errno = GCACHE_TMP_ERR_STORAGE;
            fclose(fp);
            return false;
        }
    }

    fclose(fp);
    return true;
}

NAMESPACE_END_SEISFS
}
