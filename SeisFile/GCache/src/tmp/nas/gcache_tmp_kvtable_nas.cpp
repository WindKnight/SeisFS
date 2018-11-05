/*
 * gcache_tmp_kvtable_nas.cpp
 *
 *  Created on: Dec 29, 2015
 *      Author: wb
 */

#include <dirent.h>
#include <fcntl.h>
#include <stddef.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <hash_fun.h>
#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>
//#include <iostream>

#include "gcache_tmp_kvtable_nas.h"
#include "seisfs_meta.h"
#include "gcache_io.h"
#include "gcache_log.h"
#include "gcache_scoped_pointer.h"
#include "gcache_string.h"
#include "gcache_tmp_error.h"
#include "gcache_tmp_meta.h"
#include "gcache_tmp_storage.h"
#include "gcache_tmp_meta_nas.h"

//extern int GCACHE_errno;
NAMESPACE_BEGIN_SEISFS
namespace tmp {
/**
 * constructor
 *
 * @author          weibing
 * @version                0.1.4
 * @param                table_name is the name of kvtable
 * @func                set member variable to initial values.
 * @return                no return.
 */
KVTableNAS::KVTableNAS(const std::string &table_name) :
        _table_name(table_name) {
    _storage = NULL;
}

/**
 * destructor
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                destruct object.
 * @return      no return.
 */
KVTableNAS::~KVTableNAS() {

}

/**
 * Open
 *
 * @author          weibing
 * @version                0.1.4
 * @param       lifetime_days is the life time of this table, in days
 *              cmp_adv is the compress advice for opening this table
 *              cache_lv indicates the cache level of this table
 * @func            open a table if the table exists, if the table doesn't exist then create a table then open this new table.
 * @return      if open successfully return true, else return false.
 */
bool KVTableNAS::Open(lifetime_t lifetime_days, CompressAdvice cmp_adv, CacheLevel cache_lv) {
    lifetime_days = lifetime_days > 30 ? 30 : lifetime_days;
    if (!Exists()) {
        /*create new meta data of this table, then put meta data*/
        ScopedPointer<MetaTMPNAS> meta(new MetaTMPNAS);
        meta->meta.CompAdivce = cmp_adv;
        meta->meta.header.lifetime = lifetime_days;
        CacheLevel real_cache_lv = _storage->GetRealCacheLevel(cache_lv);
        meta->meta.dirID = _storage->ElectRootDir(real_cache_lv);
        bool bRet = _storage->PutMeta(_table_name, (char*) (meta.data()), sizeof(MetaTMPNAS),
                META_TMP_KVTABLE);
        if (!bRet) {
            return NULL;
        }

        std::string root_dir = _storage->GetRootDir(meta->meta.dirID);
        std::string full_table_name = root_dir + "/GCacheObject/" + _table_name;

        bRet = RecursiveMakeDir(full_table_name);
        if (!bRet) {
            return false;
        }

        /*table actually is a folder, there will be OBJ_DIR_NUM sub-folders in this table, object will be hash to these sub-folders*/
        std::string obj_dir_base_name = full_table_name + "/obj_";
        for (int i = 0; i < OBJ_DIR_NUM; ++i) {
            std::string obj_dir_name = obj_dir_base_name + ToString(i);
            int iRet = mkdir(obj_dir_name.c_str(), 0755);
            if (iRet < 0) {
                GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_MKDIR, errno);
                return false;
            }
        }
    }

    return true;
}

/**
 * Put
 *
 * @author          weibing
 * @version                0.1.4
 * @param       key is the sign of an object
 *              data is value of an object
 *              size indicates the size of data, in bytes
 * @func            put a key value pair into this table.
 * @return      if put key-value pair successfully return true, else return false.
 */
bool KVTableNAS::Put(const std::string &key, const char* data, int64_t size) {
    std::string obj_filename = GetObjFilename(key);
    if ("" == obj_filename) {
        GCACHE_tmp_errno = GCACHE_TMP_ERR_GETOBJNAME;
        return false;
    }

    int fd = open(obj_filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (0 > fd) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_OPFILE, errno);
        return false;
    }

    ScopedPointer<MetaTMPNAS> meta(GetMeta());
    if (meta->meta.CompAdivce == CMP_GZIP) {
        /*
         * (compress data and update size)
         */
//        	std::cout<<"compress now....."<<std::endl;
        size_t cBuffSizeUpperBound = ZSTD_compressBound(size);
        char* cBuffer = new char[cBuffSizeUpperBound];
        size_t const cSize = ZSTD_compress(cBuffer, cBuffSizeUpperBound, data, size, 1);
        if (ZSTD_isError(cSize)) {
//                fprintf(stderr, "error compressing data_buffer : %s \n",  ZSTD_getErrorName(cSize));
            GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_COMPRESS, errno);
            delete[] cBuffer;
            close(fd);
            return false;
        }
        int64_t write_ret = SafeWrite(fd, cBuffer, cSize);
        delete[] cBuffer;
        if (0 > write_ret) {
            GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_WRITE, errno);
            close(fd);
            return false;
        }
    } else {
        int64_t write_ret = SafeWrite(fd, data, size);
        if (0 > write_ret) {
            GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_WRITE, errno);
            close(fd);
            return false;
        }
    }

    TouchTable();
    close(fd);
    return true;
}

/**
 * Delete
 *
 * @author          weibing
 * @version                0.1.4
 * @param       key is the sign of an object
 * @func            delete a key-value pair of table.
 * @return      if delete successfully return true, else return false.
 */
bool KVTableNAS::Delete(const std::string &key) {
    std::string obj_filename = GetObjFilename(key);
    if ("" == obj_filename) {
        return false;
    }

    int iRet = unlink(obj_filename.c_str());
    if (0 > iRet) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_LINK, errno);
        return false;
    }
    TouchTable();
    return true;
}

/**
 * SizeOf
 *
 * @author          weibing
 * @version                0.1.4
 * @param       key is the sign of an object
 *              data is the destination for object value
 *              size is the size of data, in bytes
 * @func                get the corresponding data and the size of this data according to the key.
 * @return                return true if get successfully, else return false.
 */
bool KVTableNAS::Get(const std::string &key, char*& data, int64_t& size) {
    std::string obj_filename = GetObjFilename(key);
    if ("" == obj_filename) {
        GCACHE_tmp_errno = GCACHE_TMP_ERR_GETOBJNAME;
        return false;
    }

    size = SizeOf(key);
    if (0 > size) {
        return false;
    }

    data = new char[size];
    if (data == NULL) {
        return false;
    }

    int fd = open(obj_filename.c_str(), O_RDONLY);
    if (0 > fd) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_OPFILE, errno);
        delete data;
        data = NULL;
        return false;
    }

    int64_t read_ret = SafeRead(fd, data, size);
    if (0 > read_ret) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_READ, errno);
        close(fd);
        delete data;
        data = NULL;
        return false;
    }

    ScopedPointer<MetaTMPNAS> meta(GetMeta());
    if (meta->meta.CompAdivce == CMP_GZIP) {
        /*
         * (uncompress data and update size)
         */
//        	std::cout<<"decompress now....."<<std::endl;
        size_t decompressedSizeUpperBound = ZSTD_findDecompressedSize(data, size);
        char* decompressedBuff = new char[decompressedSizeUpperBound];
        size_t const dSize = ZSTD_decompress(decompressedBuff, decompressedSizeUpperBound, data,
                size);
        delete[] data;
        if (dSize == ZSTD_CONTENTSIZE_ERROR) {
//                        fprintf(stderr, "it was not compressed by zstd.\n");
            GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_COMPRESS, errno);
            delete[] decompressedBuff;
            close(fd);
            return false;
        } else if (dSize == ZSTD_CONTENTSIZE_UNKNOWN) {
//                        fprintf(stderr,"original size unknown. Use streaming decompression instead.\n");
            GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_COMPRESS, errno);
            delete[] decompressedBuff;
            close(fd);
            return false;
        }
        data = decompressedBuff;
        size = dSize;
    }
    TouchTable();
    close(fd);
    return true;
}

/**
 * SizeOf
 *
 * @author          weibing
 * @version                0.1.4
 * @param       key is the sign of an object.
 * @func                get the size of corresponding data according to the key, in bytes.
 * @return                return the key set of this table.
 */
int64_t KVTableNAS::SizeOf(const std::string &key) {
    std::string obj_filename = GetObjFilename(key);
    if ("" == obj_filename) {
        GCACHE_tmp_errno = GCACHE_TMP_ERR_GETOBJNAME;
        return -1;
    }

    struct stat stat_buf;
    int iRet = stat(obj_filename.c_str(), &stat_buf);
    if (0 > iRet) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
        return -1;
    }
    int64_t size = stat_buf.st_size;
    return size;
}

/**
 * GetKeys
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                get the key set of this table.
 * @return                return the key set of this table.
 */
std::list<std::string> KVTableNAS::GetKeys() {
    std::list<std::string> key_list;
    key_list.clear();

    std::string full_table_name = GetFullTableName();

    std::string obj_dir_base_name = full_table_name + "/obj_";
    for (int i = 0; i < OBJ_DIR_NUM; ++i) {
        std::string obj_dir_name = obj_dir_base_name + ToString(i);

        DIR *obj_dir = opendir(obj_dir_name.c_str());
        if (NULL == obj_dir) {
            GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_OPDIR, errno);
            return key_list;
        }

        struct dirent entry;
        struct dirent *result;
        for (int dRet = readdir_r(obj_dir, &entry, &result); result != NULL && dRet == 0; dRet =
                readdir_r(obj_dir, &entry, &result)) {

            if (strcmp(entry.d_name, ".") == 0 || strcmp(entry.d_name, "..") == 0) {
                continue;
            }

            std::string full_entry_name = obj_dir_name + "/" + entry.d_name;
            struct stat stat_buf;
            int sRet = stat(full_entry_name.c_str(), &stat_buf);
            if (sRet < 0) {
                GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
                closedir(obj_dir);
                return key_list;
            }

            if (S_ISREG(stat_buf.st_mode)) {
                key_list.push_back(entry.d_name);
            }

        }

        closedir(obj_dir);
    }

    return key_list;
}

/**
 * Size
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                get the size of this table, in bytes.
 * @return                return the size of this table, if an error occurs, return -1.
 */
int64_t KVTableNAS::Size() {
    int64_t size = 0;

    std::list<std::string> key_list = GetKeys();
    for (std::list<std::string>::iterator it = key_list.begin(); it != key_list.end(); ++it) {
        int64_t obj_size = SizeOf(*it);
        if (obj_size < 0) {
            return -1;
        }
        size += obj_size;
    }
    return size;
}

/**
 * Exists
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                determine whether this table exists.
 * @return                return true if this table exists, else return false.
 */
bool KVTableNAS::Exists() {
    std::string full_table_name = GetFullTableName();
    if (full_table_name == "") {
        return false;
    }

    int iRet = access(full_table_name.c_str(), F_OK);
    if (iRet == -1) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_ACCSESS, errno);
        return false;
    } else {
        return true;
    }
}

/**
 * Lifetime
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                get the life time of this table, in days.
 * @return                return the life time of this table, if an error occurs, return -1.
 */
lifetime_t KVTableNAS::Lifetime() const {
    ScopedPointer<MetaTMPNAS> metaNASPtr(GetMeta());
    if (NULL == metaNASPtr) {
        return -1;
    }
    return metaNASPtr->meta.header.lifetime;
}

/**
 * SetLifetime
 *
 * @author          weibing
 * @version                0.1.4
 * @param       lifetime_days is the life time of this table, in days.
 * @func                set the life time of this table, in days.
 * @return                return true if the setting is successful, else return false.
 */
bool KVTableNAS::SetLifetime(lifetime_t lifetime_days) {
    lifetime_days = lifetime_days > 30 ? 30 : lifetime_days; // not bigger than 1 month
    ScopedPointer<MetaTMPNAS> metaNASPtr(GetMeta());

    if (NULL == metaNASPtr) {
        return false;
    }

    metaNASPtr->meta.header.lifetime = lifetime_days;
    bool bRet = _storage->PutMeta(_table_name, (char*) (metaNASPtr.data()), sizeof(MetaTMPNAS),
            META_TMP_KVTABLE);
    if (!bRet) {
        return false;
    }

    return true;
}

/**
 * Remove
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                remove this table exists.
 * @return                return true if this table is removed successfully, else return false.
 */
bool KVTableNAS::Remove() {
    std::string full_table_name = GetFullTableName();
    if (full_table_name == "") {
        return false;
    }

    int iRet = access(full_table_name.c_str(), F_OK);
    if (iRet == -1) {
        return true;
    }

    bool bRet = RecursiveRemove(full_table_name);
    if (!bRet) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RMDIR, errno);
        return false;
    }
    bRet = _storage->DeleteMeta(_table_name, META_TMP_KVTABLE);
    if (!bRet) {
        return false;
    }
    return true;
}

/**
 * Rename
 *
 * @author          weibing
 * @version                0.1.4
 * @param       new_name is the new name of this table.
 * @func                rename the table old name to new name.
 * @return                return true if rename successful, else return false.
 */
bool KVTableNAS::Rename(const std::string &new_name) {
    ScopedPointer<MetaTMPNAS> metaNASPtr(GetMeta());
    std::string rootDir = _storage->GetRootDir(metaNASPtr->meta.dirID);
    std::string oldRealName = GetFullTableName();
    if (oldRealName == "") {
        return false;
    }
    std::string newRealName = rootDir + "/GCacheObject/" + new_name;

    int iRet = rename(oldRealName.c_str(), newRealName.c_str());
    if (-1 == iRet) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RENAME, errno);
        return false;
    }

    bool bRet = _storage->RenameMeta(_table_name, new_name, META_TMP_KVTABLE);
    if (!bRet) {
        return false;
    }

    _table_name = new_name;
    return true;
}

/**
 * GetName
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                get the name of this table.
 * @return                return the name of this table.
 */
std::string KVTableNAS::GetName() const {
    return _table_name;
}

/**
 * LifetimeExpired
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                determine whether this table is expired or not.
 * @return                if this table is expired then return true, else return false.
 */
bool KVTableNAS::LifetimeExpired() const {
    struct timeval now;
    gettimeofday(&now, NULL);

    uint64_t time_now_ms = now.tv_sec * 1000 + now.tv_usec / 1000;

    struct stat dir_stat;
    std::string full_table_name = GetFullTableName();
    int iRet = stat(full_table_name.c_str(), &dir_stat);
    if (-1 == iRet) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
        return 0;
    }
    uint64_t time_last_modified_ms = dir_stat.st_mtim.tv_nsec / 1000000
            + dir_stat.st_mtim.tv_sec * 1000;

    lifetime_t lifetime = Lifetime();
    uint64_t lifetime_ms = lifetime * 24 * 3600;
    lifetime_ms *= 1000;
    if (time_now_ms - time_last_modified_ms > lifetime_ms) {
        return true;
    } else {
        GCACHE_tmp_errno = GCACHE_TMP_ERR_LIFETIME;
        return false;
    }
}

/**
 * Init
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                initialize member variable _storage.
 * @return      if initialize successfully return true, else return false.
 */
bool KVTableNAS::Init() {
    _storage = Storage::GetInstance(STORAGE_NAS);
    if (_storage == NULL) {
        Err("Get instance of NAS storage ERROR\n");
        GCACHE_tmp_errno = GCACHE_TMP_ERR_STORAGE;
        return false;
    }
    return true;
}

/**
 * GetMeta
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func            get the meta data of this table.
 * @return      if successfully get the meta data of this table return the pointer of MetaTMPNAS object, else return NULL.
 */
MetaTMPNAS *KVTableNAS::GetMeta() const {
    char* metaNASData = NULL;
    int size = 0;

    bool bRet = _storage->GetMeta(_table_name, metaNASData, size, META_TMP_KVTABLE);
    if (!bRet) {
        return NULL;
    }
    return (MetaTMPNAS*) metaNASData;
}

/**
 * GetFullTableName
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func            get the full file name of this table, full file name contains the root directory.
 * @return      if get full file name successfully return true, else return "".
 */
std::string KVTableNAS::GetFullTableName() const {
    ScopedPointer<MetaTMPNAS> metaNASPtr(GetMeta());
    if (NULL == metaNASPtr) {
        return "";
    }

    std::string rootDir = _storage->GetRootDir(metaNASPtr->meta.dirID);
    std::string full_table_name = rootDir + "/GCacheObject/" + _table_name;

    return full_table_name;
}

/**
 * GetObjFilename
 *
 * @author          weibing
 * @version                0.1.4
 * @param       key is the sign of an object
 * @func            get the file name of object map into this key.
 * @return      if get file name successfully return true, else return "".
 */
std::string KVTableNAS::GetObjFilename(const std::string &key) {
    std::string full_table_name = GetFullTableName();
    if (full_table_name == "") {
        return "";
    }
    __gnu_cxx ::hash<char*> h;
    size_t hash_num = h(key.c_str());
    int tail_num = hash_num % OBJ_DIR_NUM;

    std::string obj_dir_name = full_table_name + "/obj_" + ToString(tail_num);
    std::string obj_file_name = obj_dir_name + "/" + key;

    return obj_file_name;
}

/**
 * TouchTable
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func            set the table access and modification times of the table filename.
 * @return      if get file name successfully return true, else return "".
 */
void KVTableNAS::TouchTable() {
    struct timeval time_now_arr[2];
    gettimeofday(&time_now_arr[0], NULL);
    time_now_arr[1] = time_now_arr[0];
    std::string full_table_name = GetFullTableName();
    utimes(full_table_name.c_str(), time_now_arr);
}

NAMESPACE_END_SEISFS
}
