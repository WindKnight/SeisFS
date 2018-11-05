#include <fcntl.h>
#include <stddef.h>
#include <sys/types.h>
#include <cerrno>
#include <ctime>
#include <vector>
#include <hash_fun.h>
#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

#include "gcache_tmp_kvtable_hdfs.h"
#include "seisfs_meta.h"
#include "gcache_log.h"
#include "gcache_scoped_pointer.h"
#include "gcache_string.h"
#include "gcache_tmp_error.h"
#include "gcache_tmp_meta.h"
#include "gcache_tmp_storage.h"
#include "gcache_tmp_meta_hdfs.h"
#include "gcache_tmp_storage_hdfs.h"
#include "gcache_hdfs_utils.h"
#include "gcache_io.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {
/**
 * constructor
 *
 * @author          zhounan
 * @version                0.1.4
 * @param                table_name is the name of kvtable
 * @func                set member variable to initial values.
 * @return                no return.
 */
KVTableHDFS::KVTableHDFS(const std::string &table_name) :
        _table_name(table_name) {
    _storage = NULL;
    _rootFS = NULL;
}

/**
 * destructor
 *
 * @author          zhounan
 * @version                0.1.4
 * @param
 * @func                destruct object.
 * @return      no return.
 */
KVTableHDFS::~KVTableHDFS() {

}

/**
 * Open
 *
 * @author          zhounan
 * @version                0.1.4
 * @param       lifetime_days is the life time of this table, in days
 *              cmp_adv is the compress advice for opening this table
 *              cache_lv indicates the cache level of this table
 * @func            open a table if the table exists, if the table doesn't exist then create a table then open this new table.
 *                                         if a table already exists, setting lifetime won't change its original lifetime.
 * @return      if open successfully return true, else return false.
 */
bool KVTableHDFS::Open(lifetime_t lifetime_days, CompressAdvice cmp_adv, CacheLevel cache_lv) {
    lifetime_days = lifetime_days > 30 ? 30 : lifetime_days;
    if (!Exists()) {
        /*create new meta data of this table, then put meta data*/
        ScopedPointer<MetaTMPHDFS> meta(new MetaTMPHDFS);
        meta->meta.CompAdivce = cmp_adv;
        meta->meta.header.lifetime = lifetime_days;
        CacheLevel real_cache_lv = _storage->GetRealCacheLevel(cache_lv);
        meta->meta.dirID = _storage->ElectRootDir(real_cache_lv);
        bool bRet = _storage->PutMeta(_table_name, (char*) (meta.data()), sizeof(MetaTMPHDFS),
                META_TMP_KVTABLE);
        if (!bRet) {
            return NULL;
        }

        std::string root_dir = _storage->GetRootDir(meta->meta.dirID);
        std::string full_table_name = root_dir + "/GCacheObject/" + _table_name;

        hdfsFS fs = GetRootFS(root_dir);
        std::string path = hdfsGetPath(full_table_name);
        if (0 > hdfsCreateDirectory(fs, path.c_str())) {
            return false;
        }

        /*table actually is a folder, there will be OBJ_DIR_NUM sub-folders in this table, object will be hash to these sub-folders*/
        std::string obj_dir_base_name = full_table_name + "/obj_";
        for (int i = 0; i < OBJ_DIR_NUM; ++i) {
            std::string obj_dir_name = obj_dir_base_name + ToString(i);
            int iRet = hdfsCreateDirectory(fs, hdfsGetPath(obj_dir_name).c_str());
            if (iRet < 0) {
                return false;
            }
        }
    }
    return true;
}

/**
 * Put
 *
 * @author          zhounan
 * @version                0.1.4
 * @param       key is the sign of an object
 *              data is value of an object
 *              size indicates the size of data, in bytes
 * @func            put a key value pair into this table.
 * @return      if put key-value pair successfully return true, else return false.
 */
bool KVTableHDFS::Put(const std::string &key, const char* data, int64_t size) {
    std::string obj_filename = GetObjFilename(key);
    if ("" == obj_filename) {
        return false;
    }

    std::string root_dir = GetRootDirectory();
    hdfsFS fs = GetRootFS(root_dir);
    hdfsFile fd = hdfsOpenFile(fs, hdfsGetPath(obj_filename).c_str(), O_WRONLY, 0, REPLICATION, 0);
    if (fd < 0) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_OPFILE, errno);
        return false;
    }
    hdfsChmod(fs, hdfsGetPath(obj_filename).c_str(), 0644);

    ScopedPointer<MetaTMPHDFS> meta(GetMeta());
    if (meta->meta.CompAdivce == CMP_GZIP) {
        /*
         * (compress data and update size) nick
         */
        size_t compressSizeUpperBound = ZSTD_compressBound(size);
        char* compressBuffer = new char[compressSizeUpperBound];
        size_t const cSize = ZSTD_compress(compressBuffer, compressSizeUpperBound, data, size, 1);
        if (ZSTD_isError(cSize)) {
//                fprintf(stderr, "error compressing data_buffer : %s \n",  ZSTD_getErrorName(cSize));
            errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_COMPRESS, errno);
            delete[] compressBuffer;
            hdfsCloseFile(fs, fd);
            return false;
        }
        int64_t write_ret = SafeWrite(fs, fd, compressBuffer, cSize);
        delete[] compressBuffer;
        if (0 > write_ret) {
            errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_WRITE, errno);
            hdfsCloseFile(fs, fd);
            return false;
        }
    } else {
        int64_t write_ret = SafeWrite(fs, fd, data, size);
        if (0 > write_ret) {
            errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_WRITE, errno);
            hdfsCloseFile(fs, fd);
            return false;
        }
    }

    TouchTable(fs);
    hdfsCloseFile(fs, fd);
    return true;
}

/**
 * Delete
 *
 * @author          zhounan
 * @version                0.1.4
 * @param       key is the sign of an object
 * @func            delete a key-value pair of table.
 * @return      if delete successfully return true, else return false.
 */
bool KVTableHDFS::Delete(const std::string &key) {
    std::string obj_filename = GetObjFilename(key);
    if ("" == obj_filename) {
        return false;
    }

    std::string root_dir = GetRootDirectory();
    hdfsFS fs = GetRootFS(root_dir);
    std::string path = hdfsGetPath(obj_filename);
    int iRet = hdfsDelete(fs, path.c_str(), 0);
    if (0 > iRet) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RMFILE, errno);
        return false;
    }
    TouchTable(fs);
    return true;
}

/**
 * SizeOf
 *
 * @author          zhounan
 * @version                0.1.4
 * @param       key is the sign of an object
 *              data is the destination for object value
 *              size is the size of data, in bytes
 * @func                get the corresponding data and the size of this data according to the key.
 * @return                return true if get successfully, else return false.
 */
bool KVTableHDFS::Get(const std::string &key, char*& data, int64_t& size) {
    std::string obj_filename = GetObjFilename(key);
    if ("" == obj_filename) {
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

    std::string root_dir = GetRootDirectory();
    hdfsFS fs = GetRootFS(root_dir);
    std::string path = hdfsGetPath(obj_filename);
    hdfsFile fd = hdfsOpenFile(fs, path.c_str(), O_RDONLY, 0, REPLICATION, 0);
    if (0 > fd) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_OPFILE, errno);
        delete[] data;
        return false;
    }

    int64_t read_ret = SafeRead(fs, fd, data, size);
    if (0 > read_ret) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_READ, errno);
        hdfsCloseFile(fs, fd);
        delete[] data;
        return false;
    }

    ScopedPointer<MetaTMPHDFS> meta(GetMeta());
    if (meta->meta.CompAdivce == CMP_GZIP) {
        /*
         * (uncompress data and update size)
         */
        size_t decompressedSizeUpperBound = ZSTD_findDecompressedSize(data, size);
        char* rBuff = new char[decompressedSizeUpperBound];
        size_t const dSize = ZSTD_decompress(rBuff, decompressedSizeUpperBound, data, size);
        delete[] data;
        if (dSize == ZSTD_CONTENTSIZE_ERROR) {
//                        fprintf(stderr, "it was not compressed by zstd.\n");
            errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_COMPRESS, errno);
            delete[] rBuff;
            hdfsCloseFile(fs, fd);
            return false;
        } else if (dSize == ZSTD_CONTENTSIZE_UNKNOWN) {
//                        fprintf(stderr,"original size unknown. Use streaming decompression instead.\n");
            errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_COMPRESS, errno);
            delete[] rBuff;
            hdfsCloseFile(fs, fd);
            return false;
        }
        data = rBuff;
        size = dSize;
    }

    TouchTable(fs);
    hdfsCloseFile(fs, fd);
    return true;
}

/**
 * SizeOf
 *
 * @author          zhounan
 * @version                0.1.4
 * @param       key is the sign of an object.
 * @func                get the size of corresponding data according to the key, in bytes.
 * @return                return the key set of this table.
 */
int64_t KVTableHDFS::SizeOf(const std::string &key) {
    std::string obj_filename = GetObjFilename(key);
    if ("" == obj_filename) {
        return -1;
    }

    std::string root_dir = GetRootDirectory();
    hdfsFS fs = GetRootFS(root_dir);
    std::string path = hdfsGetPath(obj_filename);
    hdfsFileInfo* fi = hdfsGetPathInfo(fs, path.c_str());
    if (fi == NULL) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
        return -1;
    }
    int64_t size = fi->mSize;
    hdfsFreeFileInfo(fi, 1);
    return size;
}

/**
 * GetKeys
 *
 * @author          zhounan
 * @version                0.1.4
 * @param
 * @func                get the key set of this table.
 * @return                return the key set of this table.
 */
std::list<std::string> KVTableHDFS::GetKeys() {
    std::list<std::string> key_list;
    key_list.clear();

    std::string full_table_name = GetFullTableName();

    if (full_table_name == "") {
        return key_list;
    }

    std::string root_dir = GetRootDirectory();
    hdfsFS fs = GetRootFS(root_dir);
    std::string path = hdfsGetPath(full_table_name);
    std::string obj_dir_base_name = path + "/obj_";

    for (int i = 0; i < OBJ_DIR_NUM; ++i) {
        std::string obj_dir_name = obj_dir_base_name + ToString(i);
        int numEntries = 0;
        int *p = &numEntries;
        hdfsFileInfo* fi = hdfsListDirectory(fs, obj_dir_name.c_str(), p);
        hdfsFileInfo* info_free = fi;
        for (int j = 0; j < *p; ++j) {
            std::string str(fi->mName);
            std::vector<std::string> split_str = SplitString(str, "/");
            fi++;
            key_list.push_back(split_str.back());
        }
        hdfsFreeFileInfo(info_free, *p);
    }

    return key_list;
}

/**
 * Size
 *
 * @author          zhounan
 * @version                0.1.4
 * @param
 * @func                get the size of this table, in bytes.
 * @return                return the size of this table, if an error occurs, return -1.
 */
int64_t KVTableHDFS::Size() {
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
 * @author          zhounan
 * @version                0.1.4
 * @param
 * @func                determine whether this table exists.
 * @return                return true if this table exists, else return false.
 */
bool KVTableHDFS::Exists() {
    std::string fullPath = GetFullTableName();
    if (fullPath == "") {
        return false;
    }
    std::string root_dir = GetRootDirectory();
    hdfsFS fs = GetRootFS(root_dir);
    std::string path = hdfsGetPath(GetFullTableName());
    if (path == "") {
        return false;
    }
    int iRet = hdfsExists(fs, path.c_str());
    if (iRet == -1) {
        return false;
    } else {
        return true;
    }
}

/**
 * Lifetime
 *
 * @author          zhounan
 * @version                0.1.4
 * @param
 * @func                get the life time of this table, in days.
 * @return                return the life time of this table, if an error occurs, return -1.
 */
lifetime_t KVTableHDFS::Lifetime() const {
    ScopedPointer<MetaTMPHDFS> metaHDFSPtr(GetMeta());
    if (NULL == metaHDFSPtr) {
        return -1;
    }
    return metaHDFSPtr->meta.header.lifetime;
}

/**
 * SetLifetime
 *
 * @author          zhounan
 * @version                0.1.4
 * @param       lifetime_days is the life time of this table, in days.
 * @func                set the life time of this table, in days.
 * @return                return true if the setting is successful, else return false.
 */
bool KVTableHDFS::SetLifetime(lifetime_t lifetime_days) {
    lifetime_days = lifetime_days > 30 ? 30 : lifetime_days; // not bigger than 1 month

    ScopedPointer<MetaTMPHDFS> metaHDFSPtr(GetMeta());

    if (NULL == metaHDFSPtr) {
        return false;
    }

    metaHDFSPtr->meta.header.lifetime = lifetime_days;
    bool bRet = _storage->PutMeta(_table_name, (char*) (metaHDFSPtr.data()), sizeof(MetaTMPHDFS),
            META_TMP_KVTABLE);
    if (!bRet) {
        return false;
    }

    return true;
}

/**
 * Remove
 *
 * @author          zhounan
 * @version                0.1.4
 * @param
 * @func                remove this table exists.
 * @return                return true if this table is removed successfully, else return false.
 */
bool KVTableHDFS::Remove() {
    std::string full_table_name = GetFullTableName();
    if (full_table_name == "") {
        return false;
    }

    std::string root_dir = GetRootDirectory();
    hdfsFS fs = GetRootFS(root_dir);
    std::string path = hdfsGetPath(full_table_name);
    int iRet = hdfsExists(fs, path.c_str());
    if (iRet == -1) {
        return true;
    }

    bool bRet = hdfsDelete(fs, path.c_str(), 1);
    if (bRet != 0) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RMDIR, errno);
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
 * @author          zhounan
 * @version                0.1.4
 * @param       new_name is the new name of this table.
 * @func                rename the table old name to new name.
 * @return                return true if rename successful, else return false.
 */
bool KVTableHDFS::Rename(const std::string &new_name) {

    ScopedPointer<MetaTMPHDFS> metaHDFSPtr(GetMeta());
    std::string rootDir = _storage->GetRootDir(metaHDFSPtr->meta.dirID);
    std::string oldRealName = GetFullTableName();
    if (oldRealName == "") return false;

    std::string root_dir = GetRootDirectory();
    hdfsFS fs = GetRootFS(root_dir);
    std::string oldPath = hdfsGetPath(oldRealName);
    std::string newRealName = rootDir + "/GCacheObject/" + new_name;
    std::string newPath = hdfsGetPath(newRealName);
    int iRet = hdfsRename(fs, oldPath.c_str(), newPath.c_str());
    if (-1 == iRet) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RENAME, errno);
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
 * @author          zhounan
 * @version                0.1.4
 * @param
 * @func                get the name of this table.
 * @return                return the name of this table.
 */
std::string KVTableHDFS::GetName() const {
    return _table_name;
}

/**
 * LifetimeExpired
 *
 * @author          zhounan
 * @version                0.1.4
 * @param
 * @func                determine whether this table is expired or not.
 * @return                if this table is expired then return true, else return false.
 */
bool KVTableHDFS::LifetimeExpired() const {
    time_t now;
    time(&now);

    //uint64_t time_now_ms = now.tv_sec * 1000 + now.tv_usec / 1000;

    std::string full_table_name = GetFullTableName();
    std::string root_dir = GetRootDirectory();
    hdfsFS fs = GetRootFS(root_dir);
    std::string path = hdfsGetPath(full_table_name);
    hdfsFileInfo* fi = hdfsGetPathInfo(fs, path.c_str());
    if (fi == NULL) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
        return 0;
    }
    time_t last_modified_t = fi->mLastMod;
    time_t lifetime = Lifetime() * 24 * 3600;

    hdfsFreeFileInfo(fi, 1);
    if (now - last_modified_t < lifetime) {
        return true;
    } else {
        return false;
    }
}

/**
 * Init
 *
 * @author          zhounan
 * @version                0.1.4
 * @param
 * @func                initialize member variable _storage.
 * @return      if initialize successfully return true, else return false.
 */
bool KVTableHDFS::Init() {
    _storage = Storage::GetInstance(STORAGE_DIST);
    if (_storage == NULL) {
        Err("Get instance of HDFS storage ERROR\n");
        errno = GCACHE_TMP_ERR_STORAGE;
        return false;
    }

    _rootFS = ((StorageHDFS*) _storage)->GetAllRootFS();
    if (_rootFS == NULL) {
        return false;
    }

    return true;
}

/**
 * GetMeta
 *
 * @author          zhounan
 * @version                0.1.4
 * @param
 * @func            get the meta data of this table.
 * @return      if successfully get the meta data of this table return the pointer of MetaTMPHDFS object, else return NULL.
 */
MetaTMPHDFS *KVTableHDFS::GetMeta() const {
    char* metaHDFSData = NULL;
    int size = 0;

    bool bRet = _storage->GetMeta(_table_name, metaHDFSData, size, META_TMP_KVTABLE);
    if (!bRet) {
        return NULL;
    }
    return (MetaTMPHDFS*) metaHDFSData;
}

/**
 * GetFullTableName
 *
 * @author          zhounan
 * @version                0.1.4
 * @param
 * @func            get the full file name of this table, full file name contains the root directory.
 * @return      if get full file name successfully return true, else return "".
 */
std::string KVTableHDFS::GetFullTableName() const {
    ScopedPointer<MetaTMPHDFS> metaHDFSPtr(GetMeta());
    std::string full_table_name = "";
    if (metaHDFSPtr == NULL) {
        return full_table_name;
    } else {
        std::string rootDir = _storage->GetRootDir(metaHDFSPtr->meta.dirID);
        full_table_name = rootDir + "/GCacheObject/" + _table_name;
        return full_table_name;
    }
}

/**
 * GetRootDirectory
 *
 * @author          zhounan
 * @version                0.1.4
 * @param
 * @func                get the root directory of this file.
 * @return      return real file name, if an error occurs, return "".
 */
std::string KVTableHDFS::GetRootDirectory() const {

    ScopedPointer<MetaTMPHDFS> metaNasPtr(GetMeta());
    if (NULL == metaNasPtr) {
        return "";
    }
    std::string rootDir = _storage->GetRootDir(metaNasPtr->meta.dirID);
    return rootDir;
}

/**
 * GetObjFilename
 *
 * @author          zhounan
 * @version                0.1.4
 * @param       key is the sign of an object
 * @func            get the file name of object map into this key.
 * @return      if get file name successfully return true, else return "".
 */
std::string KVTableHDFS::GetObjFilename(const std::string &key) {
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
 * @author          zhounan
 * @version                0.1.4
 * @param
 * @func            set the table access and modification times of the table filename.
 * @return      if get file name successfully return true, else return "".
 */
void KVTableHDFS::TouchTable(hdfsFS fs) {
    time_t t;
    time(&t);
    std::string full_table_name = GetFullTableName();
    hdfsUtime(fs, hdfsGetPath(full_table_name).c_str(), t, t);
}

/**
 * GetRootFS
 *
 * @author          zhounan
 * @version                0.1.4
 * @param       rootDir is the root directory of this file.
 * @func                get the hdfsFS by rootDir.
 * @return      return hdfsFS if find fs successfully, else return NULL.
 */
hdfsFS KVTableHDFS::GetRootFS(std::string rootDir) const {
    std::map<std::string, hdfsFS>::iterator it = _rootFS->find(rootDir);
    if (it == _rootFS->end()) {
        errno = GCACHE_TMP_ERR_NOT_SUPPORT;
        return NULL;
    }
    return it->second;
}

NAMESPACE_END_SEISFS
}
