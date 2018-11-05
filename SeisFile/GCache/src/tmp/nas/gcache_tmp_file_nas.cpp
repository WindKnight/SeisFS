/*
 * gcache_tmp_file_nas.cpp
 *
 *  Created on: Dec 29, 2015
 *      Author: wb
 */

#include <sys/mman.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <cerrno>
#include <cstdio>
#include <ctime>

#include "gcache_tmp_file_nas.h"
#include "seisfs_meta.h"
#include "gcache_scoped_pointer.h"
#include "gcache_tmp_dir.h"
#include "gcache_tmp_error.h"
#include "gcache_tmp_meta.h"
#include "gcache_tmp_storage.h"
#include "gcache_tmp_meta_nas.h"
#include "gcache_log.h"

//extern int GCACHE_errno;
NAMESPACE_BEGIN_SEISFS
namespace tmp {
/**
 * constructor
 *
 * @author          weibing
 * @version                0.1.4
 * @param                file_name is the name of this file
 * @func                initialize member.
 * @return                no return
 */
FileNAS::FileNAS(const std::string &file_name) {
    _filename = file_name;
    _storage = NULL;
    boost::shared_ptr<bool> isWriterAlive(new bool(false));
    _isWriterAlive = isWriterAlive;
}

/**
 * destructor
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                destruct object
 * @return      no return
 */
FileNAS::~FileNAS() {

}

/**
 * OpenReader
 *
 * @author          weibing
 * @version                0.1.4
 * @param                advice is the advice indicates what way to read.
 * @func                set the ReaderNAS object according to advice,then return the pointer of object.
 * @return                return the pointer of ReaderNAS object, if an error occurs, return NULL.
 */
ReaderNAS* FileNAS::OpenReader(FAdvice advice) {

    /*get real file name*/
    ScopedPointer<MetaTMPNAS> meta(GetMeta());
    if (NULL == meta) {
        return NULL;
    }
    int dirID = meta->meta.dirID;
    std::string rootDir = _storage->GetRootDir(dirID);
    std::string realFilename = rootDir + "/" + _filename;

    ReaderNAS *readerNAS = NULL;
    int iRet;

    int OPEN_FLAG = O_RDONLY;
    if (advice == FADV_RAW) {
        OPEN_FLAG |= O_DIRECT;
    }

    int fd = open(realFilename.c_str(), OPEN_FLAG);
    if (fd == -1) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_OPFILE, errno);
        return NULL;
    }

    bool direct_read;
    void *map_buf;

    switch (advice) {
        case FADV_NONE:
            direct_read = false;
            map_buf = NULL;
            break;
        case FADV_RAW:
            direct_read = true;
            map_buf = NULL;
            break;
        case FADV_SEQUENTIAL:
            direct_read = false;
            map_buf = NULL;

            iRet = posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
            if (0 > iRet) {
                GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_ADVICE, errno);
                close(fd);
                return NULL;
            }
            break;
        case FADV_RANDOM:
            direct_read = false;
            map_buf = NULL;

            iRet = posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM);
            if (0 > iRet) {
                GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_ADVICE, errno);
                close(fd);
                return NULL;
            }
            break;
        case FADV_MMAP:

            direct_read = false;

            struct stat stat_buf;
            iRet = stat(realFilename.c_str(), &stat_buf);
            if (iRet < 0) {
                GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
                close(fd);
                return NULL;
            }

            map_buf = mmap(0, stat_buf.st_size, PROT_READ, MAP_SHARED, fd, 0);
            if (map_buf == MAP_FAILED) {
                GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_MAP, errno);
                close(fd);
                return NULL;
            }
            break;
        default:
            GCACHE_tmp_errno = GCACHE_TMP_ERR_PATH;
            return NULL;
            break;
    }

    readerNAS = new ReaderNAS(fd, direct_read, (char*) map_buf);
    if (readerNAS->Init()) {
        return readerNAS;
    } else {
        return NULL;
    }

}

/**
 * OpenWriter
 *
 * @author          weibing
 * @version                0.1.4
 * @param                lifetime_days is the number of days this file can live.
 *              cache_lv is the cache level of this file.
 *              advice advice is the advice indicates what way to write.
 * @func                get the file descriptor then use it to construct a WriterNAS object,then return the pointer of object.
 * @return                return the pointer of WriterNAS object, if an error occurs, return NULL.
 */
WriterNAS* FileNAS::OpenWriter(lifetime_t lifetime_days, CacheLevel cache_lv, FAdvice advice) {
    lifetime_days = lifetime_days > 30 ? 30 : lifetime_days; // not bigger than 1 month

    if (*_isWriterAlive) {
        Err("can not create a new writer, the old writer is still alive\n");
        GCACHE_tmp_errno = GCACHE_TMP_ERR_NOT_SUPPORT;
        return NULL;
    }

    int pos = _filename.find_last_of('/');
    std::string dirPath = _filename.substr(0, pos + 1);
    StorageArch s_arch = STORAGE_NAS;
    ScopedPointer<Dir> dir(Dir::New(dirPath, s_arch));
    if (dir == NULL || s_arch != STORAGE_NAS) {
        return NULL;
    }

    if (!dir->Exist()) {
        if (!dir->MakeDirs()) {
            return NULL;
        }
    }

    ScopedPointer<MetaTMPNAS> meta(GetMeta());
    WriterNAS *writerNAS = NULL;

    int OPEN_FLAG = O_WRONLY | O_CREAT;
    if (advice == FADV_RAW) {
        GCACHE_tmp_errno = GCACHE_TMP_ERR_ADVICE;
        return NULL;
    }

    CacheLevel real_cache_lv = _storage->GetRealCacheLevel(cache_lv);
    std::string full_filename;

    if (meta == NULL) { //file does not exist, write to new file
        Warn("file %s does not exist, write to new file\n", _filename.c_str());
        ScopedPointer<MetaTMPNAS> meta_new(new MetaTMPNAS());
        meta_new->meta.dirID = _storage->ElectRootDir(real_cache_lv);
        if (_storage->GetRootFreeSpace(meta_new->meta.dirID) <= 0) {
            Err("can not create a new writer, there is no space for new file\n");
            GCACHE_tmp_errno = GCACHE_TMP_ERR_SPACE_FULL;
            return NULL;
        }
        meta_new->meta.header.lifetime = lifetime_days;
        bool bRet = _storage->PutMeta(_filename, (char*) (meta_new.data()), sizeof(MetaTMPNAS),
                META_TMP_FILE);
        if (!bRet) {
            return NULL;
        }

        std::string rootDir = _storage->GetRootDir(meta_new->meta.dirID);
        full_filename = rootDir + "/" + _filename;

    } else { //file exist

        if (LifetimeExpired()) { // lifetime expired

            Warn("file exist, but lifetime is expired, write to a new file\n");

            if (!Remove()) {
                return NULL;
            }

            ScopedPointer<MetaTMPNAS> meta_new(new MetaTMPNAS());
            meta_new->meta.dirID = _storage->ElectRootDir(real_cache_lv);
            if (_storage->GetRootFreeSpace(meta_new->meta.dirID) <= 0) {
                Err("can not create a new writer, there is no space for new file\n");
                GCACHE_tmp_errno = GCACHE_TMP_ERR_SPACE_FULL;
                return NULL;
            }
            meta_new->meta.header.lifetime = lifetime_days;
            bool bRet = _storage->PutMeta(_filename, (char*) (meta_new.data()), sizeof(MetaTMPNAS),
                    META_TMP_FILE);
            if (!bRet) {
                return NULL;
            }

            std::string rootDir = _storage->GetRootDir(meta_new->meta.dirID);
            full_filename = rootDir + "/" + _filename;

        } else { // file is valid

            int old_dirID = meta->meta.dirID;
            if (_storage->GetRootFreeSpace(old_dirID) <= 0) {
                GCACHE_tmp_errno = GCACHE_TMP_ERR_SPACE_FULL;
                return NULL;
            }
            CacheLevel old_cache_lv = _storage->GetCacheLevel(old_dirID);
            if (old_cache_lv == real_cache_lv) { // file exist and cache lv is the same, write to old file
                Warn("file %s exist and cache lv is the same, write to old file\n",
                        _filename.c_str());
                std::string rootDir = _storage->GetRootDir(old_dirID);
                full_filename = rootDir + "/" + _filename;

            } else { // file exist and cache lv is different, move old file to new file and write to new file
                Warn(
                        "file %s exist and cache lv is different, move old file to new file, and write to new file\n",
                        _filename.c_str());
                std::string old_root_dir = _storage->GetRootDir(old_dirID);
                std::string old_full_filename = old_root_dir + "/" + _filename;

                meta->meta.dirID = _storage->ElectRootDir(real_cache_lv);
                bool bRet = _storage->PutMeta(_filename, (char*) (meta.data()), sizeof(MetaTMPNAS),
                        META_TMP_FILE);
                if (!bRet) {
                    return NULL;
                }

                std::string new_root_dir = _storage->GetRootDir(meta->meta.dirID);
                full_filename = new_root_dir + "/" + _filename;

                int rn_ret = rename(old_full_filename.c_str(), full_filename.c_str());
                if (0 > rn_ret) {
                    GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RENAME, errno);
                    return NULL;
                }

            }
        }
    }

    int fd = open(full_filename.c_str(), OPEN_FLAG, 0644);
    if (0 > fd) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_OPFILE, errno);
        return NULL;
    }
    off_t ret_off = lseek(fd, 0, SEEK_END);
    if (ret_off == (off_t) -1) {
        return NULL;
    }
    writerNAS = new WriterNAS(fd, _isWriterAlive);
    return writerNAS;
}

/**
 * Size
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                get the size of this file, in bytes.
 * @return                return the size of this file, if an error occurs, return -1.
 */
int64_t FileNAS::Size() const {
    std::string realFilename = GetRealFilename();
    if (realFilename == "") {
        return -1;
    }

    struct stat f_stat;
    int iRet = stat(realFilename.c_str(), &f_stat);
    if (iRet == -1) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
        return -1;
    }
    int64_t fileSize = f_stat.st_size;
    return fileSize;
}

/**
 * Lifetime
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                get the life time of this file, in days.
 * @return                return the life time of this file, if an error occurs, return -1.
 */
lifetime_t FileNAS::Lifetime() const {
    ScopedPointer<MetaTMPNAS> metaNASPtr(GetMeta());
    if (NULL == metaNASPtr) {
        return -1;
    }
    return metaNASPtr->meta.header.lifetime;
}

/**
 * Exists
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                determine whether this file exists.
 * @return                return true if this file exists, else return false.
 */
bool FileNAS::Exists() const {
    std::string realFilename = GetRealFilename();
    if (realFilename == "") {
        return false;
    }

    int iRet = access(realFilename.c_str(), F_OK);
    if (iRet == -1) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_ACCSESS, errno);
        return false;
    } else {
        return true;
    }
}

/**
 * Remove
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                remove this file exists.
 * @return                return true if this file is removed successfully, else return false.
 */
bool FileNAS::Remove() {
    std::string realFilename = GetRealFilename();
    if (realFilename == "") {
        return false;
    }

    int iRet = access(realFilename.c_str(), F_OK);
    if (iRet == -1) {
        return true;
    }
    iRet = remove(realFilename.c_str());
    if (iRet == -1) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RMFILE, errno);
        return false;
    }
    bool bRet = _storage->DeleteMeta(_filename, META_TMP_FILE);
    if (!bRet) {
        return false;
    }
    return true;
}

/**
 * Truncate
 *
 * @author          weibing
 * @version        0.1.2
 * @param      size is the length of filename, in bytes.
 * @func                changes the size of filename to length. If length is shorter than the previous length,
 data at the end will be lost.
 * @return                return true if this file is truncated successfully, else return false.
 */
bool FileNAS::Truncate(int64_t size) {
    std::string realFilename = GetRealFilename();
    if (realFilename == "") {
        return false;
    }

    int iRet = truncate(realFilename.c_str(), size);
    if (iRet == -1) {
        GCACHE_tmp_errno = GCACHE_TMP_ERR_TRUNCATE;
        return false;
    }
    return true;
}

/**
 * SetLifetime
 *
 * @author          weibing
 * @version                0.1.4
 * @param       lifetime_days is the life time of this file, in days.
 * @func                set the life time of this file, in days.
 * @return                return true if the setting is successful, else return false.
 */
bool FileNAS::SetLifetime(lifetime_t lifetime_days) {
    lifetime_days = lifetime_days > 30 ? 30 : lifetime_days; // not bigger than 1 month

    ScopedPointer<MetaTMPNAS> metaNASPtr(GetMeta());

    if (NULL == metaNASPtr) {
        return false;
    }

    metaNASPtr->meta.header.lifetime = lifetime_days;
    bool bRet = _storage->PutMeta(_filename, (char*) (metaNASPtr.data()), sizeof(MetaTMPNAS),
            META_TMP_FILE);
    if (!bRet) {
        return false;
    }

    return true;
}

/**
 * Copy
 *
 * @author          weibing
 * @version                0.1.4
 * @param       new_name is the name of this file copy.
 *              cache_lv is the cache level of file copy.
 * @func                copy this file to cache_lv.
 * @return                return true if copy successful, else return false.
 */
bool FileNAS::Copy(const std::string& new_name, CacheLevel cache_lv) {

    //ice-----------
    if (new_name.compare(GetName()) == 0) {
        GCACHE_tmp_errno = GCACHE_TMP_ERR_PATH;
        return false;
    }
    //ice-----------

    /*determine whether the path of new_name exists, if the path doesn't exist return false*/
    int pos = new_name.find_last_of('/');
    std::string dirPath = new_name.substr(0, pos + 1);
    StorageArch s_arch = STORAGE_NAS;
    ScopedPointer<Dir> dir(Dir::New(dirPath, s_arch));
    if (dir == NULL || s_arch != STORAGE_NAS) {
        return false;
    }
    if (!dir->Exist()) {
        GCACHE_tmp_errno = GCACHE_TMP_ERR_NOT_EXIST;
        return false;
    }

    ScopedPointer<MetaTMPNAS> metaNASPtr(GetMeta());
    ScopedPointer<Reader> reader(OpenReader(FADV_NONE));
    if (NULL == reader) {
        return false;
    }

    if (metaNASPtr == NULL) {
        return false;
    }

    ScopedPointer<File> file_new(File::New(new_name, s_arch));
    if (file_new == NULL || s_arch != STORAGE_NAS) {
        return false;
    }
    if (NULL == file_new) {
        return false;
    }
    file_new->Remove();
    ScopedPointer<Writer> writer(
            file_new->OpenWriter(metaNASPtr->meta.header.lifetime, cache_lv, FADV_NONE));
    if (NULL == writer) {
        return false;
    }
    writer->Truncate(0);
    writer->Seek(0, SEEK_SET);

    int64_t size = Size();
    int buf_size;
    int64_t max_buf_size = 1;
    max_buf_size <<= 30; //1G
    if (size > max_buf_size) {
        buf_size = max_buf_size;
    } else {
        buf_size = size;
    }
    ScopedPointer<char> buf(new char[buf_size]);
    int64_t left_size = size;

    while (left_size > 0) {
        int read_size = left_size > buf_size ? buf_size : left_size;
        int64_t iRet = reader->Read(buf.data(), read_size);
        if (iRet != read_size) {
            return false;
        }

        iRet = writer->Write(buf.data(), read_size);
        if (iRet != read_size) {
            return false;
        }

        left_size -= read_size;
    }

    return true;
}

/**
 * Copy
 *
 * @author          weibing
 * @version                0.1.4
 * @param       newName is the name of this file copy.
 * @func                copy this file to the same cache level.
 * @return                return true if copy successful, else return false.
 */
bool FileNAS::Copy(const std::string& newName) {
    //ice-----------
    if (newName.compare(GetName()) == 0) {
        GCACHE_tmp_errno = GCACHE_TMP_ERR_PATH;
        return false;
    }
    //ice-----------

    char *meta_src_ptr;
    int meta_size;
    if (!_storage->GetMeta(_filename, meta_src_ptr, meta_size, META_TMP_FILE)) {
        return false;
    }
    MetaTMPNAS *meta_src = (MetaTMPNAS*) meta_src_ptr;
    CacheLevel cache_src = _storage->GetCacheLevel(meta_src->meta.dirID);

    delete meta_src;
    return Copy(newName, cache_src);
}

/**
 * Rename
 *
 * @author          weibing
 * @version                0.1.4
 * @param       new_name is the new name of this file.
 * @func                rename the file old name to new name.
 * @return                return true if rename successful, else return false.
 */
bool FileNAS::Rename(const std::string &new_name) {

    //ice-----------
    if (new_name.compare(GetName()) == 0) {
        return true;
    }
    //ice-----------

    ScopedPointer<MetaTMPNAS> metaNASPtr(GetMeta());
    if (metaNASPtr == NULL) {
        return false;
    }

    std::string rootDir = _storage->GetRootDir(metaNASPtr->meta.dirID);
    std::string oldRealName = GetRealFilename();
    std::string newRealName = rootDir + "/" + new_name;

    int iRet = rename(oldRealName.c_str(), newRealName.c_str());
    if (-1 == iRet) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RENAME, errno);
        return false;
    }

    bool bRet = _storage->RenameMeta(_filename, new_name, META_TMP_FILE);
    if (!bRet) {
        return false;
    }

    _filename = new_name;
    return true;
}

/**
 * Link
 *
 * @author          weibing
 * @version                0.1.4
 * @param       link_name is the link name of this file.
 * @func                make a new hard link to the existing file named by old name.
 * @return                return true if link successful, else return false.
 */
bool FileNAS::Link(const std::string &link_name) {
    ScopedPointer<MetaTMPNAS> metaNASPtr(GetMeta());
    if (NULL == metaNASPtr) {
        return false;
    }

    std::string rootDir = _storage->GetRootDir(metaNASPtr->meta.dirID);
    std::string oldRealName = GetRealFilename();
    std::string linkRealName = rootDir + "/" + link_name;
    int iRet = link(oldRealName.c_str(), linkRealName.c_str());                          //hard link
    if (iRet == -1) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_LINK, errno);
        return false;
    }

    bool bRet = _storage->PutMeta(link_name, (char*) metaNASPtr.data(), sizeof(MetaTMPNAS),
            META_TMP_FILE);
    if (!bRet) {
        return false;
    }

    return true;
}

/**
 * LastModified
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                get the time of last modification, in milliseconds.
 * @return                return the time of last modification, if an error occurs, return 0.
 */
uint64_t FileNAS::LastModified() const {
    struct stat f_stat;
    std::string realFilename = GetRealFilename();
    int iRet = stat(realFilename.c_str(), &f_stat);
    if (-1 == iRet) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
        return 0;
    }
    uint64_t time_ms = f_stat.st_mtim.tv_nsec / 1000000 + f_stat.st_mtim.tv_sec * 1000;
    return time_ms;
}

/**
 * GetName
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                get the name of this file.
 * @return                return the name of this file.
 */
std::string FileNAS::GetName() const {
    return _filename;
}

/**
 * LifetimeExpired
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                determine whether this file is expired or not.
 * @return                if this file is expired then return true, else return false.
 */
bool FileNAS::LifetimeExpired() const {
    struct timeval now;
    gettimeofday(&now, NULL);

    uint64_t time_now_ms = now.tv_sec * 1000 + now.tv_usec / 1000;

    struct stat f_stat;
    std::string real_filename = GetRealFilename();
    int iRet = stat(real_filename.c_str(), &f_stat);
    if (-1 == iRet) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
        return 0;
    }
    uint64_t time_last_accessed_ms = f_stat.st_atim.tv_nsec / 1000000
            + f_stat.st_atim.tv_sec * 1000;
    uint64_t time_last_modified_ms = f_stat.st_mtim.tv_nsec / 1000000
            + f_stat.st_mtim.tv_sec * 1000;

    uint64_t time_last =
            time_last_accessed_ms > time_last_modified_ms ?
                    time_last_accessed_ms : time_last_modified_ms;

    lifetime_t lifetime = Lifetime();
    uint64_t lifetime_ms = lifetime * 24 * 3600;
    lifetime_ms *= 1000;
    if (time_now_ms - time_last > lifetime_ms) {
        return true;
    } else {
        GCACHE_tmp_errno = GCACHE_TMP_ERR_LIFETIME;
        return false;
    }
}

/**
 * Lock
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                set lock.
 * @return      no return
 */
void FileNAS::Lock() {

}

/**
 * Unlock
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                set unlock.
 * @return      no return
 */
void FileNAS::Unlock() {

}

/**
 * Init
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                initialize member.
 * @return      if initialize successfully return true, else return false.
 */
bool FileNAS::Init() {
    _storage = Storage::GetInstance(STORAGE_NAS);
    if (_storage == NULL) {
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
 * @func                get the meta data of this file.
 * @return      return the pointer of meta data object, if an error occurs, return NULL.
 */
MetaTMPNAS *FileNAS::GetMeta() const {

    char* metaNasData = NULL;
    int size = 0;

    bool bRet = _storage->GetMeta(_filename, metaNasData, size, META_TMP_FILE);
    if (!bRet) {
        return NULL;
    }
    return (MetaTMPNAS*) metaNasData;
}

/**
 * GetRealFilename
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                get the real file name of this file, real file name contains root directory and file name.
 * @return      return real file name, if an error occurs, return "".
 */
std::string FileNAS::GetRealFilename() const {

    ScopedPointer<MetaTMPNAS> metaNasPtr(GetMeta());
    if (NULL == metaNasPtr) {
        return "";
    }

    std::string rootDir = _storage->GetRootDir(metaNasPtr->meta.dirID);
    std::string realFilename = rootDir + "/" + _filename;

    return realFilename;
}

NAMESPACE_END_SEISFS
}

