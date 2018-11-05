/*
 * gcache_tmp_file_local.cpp
 *
 *  Created on: Dec 29, 2015
 *      Author: wyd
 */

#include "gcache_tmp_file_local.h"

#include <sys/mman.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <cerrno>
#include <ctime>

#include "seisfs_meta.h"
#include "gcache_scoped_pointer.h"
#include "gcache_tmp_dir.h"
#include "gcache_tmp_error.h"
#include "gcache_tmp_meta.h"
#include "gcache_tmp_storage.h"
#include "gcache_tmp_meta_local.h"
#include "gcache_tmp_reader_local.h"
#include "gcache_tmp_writer_local.h"
#include "gcache_log.h"

//extern int GCACHE_errno;
NAMESPACE_BEGIN_SEISFS
namespace tmp {
FileLocal::FileLocal(const std::string &file_name) {
    _filename = file_name;
    _storage = NULL;
    boost::shared_ptr<bool> isWriterAlive(new bool(false));
    _isWriterAlive = isWriterAlive;
}
FileLocal::~FileLocal() {

}

Reader *FileLocal::OpenReader(FAdvice advice) { // open fd and pass it to the reader

    ScopedPointer<MetaTMPLocal> meta(GetMeta());
    if (NULL == meta) return NULL;

    int dirID = meta->meta.dirID;
    std::string rootDir = _storage->GetRootDir(dirID);
    std::string realFilename = rootDir + "/" + _filename;

    ReaderLocal *readerLocal = NULL;
    int iRet;

    int OPEN_FLAG = O_RDONLY;
    if (advice == FADV_RAW) OPEN_FLAG |= O_DIRECT;

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
            Err("the advice is wrong, please input the right value\n");
            GCACHE_tmp_errno = GCACHE_TMP_ERR_NOT_SUPPORT;
            return NULL;
            break;
    }

    readerLocal = new ReaderLocal(fd, direct_read, (char*) map_buf);
    if (readerLocal->Init()) return readerLocal;
    else return NULL;

}

Writer *FileLocal::OpenWriter(lifetime_t lifetime_days, CacheLevel cache_lv, FAdvice advice) {

    if (*_isWriterAlive) {
        Err("can not create a new writer, the old writer is still alive\n");
        GCACHE_tmp_errno = GCACHE_TMP_ERR_NOT_SUPPORT;
        return NULL;
    }

    lifetime_days = lifetime_days > 30 ? 30 : lifetime_days; // not bigger than 1 month

    int pos = _filename.find_last_of('/');
    std::string dirPath = _filename.substr(0, pos + 1);
    StorageArch s_arch = STORAGE_LOCAL;
    ScopedPointer<Dir> dir(Dir::New(dirPath, s_arch));
    if (dir == NULL || s_arch != STORAGE_LOCAL) {
        return NULL;
    }

    if (!dir->Exist()) {
        if (!dir->MakeDirs()) {
            return NULL;
        }
    }

    ScopedPointer<MetaTMPLocal> meta(GetMeta());
    WriterLocal *writerLocal = NULL;

    int OPEN_FLAG = O_WRONLY | O_CREAT;
    if (advice == FADV_RAW) {
        Err("the advice is wrong, please input the right value\n");
        GCACHE_tmp_errno = GCACHE_TMP_ERR_NOT_SUPPORT;
        return NULL;
    }
    CacheLevel real_cache_lv = _storage->GetRealCacheLevel(cache_lv);
    std::string full_filename;

    if (meta == NULL) { //file does not exist, write to new file
        Warn("file %s does not exist, write to new file\n", _filename.c_str());
        ScopedPointer<MetaTMPLocal> meta_new(new MetaTMPLocal());
        meta_new->meta.dirID = _storage->ElectRootDir(real_cache_lv);
        if (_storage->GetRootFreeSpace(meta_new->meta.dirID) <= 0) {
            Err("can not create a new writer, there is no space for new file\n");
            GCACHE_tmp_errno = GCACHE_TMP_ERR_SPACE_FULL;
            return NULL;
        }
        meta_new->meta.header.lifetime = lifetime_days;
        bool bRet = _storage->PutMeta(_filename, (char*) (meta_new.data()), sizeof(MetaTMPLocal),
                META_TMP_FILE);
        if (!bRet) {
            return NULL;
        }

        std::string rootDir = _storage->GetRootDir(meta_new->meta.dirID);
        full_filename = rootDir + "/" + _filename;

    } else { //file exist

        if (LifetimeExpired()) { // lifetime expired

            Warn("file exist, but lifetime is expired, write to a new file\n");

            if (!Remove()) return NULL;

            ScopedPointer<MetaTMPLocal> meta_new(new MetaTMPLocal());
            meta_new->meta.dirID = _storage->ElectRootDir(real_cache_lv);
            if (_storage->GetRootFreeSpace(meta_new->meta.dirID) <= 0) {
                Err("can not create a new writer, there is no space for new file\n");
                GCACHE_tmp_errno = GCACHE_TMP_ERR_SPACE_FULL;
                return NULL;
            }
            meta_new->meta.header.lifetime = lifetime_days;
            bool bRet = _storage->PutMeta(_filename, (char*) (meta_new.data()),
                    sizeof(MetaTMPLocal), META_TMP_FILE);
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
                bool bRet = _storage->PutMeta(_filename, (char*) (meta.data()),
                        sizeof(MetaTMPLocal), META_TMP_FILE);
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
    if (ret_off == (off_t) -1) return NULL;
    writerLocal = new WriterLocal(fd, _isWriterAlive);

    return writerLocal;

}

int64_t FileLocal::Size() const {
    std::string realFilename = GetRealFilename();
    if (realFilename == "") return -1;

    struct stat f_stat;
    int iRet = stat(realFilename.c_str(), &f_stat);
    if (iRet == -1) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
        return -1;
    }
    int64_t fileSize = f_stat.st_size;
    return fileSize;
}
lifetime_t FileLocal::Lifetime() const {
    ScopedPointer<MetaTMPLocal> metaLocalPtr(GetMeta());
    if (NULL == metaLocalPtr) return -1;
    return metaLocalPtr->meta.header.lifetime;
}

bool FileLocal::Exists() const {

    std::string realFilename = GetRealFilename();
    if (realFilename == "") return false;

    int iRet = access(realFilename.c_str(), F_OK);
    if (iRet == -1) return false;
    else return true;
}
bool FileLocal::Remove() {
    std::string realFilename = GetRealFilename();
    if (realFilename == "") return false;

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
bool FileLocal::Truncate(int64_t size) {
    std::string realFilename = GetRealFilename();
    if (realFilename == "") return false;

    int iRet = truncate(realFilename.c_str(), size);
    if (iRet == -1) {
        return false;
    }
    return true;
}

bool FileLocal::SetLifetime(lifetime_t lifetime_days) {
    lifetime_days = lifetime_days > 30 ? 30 : lifetime_days; // not bigger than 1 month

    ScopedPointer<MetaTMPLocal> metaLocalPtr(GetMeta());

    if (NULL == metaLocalPtr) return false;

    metaLocalPtr->meta.header.lifetime = lifetime_days;
    bool bRet = _storage->PutMeta(_filename, (char*) (metaLocalPtr.data()), sizeof(MetaTMPLocal),
            META_TMP_FILE);
    if (!bRet) {
        return false;
    }

    return true;

}

bool FileLocal::Copy(const std::string &new_name, CacheLevel cache_lv) {

    //ice-----------
    if (new_name.compare(GetName()) == 0) {
        GCACHE_tmp_errno = GCACHE_TMP_ERR_PATH;
        return false;
    }
    //ice-----------

    /*determine whether the path of new_name exists, if the path doesn't exist return false*/
    int pos = new_name.find_last_of('/');
    std::string dirPath = new_name.substr(0, pos + 1);
    StorageArch s_arch = STORAGE_LOCAL;
    ScopedPointer<Dir> dir(Dir::New(dirPath, s_arch));
    if (dir == NULL || s_arch != STORAGE_LOCAL) {
        return false;
    }
    if (!dir->Exist()) {
        GCACHE_tmp_errno = GCACHE_TMP_ERR_NOT_EXIST;
        return false;
    }

    ScopedPointer<MetaTMPLocal> metaLocalPtr(GetMeta());
    ScopedPointer<Reader> reader(OpenReader(FADV_NONE));
    if (NULL == reader) return false;

    ScopedPointer<File> file_new(File::New(new_name, s_arch));
    if (NULL == file_new || s_arch != STORAGE_LOCAL) {
        return false;
    }
    file_new->Remove();
    ScopedPointer<Writer> writer(
            file_new->OpenWriter(metaLocalPtr->meta.header.lifetime, cache_lv, FADV_NONE));
    if (NULL == writer) return false;
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
            GCACHE_tmp_errno = GCACHE_TMP_ERR_READ;
            return false;
        }

        iRet = writer->Write(buf.data(), read_size);
        if (iRet != read_size) {
            GCACHE_tmp_errno = GCACHE_TMP_ERR_WRITE;
            return false;
        }

        left_size -= read_size;
    }

    return true;
}

bool FileLocal::Copy(const std::string &newName) {

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
    MetaTMPLocal *meta_src = (MetaTMPLocal*) meta_src_ptr;
    CacheLevel cache_src = _storage->GetCacheLevel(meta_src->meta.dirID);

    delete meta_src;
    return Copy(newName, cache_src);
}

bool FileLocal::Rename(const std::string &new_name) {

    //ice-----------
    if (new_name.compare(GetName()) == 0) {
        return true;
    }
    //ice-----------

    ScopedPointer<MetaTMPLocal> metaLocalPtr(GetMeta());
    if (metaLocalPtr == NULL) {
        return false;
    }

    std::string rootDir = _storage->GetRootDir(metaLocalPtr->meta.dirID);
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

bool FileLocal::Link(const std::string &link_name) {

    ScopedPointer<MetaTMPLocal> metaLocalPtr(GetMeta());
    if (NULL == metaLocalPtr) {
        return false;
    }

    std::string rootDir = _storage->GetRootDir(metaLocalPtr->meta.dirID);
    std::string oldRealName = GetRealFilename();
    std::string linkRealName = rootDir + "/" + link_name;
    int iRet = link(oldRealName.c_str(), linkRealName.c_str());
    if (iRet == -1) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_LINK, errno);
        return false;
    }

    bool bRet = _storage->PutMeta(link_name, (char*) metaLocalPtr.data(), sizeof(MetaTMPLocal),
            META_TMP_FILE);
    if (!bRet) {
        return false;
    }

    return true;
}

uint64_t FileLocal::LastModified() const {
    struct stat f_stat;
    std::string realFilename = GetRealFilename();
    int iRet = stat(realFilename.c_str(), &f_stat);
    if (-1 == iRet) {
        GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
        return 0;
    }
    uint64_t time_ns = f_stat.st_mtim.tv_nsec / 1000000 + f_stat.st_mtim.tv_sec * 1000;
    return time_ns;
}
std::string FileLocal::GetName() const {
    return _filename;
}

bool FileLocal::LifetimeExpired() const {
    struct timeval now;
    gettimeofday(&now, NULL);

    uint64_t time_now_ms = now.tv_sec * 1000 + now.tv_usec / 1000;

//        uint64_t time_last_accessed_ms = _storage->LastAccessed(_filename, META_TMP_FILE);
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
    if (time_now_ms - time_last > lifetime_ms) return true;
    else return false;
}

void FileLocal::Lock() {

}
void FileLocal::Unlock() {

}

bool FileLocal::Init() {
    _storage = Storage::GetInstance(STORAGE_LOCAL);
    if (_storage == NULL) {
        GCACHE_tmp_errno = GCACHE_TMP_ERR_STORAGE;
        return false;
    }
    return true;
}

MetaTMPLocal *FileLocal::GetMeta() const { //(read meta every time)

    char* metaLocalData = NULL;
    int size = 0;

    bool bRet = _storage->GetMeta(_filename, metaLocalData, size, META_TMP_FILE);
    if (!bRet) return NULL;
    return (MetaTMPLocal*) metaLocalData;
}

std::string FileLocal::GetRealFilename() const {

    ScopedPointer<MetaTMPLocal> metaLocalPtr(GetMeta());
    if (NULL == metaLocalPtr) {
        return "";
    }

    std::string rootDir = _storage->GetRootDir(metaLocalPtr->meta.dirID);
    std::string realFilename = rootDir + "/" + _filename;

    return realFilename;

}

NAMESPACE_END_SEISFS
}
