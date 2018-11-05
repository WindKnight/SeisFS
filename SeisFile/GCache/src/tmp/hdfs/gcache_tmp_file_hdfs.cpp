/*
 * gcache_tmp_file_hdfs.cpp
 *
 *  Created on: Dec 29, 2015
 *      Author: wb
 */

#include <fcntl.h>
#include <sys/time.h>
#include <cerrno>
#include <cstdio>

#include "gcache_tmp_file_hdfs.h"
#include "seisfs_meta.h"
#include "gcache_scoped_pointer.h"
#include "gcache_tmp_dir.h"
#include "gcache_tmp_error.h"
#include "gcache_tmp_meta.h"
#include "gcache_tmp_storage.h"
#include "gcache_tmp_storage_hdfs.h"
#include "gcache_tmp_meta_hdfs.h"
#include "gcache_hdfs_utils.h"
#include "gcache_log.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {

/**
 * constructor
 *
 * @author          weibing
 * @version                0.1.4
 * @param                file_name is the name of this file
 * @func                set member variable to initial values.
 * @return                no return
 */
FileHDFS::FileHDFS(const std::string &file_name) {
    _filename = file_name;
    _storage = NULL;
    _rootFS = NULL;
    boost::shared_ptr<bool> isWriterAlive(new bool(false));
    _isWriterAlive = isWriterAlive;
    boost::shared_ptr<std::string> sharedFileName(new std::string(""));
    _sharedFileName = sharedFileName;
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
FileHDFS::~FileHDFS() {

}

/**
 * OpenReader
 *
 * @author          weibing
 * @version                0.1.4
 * @param                advice is the advice indicates what way to read.
 * @func                set the ReaderHDFS object according to advice,then return the pointer of object.
 * @return                return the pointer of ReaderHDFS object, if an error occurs, return NULL.
 */
ReaderHDFS* FileHDFS::OpenReader(FAdvice advice) {

    /*get real file name*/
    ScopedPointer<MetaTMPHDFS> meta(GetMeta());
    if (NULL == meta) {
        return NULL;
    }
    int dirID = meta->meta.dirID;
    std::string rootDir = _storage->GetRootDir(dirID);
    std::string realFilename = rootDir + "/" + _filename;

    ReaderHDFS *readerHDFS = NULL;
    std::string fileHDFSPath = hdfsGetPath(realFilename);
    hdfsFS fs = GetRootFS(rootDir);
    hdfsFile fd = hdfsOpenFile(fs, fileHDFSPath.c_str(), O_RDONLY, 0, REPLICATION, 0);
    if (fd == NULL) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_OPFILE, errno);
        return NULL;
    }

    *_sharedFileName = fileHDFSPath.c_str();
    readerHDFS = new ReaderHDFS(fd, fs, _isWriterAlive, _sharedFileName);
    if (readerHDFS->Init()) {
        return readerHDFS;
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
 * @func                get the file descriptor then use it to construct a WriterHDFS object,then return the pointer of object.
 * @return                return the pointer of WriterHDFS object, if an error occurs, return NULL.
 */
WriterHDFS* FileHDFS::OpenWriter(lifetime_t lifetime_days, CacheLevel cache_lv, FAdvice advice) {
    lifetime_days = lifetime_days > 30 ? 30 : lifetime_days; // not bigger than 1 month
    if (*_isWriterAlive) {
        Err("can not create a new writer, the old writer is still alive\n");
        errno = GCACHE_TMP_ERR_NOT_SUPPORT;
        return NULL;
    }

    int pos = _filename.find_last_of('/');
    std::string dirPath = _filename.substr(0, pos + 1);
    StorageArch s_arch = STORAGE_DIST;
    ScopedPointer<Dir> dir(Dir::New(dirPath, s_arch));
    if (dir == NULL || s_arch != STORAGE_DIST) {
        return NULL;
    }

    if (!dir->Exist()) {
        if (!dir->MakeDirs()) {
            return NULL;
        }
    }

    ScopedPointer<MetaTMPHDFS> meta(GetMeta());
    WriterHDFS *writerHDFS = NULL;

    CacheLevel real_cache_lv = _storage->GetRealCacheLevel(cache_lv);
    std::string full_filename;
    ScopedPointer<MetaTMPHDFS> meta_new(new MetaTMPHDFS());
    if (meta == NULL) { //file does not exist, write to new file
        Warn("file %s does not exist, write to new file\n", _filename.c_str());
        meta_new->meta.dirID = _storage->ElectRootDir(real_cache_lv);
        if (_storage->GetRootFreeSpace(meta_new->meta.dirID) <= 0) {
            Err("can not create a new writer, there is no space for new file\n");
            errno = GCACHE_TMP_ERR_SPACE_FULL;
            return NULL;
        }
        meta_new->meta.header.lifetime = lifetime_days;

        bool bRet = _storage->PutMeta(_filename, (char*) (meta_new.data()), sizeof(MetaTMPHDFS),
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

            meta_new->meta.dirID = _storage->ElectRootDir(real_cache_lv);
            if (_storage->GetRootFreeSpace(meta_new->meta.dirID) <= 0) {
                Err("can not create a new writer, there is no space for new file\n");
                errno = GCACHE_TMP_ERR_SPACE_FULL;
                return NULL;
            }
            meta_new->meta.header.lifetime = lifetime_days;

            bool bRet = _storage->PutMeta(_filename, (char*) (meta_new.data()), sizeof(MetaTMPHDFS),
                    META_TMP_FILE);
            if (!bRet) {
                return NULL;
            }

            std::string rootDir = _storage->GetRootDir(meta_new->meta.dirID);
            full_filename = rootDir + "/" + _filename;
        } else { // file is valid

            int old_dirID = meta->meta.dirID;
            if (_storage->GetRootFreeSpace(old_dirID) <= 0) {
                errno = GCACHE_TMP_ERR_SPACE_FULL;
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
                bool bRet = _storage->PutMeta(_filename, (char*) (meta.data()), sizeof(MetaTMPHDFS),
                        META_TMP_FILE);
                if (!bRet) {
                    return NULL;
                }

                std::string new_root_dir = _storage->GetRootDir(meta->meta.dirID);
                full_filename = new_root_dir + "/" + _filename;

                std::string oldFileHDFSPath = hdfsGetPath(old_full_filename);
                std::string fullFileHDFSPath = hdfsGetPath(full_filename);

                hdfsFS fs = GetRootFS(old_root_dir);
                int rn_ret = hdfsRename(fs, oldFileHDFSPath.c_str(), fullFileHDFSPath.c_str());

                if (0 > rn_ret) {
                    errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RENAME, errno);
                    return NULL;
                }
            }
        }
    }

    std::string root_dir = GetRootDirectory();
    hdfsFS fs = GetRootFS(root_dir);

    std::string hdfsfilename = hdfsGetPath(full_filename);
    hdfsFile fd = hdfsOpenFile(fs, hdfsfilename.c_str(), O_WRONLY | O_APPEND, 0, REPLICATION, 0); // 修改备份的数量
    hdfsChmod(fs, hdfsfilename.c_str(), 0644);
    if (fd == NULL) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_OPFILE, errno);
        return NULL;
    }

    writerHDFS = new WriterHDFS(fs, fd, _isWriterAlive);
    return writerHDFS;
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
int64_t FileHDFS::Size() const {

    std::string realFilename = GetRealFilename();
    if (realFilename == "") {
        return -1;
    }

    std::string hdfsfilename = hdfsGetPath(realFilename);
    std::string root_dir = GetRootDirectory();
    hdfsFS fs = GetRootFS(root_dir);
    hdfsFileInfo * fileInfo = hdfsGetPathInfo(fs, hdfsfilename.c_str());
    if (fileInfo == NULL) {
        return -1;
    }
    int64_t fileSize = fileInfo->mSize;

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
lifetime_t FileHDFS::Lifetime() const {
    ScopedPointer<MetaTMPHDFS> metaHDFSPtr(GetMeta());
    if (NULL == metaHDFSPtr) {
        return -1;
    }
    return metaHDFSPtr->meta.header.lifetime;
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
bool FileHDFS::Exists() const {
    std::string realFilename = GetRealFilename();
    if (realFilename == "") {
        return false;
    }
    std::string realFilenameHDFSPath = hdfsGetPath(realFilename);
    std::string root_dir = GetRootDirectory();
    hdfsFS fs = GetRootFS(root_dir);
    int iRet = hdfsExists(fs, realFilenameHDFSPath.c_str());
    if (iRet == -1) {
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
bool FileHDFS::Remove() {
    std::string realFilename = GetRealFilename();
    if (realFilename == "") {
        return false;
    }

    std::string realFilenameHDFSPath = hdfsGetPath(realFilename);
    std::string root_dir = GetRootDirectory();
    hdfsFS fs = GetRootFS(root_dir);
    int iRet = hdfsExists(fs, realFilenameHDFSPath.c_str());
    if (iRet == -1) {
        return true;
    }
    iRet = hdfsDelete(fs, hdfsGetPath(realFilename).c_str(), 0);
    if (iRet == -1) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RMFILE, errno);
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
bool FileHDFS::Truncate(int64_t size) {
    if (*_isWriterAlive) {
        Err("when the writer is alive Truncate() function is not supported in hdfs\n");
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_NOT_SUPPORT, errno);
        return false;
    }

    std::string realFilename = GetRealFilename();
    if (realFilename == "") {
        return false;
    }

    int shouldWait;
    std::string hdfsFilename = hdfsGetPath(realFilename);
    std::string root_dir = GetRootDirectory();
    hdfsFS fs = GetRootFS(root_dir);
    if (hdfsFilename.length() == 0) {
        return false;
    }

    int iRet = hdfsTruncate(fs, hdfsFilename.c_str(), size, &shouldWait);
    if (iRet == -1) {
        return false;
    }

    //if truncate a file, we have to wait a few seconds to open it for appending
    int seconds = 20;
    int roll;
    for (roll = 0; roll < seconds; roll++) {
        hdfsFile wfd = hdfsOpenFile(fs, hdfsFilename.c_str(), O_WRONLY | O_APPEND, 0, REPLICATION,
                0);
        if (wfd == NULL) {
            sleep(1);
            continue;
        }

        if (hdfsCloseFile(fs, wfd) == -1) {
            errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_CLOSE, errno);
            return false;
        }
        break;
    }
    if (roll == seconds) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_OPFILE, errno);
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
bool FileHDFS::SetLifetime(lifetime_t lifetime_days) {
    lifetime_days = lifetime_days > 30 ? 30 : lifetime_days; // not bigger than 1 month

    ScopedPointer<MetaTMPHDFS> metaHDFSPtr(GetMeta());

    if (NULL == metaHDFSPtr) {
        return false;
    }

    metaHDFSPtr->meta.header.lifetime = lifetime_days;
    bool bRet = _storage->PutMeta(_filename, (char*) (metaHDFSPtr.data()), sizeof(MetaTMPHDFS),
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
bool FileHDFS::Copy(const std::string& new_name, CacheLevel cache_lv) {
    //ice-----------
    if (new_name.compare(GetName()) == 0) {
        errno = GCACHE_TMP_ERR_NOT_SUPPORT;
        return false;
    }
    //ice-----------

    if (*_isWriterAlive) {
        Err("when the writer is alive Truncate() function is not supported in hdfs\n");
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_NOT_SUPPORT, errno);
        return false;
    }

    /*determine whether the path of new_name exists, if the path doesn't exist return false*/
    int pos = new_name.find_last_of('/');
    std::string dirPath = new_name.substr(0, pos + 1);
    StorageArch s_arch = STORAGE_DIST;
    ScopedPointer<Dir> dir(Dir::New(dirPath, s_arch));
    if (dir == NULL || s_arch != STORAGE_DIST || !dir->Exist()) {
        return false;
    }

    ScopedPointer<MetaTMPHDFS> metaHDFSPtr(GetMeta());
    ScopedPointer<Reader> reader(OpenReader(FADV_NONE));
    if (NULL == reader) {
        return false;
    }
    if (metaHDFSPtr == NULL) {
        return false;
    }

    ScopedPointer<File> file_new(File::New(new_name, s_arch));
    if (NULL == file_new || s_arch != STORAGE_DIST) {
        return false;
    }
    file_new->Remove();
    ScopedPointer<Writer> writer(
            file_new->OpenWriter(metaHDFSPtr->meta.header.lifetime, cache_lv, FADV_NONE));
    if (NULL == writer) {
        return false;
    }

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
bool FileHDFS::Copy(const std::string& newName) {
    //ice-----------
    if (newName.compare(GetName()) == 0) {
        errno = GCACHE_TMP_ERR_NOT_SUPPORT;
        return false;
    }
    //ice-----------

    char *meta_src_ptr;
    int meta_size;
    if (!_storage->GetMeta(_filename, meta_src_ptr, meta_size, META_TMP_FILE)) {
        return false;
    }
    MetaTMPHDFS *meta_src = (MetaTMPHDFS*) meta_src_ptr;
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
bool FileHDFS::Rename(const std::string &new_name) {

    //ice-------------
    if (new_name.compare(GetName()) == 0) {
        return true;
    }
    //ice------------
    StorageArch s_arch = STORAGE_DIST;
    File *file = File::New(new_name, s_arch);
    if (file == NULL || s_arch != STORAGE_DIST) {
        return false;
    }
    if (file->Exists()) {
        if (file->Remove() == false) {
            delete file;
            return false;
        }
    }
    delete file;
    //ice-------------

    ScopedPointer<MetaTMPHDFS> metaHDFSPtr(GetMeta());
    if (metaHDFSPtr == NULL) {
        return false;
    }

    std::string rootDir = _storage->GetRootDir(metaHDFSPtr->meta.dirID);
    std::string oldRealName = GetRealFilename();
    std::string newRealName = rootDir + "/" + new_name;
    std::string oldRealNameHDFSPath = hdfsGetPath(oldRealName);
    std::string newRealNameHDFSPath = hdfsGetPath(newRealName);

    std::string root_dir = GetRootDirectory();
    hdfsFS fs = GetRootFS(root_dir);
    int iRet = hdfsRename(fs, oldRealNameHDFSPath.c_str(), newRealNameHDFSPath.c_str());
    if (-1 == iRet) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RENAME, errno);
        return false;
    }

    bool bRet = _storage->RenameMeta(_filename, new_name, META_TMP_FILE);
    if (!bRet) {
        return false;
    }

    _filename = new_name;
    *_sharedFileName = newRealNameHDFSPath.c_str();
    return true;
}

/**
 * Link
 *
 * @author          weibing
 * @version                0.1.4
 * @param       link_name is the link name of this file.
 * @func                this function is not supported now.
 * @return                return true if link successful, else return false.
 */
bool FileHDFS::Link(const std::string &link_name) {
    Err("Link function is not supported in HDFS now\n");
    errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_NOT_SUPPORT, errno);
    return false;
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
uint64_t FileHDFS::LastModified() const {
    std::string realFilename = GetRealFilename();
    if (realFilename == "") {
        return 0;
    }

    std::string realFileNameHDFSPath = hdfsGetPath(realFilename);
    std::string root_dir = GetRootDirectory();
    hdfsFS fs = GetRootFS(root_dir);
    hdfsFileInfo * fileInfo = hdfsGetPathInfo(fs, realFileNameHDFSPath.c_str());
    if (fileInfo == NULL) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
        return 0;
    }
    uint64_t time_ms = fileInfo->mLastAccess * 1000;
    hdfsFreeFileInfo(fileInfo, 1);
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
std::string FileHDFS::GetName() const {
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
bool FileHDFS::LifetimeExpired() const {
    struct timeval now;
    gettimeofday(&now, NULL);

    int64_t time_now_ms = now.tv_sec * 1000 + now.tv_usec / 1000;
    ScopedPointer<MetaTMPHDFS> meta(GetMeta());
    if (meta == NULL) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_NOT_EXIST, errno);
        return false;
    }

    std::string realFilename = GetRealFilename();
    if (realFilename == "") {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
        return false;
    }

    std::string realFileNameHDFSPath = hdfsGetPath(realFilename);
    std::string root_dir = GetRootDirectory();
    hdfsFS fs = GetRootFS(root_dir);
    hdfsFileInfo * fileInfo = hdfsGetPathInfo(fs, realFileNameHDFSPath.c_str());
    if (fileInfo == NULL) {
        errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
        return false;
    }
    int64_t time_last_accessed_ms = fileInfo->mLastAccess * 1000;
    hdfsFreeFileInfo(fileInfo, 1);

    lifetime_t lifetime = Lifetime();
    int64_t lifetime_ms = lifetime * 24 * 3600;
    lifetime_ms *= 1000;
    int64_t timespan = time_now_ms - time_last_accessed_ms;
    if (timespan > lifetime_ms) {
        return true;
    } else {
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
void FileHDFS::Lock() {

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
void FileHDFS::Unlock() {

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
bool FileHDFS::Init() {
    _storage = Storage::GetInstance(STORAGE_DIST);
    if (_storage == NULL) {
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
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                get the meta data of this file.
 * @return      return the pointer of meta data object, if an error occurs, return NULL.
 */
MetaTMPHDFS *FileHDFS::GetMeta() const {
    char* metaHDFSData = NULL;
    int size = 0;

    bool bRet = _storage->GetMeta(_filename, metaHDFSData, size, META_TMP_FILE);
    if (!bRet) {
        return NULL;
    }
    return (MetaTMPHDFS*) metaHDFSData;
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
std::string FileHDFS::GetRealFilename() const {
    std::string rootDir = GetRootDirectory();
    if (rootDir == "") {
        return "";
    }
    std::string realFilename = rootDir + "/" + _filename;

    return realFilename;
}

/**
 * GetRootDirectory
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                get the root directory of this file.
 * @return      return real file name, if an error occurs, return "".
 */
std::string FileHDFS::GetRootDirectory() const {
    ScopedPointer<MetaTMPHDFS> metaHDFSPtr(GetMeta());
    if (NULL == metaHDFSPtr) {
        return "";
    }
    std::string rootDir = _storage->GetRootDir(metaHDFSPtr->meta.dirID);
    return rootDir;
}

/**
 * GetRootFS
 *
 * @author          weibing
 * @version                0.1.4
 * @param       rootDir is the root directory of this file.
 * @func                get the hdfsFS by rootDir.
 * @return      return hdfsFS if find fs successfully, else return NULL.
 */
hdfsFS FileHDFS::GetRootFS(std::string rootDir) const {
    std::map<std::string, hdfsFS>::iterator it = _rootFS->find(rootDir);
    if (it == _rootFS->end()) {
        errno = GCACHE_TMP_ERR_NOT_SUPPORT;
        return NULL;
    }
    return it->second;
}

NAMESPACE_END_SEISFS
}
