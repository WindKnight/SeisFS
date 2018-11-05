/*
 * gcache_tmp_dir_local.cpp
 *
 *  Created on: Jan 8, 2016
 *      Author: wyd
 */

#include "gcache_tmp_dir_local.h"

#include <dirent.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cerrno>
#include <iterator>
#include <set>
#include <utility>

#include "gcache_io.h"
#include "gcache_log.h"
#include "gcache_scoped_pointer.h"
#include "gcache_string.h"
#include "gcache_tmp_error.h"
#include "gcache_tmp_file.h"
#include "gcache_tmp_meta.h"

//extern int GCACHE_errno;
NAMESPACE_BEGIN_SEISFS
namespace tmp {
DirLocal::~DirLocal() {

}

DirLocal::DirLocal(const std::string &path) :
        _path(path) {

}

bool DirLocal::Exist() {

    if (PartExist()) {
        MakeDirs();
        return true;
    } else {
        return false;
    }
}

int64_t DirLocal::GetTotalSpace(CacheLevel level) {
    int64_t total_space = 0;
    for (std::vector<Storage::DiskInfo>::iterator it = _cacheInfo[level].begin();
            it != _cacheInfo[level].end(); ++it) {
        total_space += _storage->GetRootTotalSpace(it->dirID);
    }
    return total_space;
}

int64_t DirLocal::GetFreeSpace(CacheLevel level) {

    int64_t free_space = 0;
    for (std::vector<Storage::DiskInfo>::iterator it = _cacheInfo[level].begin();
            it != _cacheInfo[level].end(); ++it) {
        free_space += _storage->GetRootFreeSpace(it->dirID);
    }
    return free_space;
}

bool DirLocal::MakeDirs() {
    for (std::map<int, Storage::DiskInfo>::iterator it = _rootDirs->begin(); it != _rootDirs->end();
            ++it) {
        std::string full_dir_path = (it->second).rootDir + _path;

        bool bRet = RecursiveMakeDir(full_dir_path);
        if (!bRet) return false;
    }
    if (!_storage->MakeMetaDir(_path)) return false;
    return true;
}

bool DirLocal::Remove(bool recursive) {
    if (!recursive) {
        if (!IsEmpty()) {
            GCACHE_tmp_errno = GCACHE_TMP_ERR_NOTEMPTY;
            return false;
        }
        for (std::map<int, Storage::DiskInfo>::iterator it = _rootDirs->begin();
                it != _rootDirs->end(); ++it) {
            std::string full_dir_path = (it->second).rootDir + _path;
            if (0 == access(full_dir_path.c_str(), F_OK)) {
                int iRet = rmdir(full_dir_path.c_str());
                if (iRet < 0) {
                    GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RMDIR, errno);
                    return false;
                }
            }
        }
    } else {

        for (std::map<int, Storage::DiskInfo>::iterator it = _rootDirs->begin();
                it != _rootDirs->end(); ++it) {
            std::string full_path = (it->second).rootDir + "/" + _path;
            if (0 == access(full_path.c_str(), F_OK)) {
                bool rm_ret = RecursiveRemove(full_path);
                if (!rm_ret) {
                    GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RMDIR, errno);
                    return false;
                }
            }
        }
    }

    if (!_storage->DeleteMeta(_path, META_TMP_FILE)) return false;

    return true;
}

bool DirLocal::Rename(const std::string& new_name) {

    ScopedPointer<DirLocal> dir_new(new DirLocal(new_name));
    dir_new->Init();
    if (dir_new->Exist() && (!dir_new->IsEmpty())) {
        GCACHE_tmp_errno = GCACHE_TMP_ERR_NOTEMPTY;
        return false;
    }
    for (std::map<int, Storage::DiskInfo>::iterator it = _rootDirs->begin(); it != _rootDirs->end();
            ++it) {
        std::string full_src_path = (it->second).rootDir + _path;
        std::string full_dest_path = (it->second).rootDir + new_name;

        if (0 > rename(full_src_path.c_str(), full_dest_path.c_str())) {
            GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RENAME, errno);
            return false;
        }
    }
    if (!_storage->RenameMeta(_path, new_name, META_TMP_FILE)) {
        return false;
    }
    _path = new_name;
    return true;

}

bool DirLocal::Move(const std::string& dest_dir) {

    if (!StartsWith(dest_dir, "/")) { // full path
        GCACHE_tmp_errno = GCACHE_TMP_ERR_PATH;
        return false;
    }
    std::vector<std::string> paths = SplitString(_path, "/");
    std::string cleanDirName = paths[paths.size() - 1];

    ScopedPointer<DirLocal> dirDest(new DirLocal(dest_dir));
    dirDest->Init();
    if (dirDest->Exist()) {
        std::string realDestPath = dest_dir + "/" + cleanDirName;
        ScopedPointer<DirLocal> dirRealDest(new DirLocal(realDestPath));
        dirRealDest->Init();
        if (dirRealDest->Exist() && (!dirRealDest->IsEmpty())) {
            GCACHE_tmp_errno = GCACHE_TMP_ERR_NOTEMPTY;
            return false;
        }

        if (!MoveToRealDest(realDestPath)) return false;
    } else {
        std::string realDestPath = dest_dir;

        if (!MoveToRealDest(realDestPath)) return false;
    }
    return true;
}

bool DirLocal::MoveToRealDest(const std::string &real_dest_path) {

#if 0
    bool rename_meta = true;
    if(this->IsEmpty()) {
        rename_meta = false;
    }
#endif

    for (std::map<int, Storage::DiskInfo>::iterator it = _rootDirs->begin(); it != _rootDirs->end();
            ++it) {
        std::string oldDirFullPath = (it->second).rootDir + _path;
        std::string destDirFullPath = (it->second).rootDir + real_dest_path;

        if (0 > rename(oldDirFullPath.c_str(), destDirFullPath.c_str())) {
            GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RENAME, errno);
            return false;
        }
    }

    if (!_storage->RenameMeta(_path, real_dest_path, META_TMP_FILE)) {
        return false;
    }

    return true;
}

bool DirLocal::Copy(const std::string& dest_dir) {
    if (!StartsWith(dest_dir, "/")) { // full path
        GCACHE_tmp_errno = GCACHE_TMP_ERR_PATH;
        return false;
    }

    Info("calling copy, from %s to %s\n", _path.c_str(), dest_dir.c_str());
    fflush(stdout);

    std::vector<std::string> paths = SplitString(_path, "/");
    std::string cleanDirName = paths[paths.size() - 1];

    ScopedPointer<DirLocal> dirDest(new DirLocal(dest_dir));
    dirDest->Init();
    if (dirDest->Exist()) {

        std::string realDestPath = dest_dir + "/" + cleanDirName;

        Warn("%s exists, real_dest_path = %s\n", dest_dir.c_str(), realDestPath.c_str());
        fflush(stdout);

        if (!RecursiveCopy(_path, realDestPath)) return false;

    } else { // dest not exist

        std::string realDestPath = dest_dir;

        Warn("%s does not exist, real_dest_path = %s\n", dest_dir.c_str(), realDestPath.c_str());
        fflush(stdout);

        std::vector<std::string> dest_paths = SplitString(dest_dir, "/");
        std::string dest_parent_path = "";
        for (unsigned int i = 0; i < dest_paths.size() - 1; ++i) {
            dest_parent_path = dest_parent_path + "/" + dest_paths[i];
        }
        ScopedPointer<DirLocal> dirDestParent(new DirLocal(dest_parent_path));
        dirDestParent->Init();
        if (!dirDestParent->Exist()) {
            GCACHE_tmp_errno = GCACHE_TMP_ERR_NOT_EXIST;
            return false;
        }

        if (!RecursiveCopy(_path, realDestPath)) return false;
    }
    return true;
}

bool DirLocal::RecursiveCopy(const std::string &real_src_path, const std::string &real_dest_path) {
//    printf("calling recursive copy, from %s to %s\n", real_src_path.c_str(),
//            real_dest_path.c_str());
    fflush(stdout);

    ScopedPointer<DirLocal> dirRealDest(new DirLocal(real_dest_path));
    dirRealDest->Init();
    if (!dirRealDest->MakeDirs()) {
        GCACHE_tmp_errno = GCACHE_TMP_ERR_MKDIR;
        return false;
    }

    ScopedPointer<DirLocal> dir_src(new DirLocal(real_src_path));
    dir_src->Init();
    if (dir_src->IsEmpty()) {
        ScopedPointer<DirLocal> dir_dest(new DirLocal(real_dest_path));
        dir_dest->Init();
        return dir_dest->MakeDirs();
    } else {
        std::list<std::string> child_dir_list, child_file_list;
        dir_src->ListFiles(child_file_list, "*");
        dir_src->ListDirs(child_dir_list, "*");

        for (std::list<std::string>::iterator it = child_file_list.begin();
                it != child_file_list.end(); ++it) {
            std::string src_child_file_path = real_src_path + "/" + *it;
            std::string dest_child_file_path = real_dest_path + "/" + *it;

            StorageArch s_arch = STORAGE_LOCAL;
            ScopedPointer<File> f_src(File::New(src_child_file_path, s_arch));
            if (f_src == NULL || s_arch != STORAGE_LOCAL || !f_src->Copy(dest_child_file_path)) {
                return false;
            }
        }

        for (std::list<std::string>::iterator it = child_dir_list.begin();
                it != child_dir_list.end(); ++it) {
            std::string src_child_dir_path = real_src_path + "/" + *it;
            std::string dest_child_dir_path = real_dest_path + "/" + *it;

            if (!RecursiveCopy(src_child_dir_path, dest_child_dir_path)) return false;
        }
    }
    return true;

}

bool DirLocal::ListFiles(std::list<std::string>& fileList, const std::string& wildcard) {
    std::set<std::string> file_set;

    for (std::map<int, Storage::DiskInfo>::iterator it = _rootDirs->begin(); it != _rootDirs->end();
            ++it) {
        std::string full_dir_path = (it->second).rootDir + _path;
        if (0 == access(full_dir_path.c_str(), F_OK)) {
            DIR *dir = opendir(full_dir_path.c_str());
            if (NULL == dir) {
                GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_OPDIR, errno);
                return false;
            }

            struct dirent entry;
            struct dirent *result;
            for (int iRet = readdir_r(dir, &entry, &result); iRet == 0 && result != NULL; iRet =
                    readdir_r(dir, &entry, &result)) {

                if (0 == strcmp(entry.d_name, ".") || 0 == strcmp(entry.d_name, "..")) {
                    continue;
                }

                std::string full_entry_name = full_dir_path + "/" + entry.d_name;
                struct stat stat_buf;
                int sRet = stat(full_entry_name.c_str(), &stat_buf);
                if (sRet < 0) {
                    GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
                    closedir(dir);
                    return false;
                }

                if (S_ISREG(stat_buf.st_mode)) {
                    if (WildcardMatch(wildcard.c_str(), entry.d_name, true)) {
                        file_set.insert(entry.d_name);
                    }
                }
            }
            closedir(dir);
        } // full dir exist
    }

    for (std::set<std::string>::iterator it = file_set.begin(); it != file_set.end(); ++it) {
        fileList.push_back(*it);
    }

    return true;
}

bool DirLocal::ListDirs(std::list<std::string>& dirList, const std::string& wildcard) {
    std::set<std::string> dir_set;
    for (std::map<int, Storage::DiskInfo>::iterator it = _rootDirs->begin(); it != _rootDirs->end();
            ++it) {
        std::string full_dir_path = (it->second).rootDir + _path;
        if (0 == access(full_dir_path.c_str(), F_OK)) {
            DIR *dir = opendir(full_dir_path.c_str());
            if (NULL == dir) {
                GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_OPDIR, errno);
                return false;
            }

            struct dirent entry;
            struct dirent *result;
            for (int iRet = readdir_r(dir, &entry, &result); iRet == 0 && result != NULL; iRet =
                    readdir_r(dir, &entry, &result)) {

                if (0 == strcmp(entry.d_name, ".") || 0 == strcmp(entry.d_name, "..")) {
                    continue;
                }

                std::string full_entry_name = full_dir_path + "/" + entry.d_name;
                struct stat stat_buf;
                int sRet = stat(full_entry_name.c_str(), &stat_buf);
                if (sRet < 0) {
                    GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
                    closedir(dir);
                    return false;
                }

                if (S_ISDIR(stat_buf.st_mode)) {
                    if (WildcardMatch(wildcard.c_str(), entry.d_name, true)) {
                        dir_set.insert(entry.d_name);
                    }
                }
            }
            closedir(dir);
        } // full dir exist
    }

    for (std::set<std::string>::iterator it = dir_set.begin(); it != dir_set.end(); ++it) {
        dirList.push_back(*it);
    }
    return true;
}

bool DirLocal::Init() {
    StorageArch s_arch = STORAGE_LOCAL;
    File *f_tmp = File::New(_path, s_arch);
    if (f_tmp == NULL || s_arch != STORAGE_LOCAL) {
        return false;
    }
    if (f_tmp->Exists()) {
        delete f_tmp;
        GCACHE_tmp_errno = GCACHE_TMP_ERR_NOTDIR;
        return false;
    }

    _storage = Storage::GetInstance(STORAGE_LOCAL);
    if (NULL == _storage) {
        //                delete *f_tmp;
        GCACHE_tmp_errno = GCACHE_TMP_ERR_STORAGE;
        return false;
    }
    _rootDirs = _storage->GetAllRootDir();
    _cacheInfo = _storage->GetCacheInfo();
    return true;
}

bool DirLocal::IsEmpty() {
    for (std::map<int, Storage::DiskInfo>::iterator it = _rootDirs->begin(); it != _rootDirs->end();
            ++it) {
        std::string full_dir_path = (it->second).rootDir + _path;
        if (0 == access(full_dir_path.c_str(), F_OK)) {
            DIR *dir = opendir(full_dir_path.c_str());
            if (dir == NULL) {
                GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_OPDIR, errno);
                return false;
            }

            struct dirent entry;
            struct dirent *result;
            for (int iRet = readdir_r(dir, &entry, &result); iRet == 0 && result != NULL; iRet =
                    readdir_r(dir, &entry, &result)) {

                if (strcmp(entry.d_name, ".") != 0 && strcmp(entry.d_name, "..") != 0) {
                    closedir(dir);
                    return false;
                }
            } // for dir entry
            closedir(dir);
        } //full_dir_path exist
    } // for dir
    return true;
}

#if 0
bool DirLocal::RecursiveRemove(const std::string &path) {
    for(std::map<int, Storage::DiskInfo>::iterator it = _rootDirs->begin(); it != _rootDirs->end(); ++ it) {
        std::string full_path = (it->second).rootDir + "/" + path;
        if(0 == access(full_path.c_str(), F_OK)) {
            DIR *dir = opendir(full_path.c_str());
            if(NULL == dir) {
                errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_OPDIR, errno);
                return false;
            }

            struct dirent entry;
            struct dirent *result;
            for(int iRet = readdir_r(dir, &entry, &result);
                    iRet == 0 && result != NULL;
                    iRet = readdir_r(dir, &entry, &result)) {

                if(0 == strcmp(entry.d_name, ".") || 0 == strcmp(entry.d_name, "..")) {
                    continue;
                }
                std::string user_entry_path = path + "/" + entry.d_name;
                std::string full_entry_path = full_path + "/" + entry.d_name;
                struct stat stat_buf;
                int sRet = stat(full_entry_path.c_str(), &stat_buf);
                if(sRet < 0) {
                    errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
                    closedir(dir);
                    return false;
                }
                if(S_ISDIR(stat_buf.st_mode)) {
                    bool bRet = RecursiveRemove(user_entry_path);
                    if(!bRet) {
                        closedir(dir);
                        return false;
                    }
                } else {
                    ScopedPointer<File> file(File::New(user_entry_path, STORAGE_LOCAL));
                    if(!file->Remove()) {
                        closedir(dir);
                        return false;
                    }
                }
            }
            closedir(dir);
            int iRet = rmdir(full_path.c_str());
            if(iRet < 0) {
                errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RMDIR, errno);
                return false;
            }
        } //full_dir exist
    } // for rootDirs
    if(!_storage->DeleteMeta(path, META_TMP_FILE)) {
        return false;
    }

    errno = GCACHE_TMP_SUCCESS;
    return true;
}
#endif

bool DirLocal::PartExist() {
    for (std::map<int, Storage::DiskInfo>::iterator it = _rootDirs->begin(); it != _rootDirs->end();
            ++it) {
        std::string full_dir_path = (it->second).rootDir + _path;
        if (0 == access(full_dir_path.c_str(), F_OK)) {
            return true;
        }
    }
    return false;
}

NAMESPACE_END_SEISFS
}
