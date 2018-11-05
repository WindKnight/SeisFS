/*
 * gcache_tmp_dir_nas.cpp
 *
 *  Created on: Jan 8, 2016
 *      Author: wyd
 *
 *                      May 18, 2016
 *      Author: Joker
 */

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <iterator>
#include <set>
#include <utility>

#include "gcache_tmp_dir_nas.h"
#include "gcache_io.h"
#include "gcache_log.h"
#include "gcache_scoped_pointer.h"
#include "gcache_string.h"
#include "gcache_tmp_file.h"
#include "gcache_tmp_meta.h"

//extern int GCACHE_errno;
NAMESPACE_BEGIN_SEISFS
namespace tmp {

/**
 * destructor
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param
 * @func                destruct object
 * @return                no return
 */
DirNAS::~DirNAS() {

}
/**
 * constructor
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                path is the name of this file
 * @func                set member variable to initial values.
 * @return                no return
 */

DirNAS::DirNAS(const std::string &path) :
        _path(path) {
    _cacheInfo = NULL;
    _rootDirs = NULL;
    _storage = NULL;

}
/**
 * Exist
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param
 * @func                Check The Existence of Current Dir Object
 * @return                return True if there exists one dir in the file system,else return false
 */
bool DirNAS::Exist() {

    if (PartExist()) {
        MakeDirs();
        return true;
    } else {
        return false;
    }
}
/**
 * GetTotalSpace
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                level the CacheLevel of the exist Storage
 * @func                get total space of the right Storage according to the given CacheLevel(EnumType)
 * @return                return a int64_t as the total space of the Storage
 */
int64_t DirNAS::GetTotalSpace(CacheLevel level) {
    int64_t total_space = 0;
    for (std::vector<Storage::DiskInfo>::iterator it = _cacheInfo[level].begin();
            it != _cacheInfo[level].end(); ++it) {
        total_space += _storage->GetRootTotalSpace(it->dirID);
    }
    return total_space;
}
/**
 * GetFreeSpace
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                level the CacheLevel of the exist Storage
 * @func                get total  free space of the right Storage according to the given CacheLevel(EnumType)
 * @return                return a int64_t as the total free space of the Storage
 */
int64_t DirNAS::GetFreeSpace(CacheLevel level) {
    int64_t free_space = 0;
    for (std::vector<Storage::DiskInfo>::iterator it = _cacheInfo[level].begin();
            it != _cacheInfo[level].end(); ++it) {
        free_space += _storage->GetRootFreeSpace(it->dirID);
    }
    return free_space;
}
/**
 * MakeDirs
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param
 * @func                Make Dirs on all the exists Storage Disks
 * @return                return True if action succeed,if an error occurs return False
 */
bool DirNAS::MakeDirs() {
    for (std::map<int, Storage::DiskInfo>::iterator it = _rootDirs->begin(); it != _rootDirs->end();
            ++it) {
        std::string full_dir_path = (it->second).rootDir + _path;

        bool bRet = RecursiveMakeDir(full_dir_path);

        if (!bRet) {
            GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_MKDIR, errno);
            return false;
        }
    }
    if (!_storage->MakeMetaDir(_path)) return false;
    return true;
}
/**
 * Remove
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                recursive :If recursive remove all subdirs enabled,set True,or else set False
 * @func                Remove the Current directory, according to the parameter, remove subdir or not
 * @return                return True when remove succeed,if an error occurs ,return False
 */
bool DirNAS::Remove(bool recursive) {
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
/**
 * Rename
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                new_name is the new name string of the Dir , if it is a exist dir's path,an error occurs
 * @func                Rename The Directory use the parameter
 * @return                return True if Rename Succeed,if an error occurs ,return False
 */
bool DirNAS::Rename(const std::string& new_name) {

    if (!StartsWith(new_name, "/")) { // full path
        GCACHE_tmp_errno = GCACHE_TMP_ERR_PATH;
        return false;
    }

    ScopedPointer<DirNAS> dir_new(new DirNAS(new_name));
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
/**
 * Move
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                dest_dir is the destination path of the move action
 * @func                move the current directory an all its subdirectories to the destination path
 * @return                return True when Move succeed,if an error occurs ,return False
 */
bool DirNAS::Move(const std::string& dest_dir) {

    if (!StartsWith(dest_dir, "/")) { // full path
        GCACHE_tmp_errno = GCACHE_TMP_ERR_PATH;
        return false;
    }
    std::vector<std::string> paths = SplitString(_path, "/");
    std::string cleanDirName = paths[paths.size() - 1];

    ScopedPointer<DirNAS> dirDest(new DirNAS(dest_dir));
    dirDest->Init();
    if (dirDest->Exist()) {
        std::string realDestPath = dest_dir + "/" + cleanDirName;
        ScopedPointer<DirNAS> dirRealDest(new DirNAS(realDestPath));
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
/**
 * MoveToRealDest
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                real_dest_path The Destination path of the dir
 *
 * @func                Move current directory to dest_dir path
 * @return                return True when process succeed,if an error occurs,return false
 */
bool DirNAS::MoveToRealDest(const std::string &real_dest_path) {

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
/**
 * Copy
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                dest_dir is the destination of the current dir
 *                                 if dest_dir exists, Copy current dir to the destination path as a subdir
 *                                 if dest_dir doesn't exists , make a new diretory according to the parameter dest_dir,
 *                                         and Copy all of the current dir's content such as files,subdirs to the dest path
 *
 * @func                Copy the current directory to the destination path
 * @return                return True when Copy succeed,if an error occurs,return false
 */
bool DirNAS::Copy(const std::string& dest_dir) {
    if (!StartsWith(dest_dir, "/")) { // full path
        GCACHE_tmp_errno = GCACHE_TMP_ERR_PATH;
        return false;
    }

    Info("calling copy, from %s to %s\n", _path.c_str(), dest_dir.c_str());
    fflush(stdout);

    std::vector<std::string> paths = SplitString(_path, "/");
    std::string cleanDirName = paths[paths.size() - 1];

    ScopedPointer<DirNAS> dirDest(new DirNAS(dest_dir));
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
        for (int i = 0; i < (int) (dest_paths.size() - 1); ++i) {
            dest_parent_path = dest_parent_path + "/" + dest_paths[i];
        }
        ScopedPointer<DirNAS> dirDestParent(new DirNAS(dest_parent_path));
        if (!dirDestParent->Init()) {
            return false;
        }
        if (!RecursiveCopy(_path, realDestPath)) return false;
    }
    return true;
}
/**
 * RecursiveCopy
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                real_dest_path The Destination path of the dir
 *                                 real_src_path The Source path of the dir
 *
 * @func                Copy source directory to dest_dir path
 * @return                return True when process succeed,if an error occurs,return false
 */
bool DirNAS::RecursiveCopy(const std::string &real_src_path, const std::string &real_dest_path) {
//    printf("calling recursive copy, from %s to %s\n", real_src_path.c_str(),
//            real_dest_path.c_str());
//    fflush(stdout);

    ScopedPointer<DirNAS> dirRealDest(new DirNAS(real_dest_path));
    dirRealDest->Init();
    if (!dirRealDest->MakeDirs()) {
        return false;
    }

    ScopedPointer<DirNAS> dir_src(new DirNAS(real_src_path));
    dir_src->Init();
    if (dir_src->IsEmpty()) {
        ScopedPointer<DirNAS> dir_dest(new DirNAS(real_dest_path));
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

            StorageArch s_arch = STORAGE_NAS;
            ScopedPointer<File> f_src(File::New(src_child_file_path, s_arch));
            if (f_src == NULL || s_arch != STORAGE_NAS || !f_src->Copy(dest_child_file_path)) {
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
/**
 * ListFiles
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                fileList the return list of files
 *                                 wildcard the pattern symbol
 *
 * @func                Store the File of current directory in the parmeter fileList
 * @return                return True when process succeed,if an error occurs,return false
 */

bool DirNAS::ListFiles(std::list<std::string>& fileList, const std::string& wildcard) {
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
/**
 * ListDirs
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                dirList the Return list of the result
 *              wildcard the match pattern string("*" and so on.)
 *
 * @func                Store the Subdirectories of current directory in the parmeter dirList
 * @return                return True when process succeed,if an error occurs,return false
 *
 */
bool DirNAS::ListDirs(std::list<std::string>& dirList, const std::string& wildcard) {
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

/**
 * Init
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param
 *
 * @func                Do some Initial jobs
 * @return                return True when process succeed,if an error occurs,return false
 */
bool DirNAS::Init() {
    StorageArch s_arch = STORAGE_NAS;
    File *f_tmp = File::New(_path, s_arch);
    if (f_tmp == NULL || s_arch != STORAGE_NAS) {
        return false;
    }
    if (f_tmp->Exists()) {
        delete f_tmp;
        GCACHE_tmp_errno = GCACHE_TMP_ERR_NOTDIR;
        return false;
    }

    _storage = Storage::GetInstance(STORAGE_NAS);
    if (NULL == _storage) {
//                delete *f_tmp;
        GCACHE_tmp_errno = GCACHE_TMP_ERR_STORAGE;
        return false;
    }
    _rootDirs = _storage->GetAllRootDir();
    _cacheInfo = _storage->GetCacheInfo();
    return true;
}
/**
 * IsEmpty
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param
 *
 * @func                check if the dir has no contents
 * @return                return True when process succeed ,if an error occurs or directory is not empty,return false
 */
bool DirNAS::IsEmpty() {
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
bool DirNAS::RecursiveRemove(const std::string &path) {
    for(std::map<int, Storage::DiskInfo>::iterator it = _rootDirs->begin(); it != _rootDirs->end(); ++ it) {
        std::string full_path = (it->second).rootDir + "/" + path;
        if(0 == access(full_path.c_str(), F_OK)) {
            DIR *dir = opendir(full_path.c_str());
            if(NULL == dir) {
                GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_OPDIR, errno);
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
                    ScopedPointer<File> file(File::New(user_entry_path, STORAGE_NAS));
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
/**
 * PartExist
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param
 * @func                Test the existence of the current directory
 * @return                return True when process succeed,if an error occurs,return false
 */
bool DirNAS::PartExist() {
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
