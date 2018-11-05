/*
 * gcache_tmp_dir_dis.cpp
 *
 *  Created on: Jan 8, 2016
 *      Author: wyd
 */
#include "gcache_tmp_dir_hdfs.h"

#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <cerrno>
#include <cstdio>
#include <iterator>
#include <list>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "gcache_global.h"
#include "gcache_io.h"
#include "gcache_scoped_pointer.h"
#include "gcache_string.h"
#include "gcache_advice.h"
#include "gcache_tmp_error.h"
#include "gcache_tmp_file.h"
#include "gcache_tmp_meta.h"

#include "gcache_hdfs_utils.h"
#include "gcache_log.h"
#include "gcache_tmp_dir_hdfs.h"
#include "gcache_tmp_storage_hdfs.h"

//extern int GCACHE_errno;
NAMESPACE_BEGIN_SEISFS
namespace tmp {
/**
 * constructor
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                dir_name is the name of this file
 * @func                set member variable to initial values.
 * @return                no return
 */
DirHDFS::DirHDFS(const std::string &dir_name) :
        _path(dir_name) {
    _storage = NULL;
    _rootDirs = NULL;
    _cacheInfo = NULL;
    _FS = NULL;
    _rootFS = NULL;
    _File = NULL;

}
/**
 * destructor
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param
 * @func                destruct object
 * @return                no return
 */

DirHDFS::~DirHDFS() {

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
bool DirHDFS::Exist() {
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
int64_t DirHDFS::GetTotalSpace(CacheLevel level) {
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
int64_t DirHDFS::GetFreeSpace(CacheLevel level) {
    int64_t free_space = 0;
    for (std::vector<Storage::DiskInfo>::iterator it = _cacheInfo[level].begin();
            it != _cacheInfo[level].end(); ++it) {
        free_space += _storage->GetRootFreeSpace(it->dirID);
    }
    return free_space;

}
/**
 * getRootFS
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param                path a String which are one of the rootDirs
 * @func                get The HDFS Link of The given rootDir
 * @return                return a hdfsFS type or NULL if rootDir doesn't exsits
 */
hdfsFS DirHDFS::getRootFS(std::string path) {
    std::map<std::string, hdfsFS>::iterator it = _rootFS->find(path);
    if (it == _rootFS->end()) {
        GCACHE_tmp_errno = GCACHE_TMP_ERR_GETFS;
        return NULL;
    }
    return it->second;
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
bool DirHDFS::MakeDirs() {
    for (std::map<int, Storage::DiskInfo>::iterator it = _rootDirs->begin(); it != _rootDirs->end();
            ++it) {
        std::string full_dir_path = (it->second).rootDir + _path;
        //Modify RecursiveMakeDir (params pattern: hdfs://path/to/dir/file)

        hdfsFS fs = getRootFS((it->second).rootDir);
        std::string rootFileName = hdfsGetPath(full_dir_path);
        int bRet = hdfsCreateDirectory(fs, rootFileName.c_str());
        if (bRet != 0) {
            //hdfsDisconnect(fs);
            GCACHE_tmp_errno = GcacheTemCombineErrno(errno, GCACHE_TMP_ERR_MKDIR);
            return false;
        }
        hdfsChmod(fs, rootFileName.c_str(), 0755);

        //hdfsDisconnect(fs);

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
bool DirHDFS::Remove(bool recursive) {
    if (!recursive) {
        if (!IsEmpty()) {
            GCACHE_tmp_errno = GCACHE_TMP_ERR_NOTEMPTY;
            return false;
        }
        for (std::map<int, Storage::DiskInfo>::iterator it = _rootDirs->begin();
                it != _rootDirs->end(); ++it) {
            std::string full_dir_path = (it->second).rootDir + _path;
            //hdfsFS fs = getHDFS(full_dir_path);
            hdfsFS fs = getRootFS((it->second).rootDir);
            std::string file_path = hdfsGetPath(full_dir_path);
            if (0 == hdfsExists(fs, file_path.c_str())) {
                int iRet = hdfsDelete(fs, file_path.c_str(), 0);
                if (iRet < 0) {
                    //hdfsDisconnect(fs);
                    GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RMDIR, errno);
                    return false;
                }
            }
            //hdfsDisconnect(fs);
        }
    } else {

        for (std::map<int, Storage::DiskInfo>::iterator it = _rootDirs->begin();
                it != _rootDirs->end(); ++it) {
            std::string full_path = (it->second).rootDir + _path;
            //hdfsFS fs = getHDFS(full_path);
            hdfsFS fs = getRootFS((it->second).rootDir);

            std::string file_path = hdfsGetPath(full_path);
            if (0 == hdfsExists(fs, file_path.c_str())) {
                //modify RecursiveRemove or use functions from library
                int rm_ret = hdfsDelete(fs, file_path.c_str(), 1);
                if (rm_ret != 0) {
                    //hdfsDisconnect(fs);
                    GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RMDIR, errno);
                    return false;
                }
            }
            //hdfsDisconnect(fs);
        }
    }
    //std::string file_real_path = hdfsGetPath(_path); _path /path/to/file
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
bool DirHDFS::Rename(const std::string& new_name) {

    if (!StartsWith(new_name, "/")) { // full path
        GCACHE_tmp_errno = GCACHE_TMP_ERR_PATH;
        return false;
    }

    ScopedPointer<DirHDFS> dir_new(new DirHDFS(new_name));
    dir_new->Init();
    if (dir_new->Exist() && (!dir_new->IsEmpty())) {
        GCACHE_tmp_errno = GCACHE_TMP_ERR_NOTEMPTY;
        return false;
    }

    //ice------------
    if (dir_new->Exist() && dir_new->IsEmpty()) {
        if (!dir_new->Remove()) {
            return false;
        }
    }
    //ice------------

    for (std::map<int, Storage::DiskInfo>::iterator it = _rootDirs->begin(); it != _rootDirs->end();
            ++it) {
        std::string full_src_path = (it->second).rootDir + _path;
        std::string full_dest_path = (it->second).rootDir + new_name;
        //hdfsFS src_fs = getHDFS(full_src_path);
        hdfsFS src_fs = getRootFS((it->second).rootDir);

        //hdfsFS dst_fs = getHDFS(full_dest_path);
        //hdfsFS dst_fs = getRootFS((it->second).rootDir);

        std::string src_path = hdfsGetPath(full_src_path);
        std::string dst_path = hdfsGetPath(full_dest_path);
        if (0 > hdfsRename(src_fs, src_path.c_str(), dst_path.c_str())) {
            //hdfsDisconnect(src_fs);
            //hdfsDisconnect(dst_fs);
            GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RENAME, errno);
            return false;
        }
        //hdfsDisconnect(src_fs);
        //hdfsDisconnect(dst_fs);
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
bool DirHDFS::Move(const std::string& dest_dir) {

    if (!StartsWith(dest_dir, "/")) { // full path
        GCACHE_tmp_errno = GCACHE_TMP_ERR_PATH;
        return false;
    }
    std::vector<std::string> paths = SplitString(_path, "/");
    std::string cleanDirName = paths[paths.size() - 1];

    ScopedPointer<DirHDFS> dirDest(new DirHDFS(dest_dir));
    dirDest->Init();
    if (dirDest->Exist()) {
        std::string realDestPath = dest_dir + "/" + cleanDirName;
        ScopedPointer<DirHDFS> dirRealDest(new DirHDFS(realDestPath));
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
bool DirHDFS::Copy(const std::string& dest_dir) {
    if (!StartsWith(dest_dir, "/")) { // full path
        GCACHE_tmp_errno = GCACHE_TMP_ERR_PATH;
        return false;
    }

    Info("calling copy, from %s to %s\n", _path.c_str(), dest_dir.c_str());
    fflush(stdout);

    std::vector<std::string> paths = SplitString(_path, "/");
    std::string cleanDirName = paths[paths.size() - 1];

    ScopedPointer<DirHDFS> dirDest(new DirHDFS(dest_dir));
    dirDest->Init();
    if (dirDest->Exist()) {

        std::string realDestPath = dest_dir + "/" + cleanDirName;

        Warn("%s exists, real_dest_path = %s\n", dest_dir.c_str(), realDestPath.c_str());
        fflush(stdout);

        //RecursiveCopy
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
        ScopedPointer<DirHDFS> dirDestParent(new DirHDFS(dest_parent_path));
        dirDestParent->Init();
        if (!dirDestParent->Exist()) {
            GCACHE_tmp_errno = GCACHE_TMP_ERR_NOT_EXIST;
            return false;
        }

        if (!RecursiveCopy(_path, realDestPath)) return false;
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
bool DirHDFS::ListFiles(std::list<std::string>& fileList, const std::string& wildcard) {
    std::set<std::string> file_set;

    for (std::map<int, Storage::DiskInfo>::iterator it = _rootDirs->begin(); it != _rootDirs->end();
            ++it) {
        std::string full_dir_path = (it->second).rootDir + _path;
        //hdfsFS fs = getHDFS(full_dir_path);
        hdfsFS fs = getRootFS((it->second).rootDir);

        std::string file_path = hdfsGetPath(full_dir_path);
        if (0 == hdfsExists(fs, file_path.c_str())) {
            int entries = 0;
            hdfsFileInfo *hf_info = NULL;
            hf_info = hdfsListDirectory(fs, file_path.c_str(), &entries);
            if (NULL == hf_info) {
                //hdfsDisconnect(fs);
                GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_OPDIR, errno);
                return false;
            }

            for (int i = 0; i < entries; ++i) {

                char * entry_name = hf_info[i].mName;
                std::string s_entry_name(entry_name);
                std::vector<std::string> entry_paths = SplitString(s_entry_name, "/");
                std::string relative_path = "";
                std::string pure_root_path = hdfsGetPath(it->second.rootDir);
                std::vector<std::string> pure_paths = SplitString(pure_root_path, "/");
                std::vector<std::string> _paths = SplitString(_path, "/");
                int root_len = pure_paths.size();
                int _path_len = _paths.size();
                for (unsigned int j = root_len + _path_len; j < entry_paths.size(); ++j) {
                    relative_path = relative_path + "/" + entry_paths[j];
                }

                if (0 == strcmp(entry_name, ".") || 0 == strcmp(entry_name, "..")) {
                    continue;
                }
                tObjectKind type = hf_info[i].mKind;
                if (type < 0) {
                    GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
                    hdfsFreeFileInfo(hf_info, entries);
                    //hdfsDisconnect(fs);
                    return false;
                }

                if (type == 'F') {
                    if (WildcardMatch(wildcard.c_str(), entry_name, true)) {
                        file_set.insert(relative_path.c_str());
                    }
                }
            }
            hdfsFreeFileInfo(hf_info, entries);
            //hdfsDisconnect(fs);
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
 *         Option:
 *                 In case of not only one root dir exists
 *                 The return path of file or dir is relative path( get rid of /user/username/ + rootDir )
 *                an example here
 *                root1: u0/d0
 *                root2: u0/d1
 *                dir:         test1
 *                subdir: test11,test12,test13
 *                we just return a list of [test12,test13,test11]
 *                but not [u0/d0/test1/tes12,u0/d0/test1/test13,u0/d0/test1/test11,
 *                                u0/d1/test1/tes12,u0/d1/test1/test13,u0/d1/test1/test11,
 *                                ]
 *
 */
bool DirHDFS::ListDirs(std::list<std::string>& dirList, const std::string& wildcard) {
    std::set<std::string> dir_set;

    for (std::map<int, Storage::DiskInfo>::iterator it = _rootDirs->begin(); it != _rootDirs->end();
            ++it) {
        std::string full_dir_path = (it->second).rootDir + _path;
        //hdfsFS fs = getHDFS(full_dir_path);
        hdfsFS fs = getRootFS((it->second).rootDir);

        std::string file_path = hdfsGetPath(full_dir_path);
        if (0 == hdfsExists(fs, file_path.c_str())) {
            int entries = 0;
            hdfsFileInfo *hf_info = NULL;
            hf_info = hdfsListDirectory(fs, file_path.c_str(), &entries);
            if (NULL == hf_info) {
                //hdfsDisconnect(fs);
                GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_OPDIR, errno);
                return false;
            }

            for (int i = 0; i < entries; ++i) {
                //File Path is the Whole path include user/username + /GCACHETEMP/name/+path(Done)
                char * entry_name = hf_info[i].mName;
                std::string s_entry_name(entry_name);
                std::vector<std::string> entry_paths = SplitString(s_entry_name, "/");
#if 0
                std::string real_path = "";
                for(int i = 2; i < entry_paths.size(); ++i) {
                    real_path = real_path + "/" + entry_paths[i];
                }
#endif
                std::string relative_path = "";
                std::string pure_root_path = hdfsGetPath(it->second.rootDir);
                std::vector<std::string> pure_paths = SplitString(pure_root_path, "/");
                std::vector<std::string> _paths = SplitString(_path, "/");
                int root_len = pure_paths.size();
                int _path_len = _paths.size();
                for (unsigned int j = root_len + _path_len; j < entry_paths.size(); ++j) {
                    relative_path = relative_path + "/" + entry_paths[j];
                }

                if (0 == strcmp(entry_name, ".") || 0 == strcmp(entry_name, "..")) {
                    continue;
                }
                tObjectKind type = hf_info[i].mKind;
                if (type < 0) {
                    GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_STAT, errno);
                    hdfsFreeFileInfo(hf_info, entries);
                    //hdfsDisconnect(fs);
                    return false;
                }

                if (type == 'D') {
                    if (WildcardMatch(wildcard.c_str(), entry_name, true)) {
                        dir_set.insert(relative_path.c_str());
                    }
                }
            }
            hdfsFreeFileInfo(hf_info, entries);
            //hdfsDisconnect(fs);
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
bool DirHDFS::Init() {
    StorageArch s_arch = STORAGE_DIST;
    File *f_tmp = File::New(_path, s_arch);
    if (f_tmp == NULL || s_arch != STORAGE_DIST) {
        return false;
    }
    if (f_tmp->Exists()) {
        delete f_tmp;
        GCACHE_tmp_errno = GCACHE_TMP_ERR_NOTDIR;
        return false;
    }
    _storage = Storage::GetInstance(STORAGE_DIST);
    if (NULL == _storage) {
        //delete f_tmp;
        GCACHE_tmp_errno = GCACHE_TMP_ERR_STORAGE;
        return false;
    }
    _rootDirs = _storage->GetAllRootDir();
    _cacheInfo = _storage->GetCacheInfo();
    _rootFS = ((StorageHDFS*) _storage)->GetAllRootFS();
    //delete f_tmp;
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

bool DirHDFS::IsEmpty() {
    for (std::map<int, Storage::DiskInfo>::iterator it = _rootDirs->begin(); it != _rootDirs->end();
            ++it) {
        std::string full_dir_path = (it->second).rootDir + _path;
        //hdfsFS fs =getHDFS(full_dir_path);
        hdfsFS fs = getRootFS((it->second).rootDir);

        std::string file_path = hdfsGetPath(full_dir_path);
        //hdfsFile file;
        if (0 == hdfsExists(fs, file_path.c_str())) {
#if 0
            //OpenFile here Just For File
            file = hdfsOpenFile(fs,file_path.c_str(),O_RDONLY,0,REPLICATION,0);
            if(file == NULL) {
                //Open file fail, need not to close file
                errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_OPDIR, errno);
                //hdfsDisconnect(fs);
                return false;
            }
#endif
            hdfsFileInfo * hf_info;
            int file_count;
            hf_info = hdfsListDirectory(fs, file_path.c_str(), &file_count);
            if (hf_info == NULL) {
                // Get info Failed need not to deallocate hf_info
                //hdfsDisconnect(fs);
                GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_OPDIR, errno);
                return false;
            }
            if (file_count > 0) {
                hdfsFreeFileInfo(hf_info, file_count);
                //hdfsCloseFile(fs,file);
                //hdfsDisconnect(fs);
                return false;
            }
            hdfsFreeFileInfo(hf_info, file_count);
            //hdfsCloseFile(fs,file);
            //hdfsDisconnect(fs);
        } //full_dir_path exist
    } // for dir
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
bool DirHDFS::MoveToRealDest(const std::string &real_dest_path) {
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
        //hdfsFS fs_src =getHDFS(oldDirFullPath);

        hdfsFS fs_src = getRootFS((it->second).rootDir);

        //hdfsFS fs_dst =getHDFS(destDirFullPath);

        //hdfsFS fs_dst = getRootFS((it->second).rootDir);

        std::string src_file_path = hdfsGetPath(oldDirFullPath);
        std::string dst_file_path = hdfsGetPath(destDirFullPath);
        // test Rename
        if (0 > hdfsRename(fs_src, src_file_path.c_str(), dst_file_path.c_str())) {
            //hdfsDisconnect(fs_src);
            //hdfsDisconnect(fs_dst);
            GCACHE_tmp_errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_RENAME, errno);
            return false;
        }
        //hdfsDisconnect(fs_src);
        //hdfsDisconnect(fs_dst);
    }

    if (!_storage->RenameMeta(_path, real_dest_path, META_TMP_FILE)) {
        return false;
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
bool DirHDFS::RecursiveCopy(const std::string &real_src_path, const std::string &real_dest_path) {

//    Info("calling recursive copy, from %s to %s\n", real_src_path.c_str(),
//            real_dest_path.c_str());
    fflush(stdout);

    ScopedPointer<DirHDFS> dirRealDest(new DirHDFS(real_dest_path));
    dirRealDest->Init();
    if (!dirRealDest->MakeDirs()) {
        return false;
    }

    ScopedPointer<DirHDFS> dir_src(new DirHDFS(real_src_path));
    dir_src->Init();
    if (dir_src->IsEmpty()) {
        ScopedPointer<DirHDFS> dir_dest(new DirHDFS(real_dest_path));
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

            StorageArch s_arch = STORAGE_DIST;
            ScopedPointer<File> f_src(File::New(src_child_file_path, s_arch));
            if (f_src == NULL || s_arch != STORAGE_DIST || !f_src->Copy(dest_child_file_path)) {
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
 * PartExist
 *
 * @author          buaa_zhang
 * @version                0.1.4
 * @param
 * @func                Test the existence of the current directory
 * @return                return True when process succeed,if an error occurs,return false
 */
bool DirHDFS::PartExist() {
    for (std::map<int, Storage::DiskInfo>::iterator it = _rootDirs->begin(); it != _rootDirs->end();
            ++it) {
        std::string full_dir_path = (it->second).rootDir + _path;
        //hdfsFS fs =getHDFS(full_dir_path);

        hdfsFS fs = getRootFS((it->second).rootDir);

        std::string file_path = hdfsGetPath(full_dir_path);
        if (0 == hdfsExists(fs, file_path.c_str())) {
            //hdfsDisconnect(fs);
            return true;
        }
        //hdfsDisconnect(fs);
    }

    return false;

}

NAMESPACE_END_SEISFS
}
