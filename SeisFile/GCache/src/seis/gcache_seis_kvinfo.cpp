/*
 * gcache_seis_reel.cpp
 *
 *  Created on: Jul 19, 2016
 *      Author: wb
 */

#include "gcache_hdfs_utils.h"
#include "gcache_seis_utils.h"
#include "gcache_seis_error.h"
#include "gcache_io.h"
#include "gcache_string.h"

#include <bits/functional_hash.h>
#include "gcache_seis_kvinfo.h"

NAMESPACE_BEGIN_SEISFS
namespace file {
SeisKVInfo::SeisKVInfo(hdfsFS fs, const std::string& hdfs_data_name, SharedPtr<MetaSeisHDFS> meta) {
    _fs = fs;
    _hdfs_data_name = hdfs_data_name;
    _hdfs_data_reel_name = hdfs_data_name + "/Seis.Reel";
    _meta = meta;
    _is_reel_modified = false;

}

SeisKVInfo::~SeisKVInfo() {
    if (_is_reel_modified) {
        TouchSeisModifyTime(_fs, _hdfs_data_name);
    }
}

bool SeisKVInfo::Put(const std::string &key, const char* data, uint32_t size) {
    std::string obj_filename = GetObjFilename(key);
    if ("" == obj_filename) {
        errno = GCACHE_SEIS_ERR_PARAMERR;
        return false;
    }

    hdfsFile fd = hdfsOpenFile(_fs, obj_filename.c_str(), O_WRONLY, 0,
    REPLICATION, 0);
    if (fd == NULL) {
        errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
        return false;
    }
    hdfsChmod(_fs, obj_filename.c_str(), 0644);

    int64_t write_ret = SafeWrite(_fs, fd, data, size);
    hdfsCloseFile(_fs, fd);
    if (write_ret != size) {
        errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_WRITE, errno);
        return false;
    }

    _is_reel_modified = true;
    return true;
}

bool SeisKVInfo::Delete(const std::string &key) {
    std::string obj_filename = GetObjFilename(key);
    if ("" == obj_filename) {
        return false;
    }

    int iRet = hdfsDelete(_fs, obj_filename.c_str(), 0);
    if (-1 == iRet) {
        errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_RMFILE, errno);
        return false;
    }

    _is_reel_modified = true;
    return true;
}

bool SeisKVInfo::Get(const std::string &key, char*& data, uint32_t& size) {
    std::string obj_filename = GetObjFilename(key);
    if ("" == obj_filename) {
        return false;
    }
    size = SizeOf(key);
    data = new char[size];

    hdfsFile fd = hdfsOpenFile(_fs, obj_filename.c_str(), O_RDONLY, 0,
    REPLICATION, 0);
    if (fd == NULL) {
        delete[] data;
        errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
        return false;
    }

    int64_t read_ret = SafeRead(_fs, fd, data, size);
    hdfsCloseFile(_fs, fd);
    if (read_ret != size) {
        delete[] data;
        errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
        return false;
    }

    return true;
}

uint32_t SeisKVInfo::SizeOf(const std::string &key) {

    std::string obj_filename = GetObjFilename(key);
    if ("" == obj_filename) {
        return 0;
    }

    hdfsFileInfo* info = hdfsGetPathInfo(_fs, obj_filename.c_str());
    if (info == NULL) {
        errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_STAT, errno);
        return 0;
    }
    int64_t size = info->mSize;
    hdfsFreeFileInfo(info, 1);

    return size;
}

bool SeisKVInfo::Exists(const std::string &key) {
    std::string obj_filename = GetObjFilename(key);
    if ("" == obj_filename) {
        return false;
    }

    if (-1 == hdfsExists(_fs, obj_filename.c_str())) {
        return false;
    }

    return true;
}

bool SeisKVInfo::Init() {
    if (hdfsExists(_fs, _hdfs_data_reel_name.c_str()) == -1) {

        if (hdfsCreateDirectory(_fs, _hdfs_data_reel_name.c_str()) == -1) {
            return false;
        }

        /*reel actually is a folder, there will be OBJ_DIR_NUM sub-folders in this table, object will be hash to these sub-folders*/
        std::string obj_dir_base_name = _hdfs_data_reel_name + "/obj_";
        for (int i = 0; i < OBJ_DIR_NUM; ++i) {
            std::string obj_dir_name = obj_dir_base_name + ToString(i);
            int ret = hdfsCreateDirectory(_fs, obj_dir_name.c_str());
            if (ret < 0) {
                return false;
            }
        }
    }

    return true;
}

std::string SeisKVInfo::GetObjFilename(const std::string &key) {
    std::hash < std::string > hash_str;
    size_t hash_num = hash_str(key);
    int tail_num = hash_num % OBJ_DIR_NUM;

    std::string obj_dir_name = _hdfs_data_reel_name + "/obj_" + ToString(tail_num);
    std::string obj_file_name = obj_dir_name + "/" + key;

    return obj_file_name;
}

bool SeisKVInfo::GetKeys(std::vector<std::string> &keys) {
    keys.clear();

    std::string obj_dir_base_name = _hdfs_data_reel_name + "/obj_";

    for (int i = 0; i < OBJ_DIR_NUM; ++i) {
        std::string obj_dir_name = obj_dir_base_name + ToString(i);
        int num_entries = 0;
        int *p = &num_entries;
        hdfsFileInfo* fileInfo = hdfsListDirectory(_fs, obj_dir_name.c_str(), p);
        if (fileInfo == NULL) {
            return false;
        }
        hdfsFileInfo* infoFree = fileInfo;
        for (int j = 0; j < *p; ++j) {
            std::string str(fileInfo->mName);
            std::vector < std::string > split_str = SplitString(str, "/");
            fileInfo++;
            keys.push_back(split_str.back());
        }
        hdfsFreeFileInfo(infoFree, *p);
    }

    return true;
}

NAMESPACE_END_SEISFS
}
