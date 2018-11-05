/*
 * gcache_seis_merger.cpp
 *
 *  Created on: Jul 19, 2016
 *      Author: wb
 */
#include "gcache_seis_merger.h"
#include "gcache_seis_error.h"
#include "gcache_io.h"
#include "gcache_hdfs_utils.h"
#include "gcache_seis_utils.h"
#include "string.h"

//extern int GCACHE_seis_errno;
NAMESPACE_BEGIN_SEISFS
namespace file {
/**
 * SeisMerger
 *
 * @author              weibing
 * @version             0.2.7
 * @param               fs is the hdfs file system handle.
 *                      hdfs_data_name is the full name of seis.
 *                      meta is the meta of seis.
 * @func                construct a object.
 * @return              no return.
 */
SeisMerger::SeisMerger(hdfsFS fs, const std::string& hdfs_data_name, SharedPtr<MetaSeisHDFS> meta) {

    _fs = fs;
    _hdfs_data_name = hdfs_data_name;
    _meta = meta;
}

/**
 * ~SeisMerger
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                destruct a object.
 * @return              no return.
 */
SeisMerger::~SeisMerger() {
}

/**
 * Merge
 *
 * @author              weibing
 * @version             0.2.7
 * @param               src_meta the meta of seis to be merged.
 *                      _root_dir the root directory of all seis.
 *                      src_data_name the relative path of seis to be merged.
 *                      reelMerge the reel merge.
 * @func                merge log data and original write data.
 * @return              return true if merge successfully.
 */
bool SeisMerger::Merge(SharedPtr<MetaSeisHDFS> src_meta, const std::string _root_dir,
        const std::string& src_data_name) {

    std::string hdfs_src_data_name = hdfsGetPath(_root_dir + "/" + src_data_name);
    if (!MergeTrace(src_meta, hdfs_src_data_name)) {
        return false;
    }

    if (!MergeHead(src_meta, hdfs_src_data_name)) {
        return false;
    }

    return true;
}

/**
 * MergeHead
 *
 * @author              weibing
 * @version             0.2.7
 * @param               src_meta the meta of seis to be merged.
 *                      hdfs_src_data_name the root directory of all seis.
 *                      reelMerge the reel merge.
 * @func                merge log data and original write data.
 * @return              return true if merge successfully.
 */
bool SeisMerger::MergeHead(SharedPtr<MetaSeisHDFS> src_meta, const std::string hdfs_src_data_name) {
    if (src_meta->head_total_rows == 0) //if the merged seis head rows is 0, we don't need to merge, just remove it
            {
        return true;
    }

    if (_meta->head_total_rows == 0) //the seis head rows is 0, we remove all the head directory
            {
        if (hdfsDelete(_fs, (_hdfs_data_name + "/" + _meta->head_dir_uuid_array[0]).c_str(), 1)
                == -1) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_DELETE, errno);
            return false;
        }
        delete[] _meta->head_dir_front_array;
        delete[] _meta->head_dir_rows_array;
        delete[] _meta->head_dir_uuid_array[0];
        delete[] _meta->head_dir_uuid_array;

        _meta->head_dir_front_array = NULL;
        _meta->head_dir_rows_array = NULL;
        _meta->head_dir_uuid_array = NULL;
        _meta->head_dir_size = 0;
    }

    //get the merged seis last head directory information
    std::string src_last_head_dir = hdfs_src_data_name + "/"
            + src_meta->head_dir_uuid_array[src_meta->head_dir_size - 1];
    int64_t last_head_dir_file_size = GetTotalFileSize(_fs, src_last_head_dir + "/" + "Write_Data");
    if (last_head_dir_file_size == -1) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_LISTDIRECTORY, errno);
        return false;
    }

    int64_t cache_size = 0;
    if (_meta->head_placement == BY_COLUMN) {
        cache_size = GetTotalFileSize(_fs, src_last_head_dir + "/" + "Cache_Data");
        if (cache_size == -1) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_LISTDIRECTORY,
            errno);
            return false;
        }
    }
    src_meta->head_dir_rows_array[src_meta->head_dir_size - 1] = (last_head_dir_file_size
            + cache_size) / src_meta->head_row_bytes;

    //logic merge seis
    for (int64_t i = 0; i < src_meta->head_dir_size; ++i) {
        std::string old_path = hdfs_src_data_name + "/" + src_meta->head_dir_uuid_array[i];
        std::string new_path = _hdfs_data_name + "/" + src_meta->head_dir_uuid_array[i];
        int rst = hdfsRename(_fs, old_path.c_str(), new_path.c_str());
        if (rst == -1) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_RENAME, errno);
            return false;
        }
    }

    //delete old head directory information, use new head directory information
    int64_t new_head_dir_size = _meta->head_dir_size + src_meta->head_dir_size;
    char** new_head_dir_uuid_array = new char*[new_head_dir_size];

    for (int64_t i = 0; i < _meta->head_dir_size; ++i) {
        new_head_dir_uuid_array[i] = _meta->head_dir_uuid_array[i];
    }
    for (int64_t i = _meta->head_dir_size, j = 0; j < src_meta->head_dir_size; ++i, ++j) {
        new_head_dir_uuid_array[i] = src_meta->head_dir_uuid_array[j];
        src_meta->head_dir_uuid_array[j] = NULL;
    }
    delete[] _meta->head_dir_uuid_array;
    _meta->head_dir_uuid_array = new_head_dir_uuid_array;

    int64_t* new_head_dir_rows_array = new int64_t[new_head_dir_size];

    if (_meta->head_dir_size > 0) {
        memcpy(new_head_dir_rows_array, _meta->head_dir_rows_array,
                _meta->head_dir_size * sizeof(int64_t));
    }
    memcpy(new_head_dir_rows_array + _meta->head_dir_size, src_meta->head_dir_rows_array,
            src_meta->head_dir_size * sizeof(int64_t));
    delete[] _meta->head_dir_rows_array;
    _meta->head_dir_rows_array = new_head_dir_rows_array;

    int64_t* new_head_dir_front_array = new int64_t[new_head_dir_size];
    for (int64_t i = 0; i < src_meta->head_dir_size; ++i) {
        src_meta->head_dir_front_array[i] += _meta->head_total_rows;
    }
    if (_meta->head_dir_size > 0) {
        memcpy(new_head_dir_front_array, _meta->head_dir_front_array,
                _meta->head_dir_size * sizeof(int64_t));
    }
    memcpy(new_head_dir_front_array + _meta->head_dir_size, src_meta->head_dir_front_array,
            src_meta->head_dir_size * sizeof(int64_t));
    delete[] _meta->head_dir_front_array;
    _meta->head_dir_front_array = new_head_dir_front_array;

    for (int64_t i = 0; i < src_meta->head_dir_size; ++i) {
        _meta->head_total_rows += src_meta->head_dir_rows_array[i];
    }
    _meta->head_dir_size += src_meta->head_dir_size;
    return true;
}

/**
 * MergeTrace
 *
 * @author              weibing
 * @version             0.2.7
 * @param               src_meta the meta of seis to be merged.
 *                      hdfs_src_data_name the root directory of all seis.
 *                      reelMerge the reel merge.
 * @func                merge log data and original write data.
 * @return              return true if merge successfully.
 */
bool SeisMerger::MergeTrace(SharedPtr<MetaSeisHDFS> src_meta,
        const std::string hdfs_src_data_name) {

    if (src_meta->trace_total_rows == 0) //if the merged seis trace rows is 0, we don't need to merge, just remove it
            {
        return true;
    }

    if (_meta->trace_total_rows == 0) //the seis trace rows is 0, we remove all the trace directory
            {
        if (hdfsDelete(_fs, (_hdfs_data_name + "/" + _meta->trace_dir_uuid_array[0]).c_str(), 1)
                == -1) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_DELETE, errno);
            return false;
        }
        delete[] _meta->trace_dir_front_array;
        delete[] _meta->trace_dir_rows_array;
        delete[] _meta->trace_dir_uuid_array[0];
        delete[] _meta->trace_dir_uuid_array;

        _meta->trace_dir_front_array = NULL;
        _meta->trace_dir_rows_array = NULL;
        _meta->trace_dir_uuid_array = NULL;
        _meta->trace_dir_size = 0;
    }

    //get the merged seis last trace directory information
    std::string src_last_trace_dir = hdfs_src_data_name + "/"
            + src_meta->trace_dir_uuid_array[src_meta->trace_dir_size - 1];
    int64_t last_trace_dir_file_size = GetTotalFileSize(_fs,
            src_last_trace_dir + "/" + "Write_Data");
    if (last_trace_dir_file_size == -1) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_LISTDIRECTORY, errno);
        return false;
    }

    src_meta->trace_dir_rows_array[src_meta->trace_dir_size - 1] = last_trace_dir_file_size
            / src_meta->trace_row_bytes;

    //logic merge seis
    for (int64_t i = 0; i < src_meta->trace_dir_size; ++i) {
        std::string old_path = hdfs_src_data_name + "/" + src_meta->trace_dir_uuid_array[i];
        std::string new_path = _hdfs_data_name + "/" + src_meta->trace_dir_uuid_array[i];
        int rst = hdfsRename(_fs, old_path.c_str(), new_path.c_str());
        if (rst == -1) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_RENAME, errno);
            return false;
        }
    }

    //delete old trace directory information, use new trace directory information
    int64_t new_trace_dir_size = _meta->trace_dir_size + src_meta->trace_dir_size;
    char** new_trace_dir_uuid_array = new char*[new_trace_dir_size];

    for (int64_t i = 0; i < _meta->trace_dir_size; ++i) {
        new_trace_dir_uuid_array[i] = _meta->trace_dir_uuid_array[i];
    }
    for (int64_t i = _meta->trace_dir_size, j = 0; j < src_meta->trace_dir_size; ++i, ++j) {
        new_trace_dir_uuid_array[i] = src_meta->trace_dir_uuid_array[j];
        src_meta->trace_dir_uuid_array[j] = NULL;
    }
    delete[] _meta->trace_dir_uuid_array;
    _meta->trace_dir_uuid_array = new_trace_dir_uuid_array;

    int64_t* new_trace_dir_rows_array = new int64_t[new_trace_dir_size];

    if (_meta->trace_dir_size > 0) {
        memcpy(new_trace_dir_rows_array, _meta->trace_dir_rows_array,
                _meta->trace_dir_size * sizeof(int64_t));
    }
    memcpy(new_trace_dir_rows_array + _meta->trace_dir_size, src_meta->trace_dir_rows_array,
            src_meta->trace_dir_size * sizeof(int64_t));
    delete[] _meta->trace_dir_rows_array;
    _meta->trace_dir_rows_array = new_trace_dir_rows_array;

    int64_t* new_trace_dir_front_array = new int64_t[new_trace_dir_size];
    for (int64_t i = 0; i < src_meta->trace_dir_size; ++i) {
        src_meta->trace_dir_front_array[i] += _meta->trace_total_rows;
    }
    if (_meta->trace_dir_size > 0) {
        memcpy(new_trace_dir_front_array, _meta->trace_dir_front_array,
                _meta->trace_dir_size * sizeof(int64_t));
    }
    memcpy(new_trace_dir_front_array + _meta->trace_dir_size, src_meta->trace_dir_front_array,
            src_meta->trace_dir_size * sizeof(int64_t));
    delete[] _meta->trace_dir_front_array;
    _meta->trace_dir_front_array = new_trace_dir_front_array;

    for (int64_t i = 0; i < src_meta->trace_dir_size; ++i) {
        _meta->trace_total_rows += src_meta->trace_dir_rows_array[i];
    }
    _meta->trace_dir_size += src_meta->trace_dir_size;
    return true;
}

NAMESPACE_END_SEISFS
}
