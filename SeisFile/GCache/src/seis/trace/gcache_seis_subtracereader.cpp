#include "gcache_seis_subtracereader.h"

#define PRINTL printf("file : %s, line : %d\n", __FILE__, __LINE__);fflush(stdout);

//extern int GCACHE_seis_errno;
NAMESPACE_BEGIN_SEISFS
namespace file {
/**
 * SeisTraceUpdater
 *
 * @author              weibing
 * @version             0.2.7
 * @param               fs is the hdfs file system handle.
 *                      hdfs_data_name is the full name of seis.
 *                      meta is the meta of seis.
 *                      trace_dir_idx is the index of this sub trace updater.
 * @func                construct a object.
 * @return              no return.
 */
SeisSubTraceReader::SeisSubTraceReader(hdfsFS fs, const std::string& hdfs_data_name,
        SharedPtr<MetaSeisHDFS> meta, int64_t trace_dir_idx) {
    _fs = fs;
    _meta = meta;
    _hdfs_data_name = hdfs_data_name;
    _is_close_flag = false;
    _trace_dir_idx = trace_dir_idx;
    _trace_dir_uuid = _meta->trace_dir_uuid_array[trace_dir_idx];
    _update_dir_num = -1;

}

/**
 * ~SeisSubTraceReader
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                destruct a object.
 * @return              no return.
 */
SeisSubTraceReader::~SeisSubTraceReader() {
    Close();
}

/**
 * Get
 *
 * @author              weibing
 * @version             0.2.7
 * @param               trace_idx is the index of trace.
 *                      trace is the trace to be read.
 * @func                get a trace by trace index.
 * @return              return true if get successfully, else return false.
 */
bool SeisSubTraceReader::Get(int64_t trace_idx, void* trace) {

    int64_t trace_num = _meta->trace_dir_rows_array[_trace_dir_idx];
    if (trace_idx >= trace_num) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PARAMERR;
        return false;
    }
    if (_update_dir_num < 0 || _update_dir_num != trace_idx / _meta->update_block_num) {
        _update_dir_num = trace_idx / _meta->update_block_num;
        if(_idx_hash_map.size()>=HASH_MAP_MAX_SIZE){
            _idx_hash_map.clear();
        }
        if(!LoadIndex()){
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
            return false;
        }
    }

    unordered_map<int64_t, pair<int, int64_t>>::iterator rst = _idx_hash_map.find(trace_idx);
    if (rst != _idx_hash_map.end())  //the require trace is in log
            {
        int upd_dat_fd_index = rst->second.first; //indicate the updated data file handle
        int64_t upd_dat_offset = _meta->trace_row_bytes
                * (rst->second.second % _meta->file_max_rows); //indicate the offset of the trace that user need
        hdfsFile fd = GetFileHandle(_fs, _upd_dat_fd_vector, _upd_file_number_cache,
                upd_dat_fd_index);
        if (fd == NULL) {
            GCACHE_seis_errno = GCACHE_SEIS_ERR_GETFILEHANDLE;
            return false;
        }

        int ret_seek = hdfsSeek(_fs, fd, upd_dat_offset);
        if (ret_seek == -1) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_SEEK, errno);
            return false;
        }

        int64_t ret_read = SafeRead(_fs, fd, (char*) trace, _meta->trace_row_bytes);
        if (ret_read != _meta->trace_row_bytes) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
            return false;
        }
    } else  //the require trace is in original write file
    {
        int64_t file_idx = trace_idx / _meta->file_max_rows; //indicate the index of file
        int64_t offset = _meta->trace_row_bytes * (trace_idx % _meta->file_max_rows); //read require trace from this offset
        hdfsFile fd = GetFileHandle(_fs, _wrt_dat_fd_vector, _wrt_file_number_cache, file_idx);
        if (fd == NULL) {
            GCACHE_seis_errno = GCACHE_SEIS_ERR_GETFILEHANDLE;
            return false;
        }

        int retSeek = hdfsSeek(_fs, fd, offset);
        if (retSeek == -1) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_SEEK, errno);
            return false;
        }

        int64_t ret_read = SafeRead(_fs, fd, (char*) trace, _meta->trace_row_bytes);
        if (ret_read != _meta->trace_row_bytes) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
            return false;
        }
    }
    return true;
}

/**
 * Close
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                close file.
 * @return              return true if close successfully, else return false.
 */
bool SeisSubTraceReader::Close() {
    if (!_is_close_flag) {
        if (!CloseAllFile(_fs, _upd_dat_fd_vector) || !CloseAllFile(_fs, _wrt_dat_fd_vector)) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
            return false;
        }
        _idx_hash_map.clear();
        _upd_dat_fd_vector.clear();
        _wrt_dat_fd_vector.clear();
        _upd_file_number_cache.clear();
        _wrt_file_number_cache.clear();
        _is_close_flag = true;
    }
    return true;
}

/**
 * Init
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                preliminary work for write data.
 * @return              return true if init successfully, else return false.
 */
bool SeisSubTraceReader::Init() {
    //index init
//    std::vector<std::pair<int64_t, std::pair<int, int64_t>>> index_vector;
    std::string trace_uuid_dir = _hdfs_data_name + "/" + _trace_dir_uuid;
//    std::vector<std::pair<int64_t, int64_t>> trun_vector;
//
//    if (!GetIndexInfo(_fs, trace_uuid_dir, _upd_dat_fd_vector, index_vector)) //get all the index information, these index information is the index of updated trace
//            {
//        return false;
//    }
//    if (!GetTruncateInfo(_fs, trace_uuid_dir, trun_vector)) //get all the truncate information
//            {
//        return false;
//    }
//
//    FilterTruncatedData(_idx_hash_map, index_vector, trun_vector); //filter the unavailable index information, index hash map store all the available index information

    std::string trace_wrt_dat_dir = trace_uuid_dir + "/Write_Data";
    if (!OpenAllFile(_fs, trace_wrt_dat_dir, _wrt_dat_fd_vector)) //open all the original write file
            {
        return false;
    }
    return true;
}

bool SeisSubTraceReader::GetLocation(const int64_t trace_idx, std::vector<std::string> &hostname) {
    hostname.clear();
    int64_t trace_num = _meta->trace_dir_rows_array[_trace_dir_idx];
    if (trace_idx >= trace_num) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PARAMERR;
        return false;
    }
    char *** hostInfos;
    unordered_map<int64_t, pair<int, int64_t>>::iterator rst = _idx_hash_map.find(trace_idx);
    if (rst != _idx_hash_map.end())  //the require trace is in log
            {
        int upd_dat_fd_index = rst->second.first; //indicate the updated data file handle
        int64_t upd_dat_offset = _meta->trace_row_bytes
                * (rst->second.second % _meta->file_max_rows); //indicate the offset of the trace that user need
        std::string filePath = _upd_dat_fd_vector[upd_dat_fd_index].file_name;
        hostInfos = hdfsGetHosts(_fs, _upd_dat_fd_vector[upd_dat_fd_index].file_name.c_str(),
                upd_dat_offset, 1);

    } else  //the require trace is in original write file
    {
        int64_t file_idx = trace_idx / _meta->file_max_rows; //indicate the index of file
        int64_t offset = _meta->trace_row_bytes * (trace_idx % _meta->file_max_rows); //read require trace from this offset

        hostInfos = hdfsGetHosts(_fs, _wrt_dat_fd_vector[file_idx].file_name.c_str(), offset, 1);
    }

    if (hostInfos == NULL) {
        return false;
    }
    if (hostInfos[0] != NULL) {
        int j = 0;
        while (hostInfos[0][j] != NULL) {
            hostname.push_back(hostInfos[0][j]);
            j++;
        }
        if (j == 0) {
            return false;
        }
    } else {
        return false;
    }
    return true;
}

bool SeisSubTraceReader::LoadIndex() {
    std::vector<std::pair<int64_t, std::pair<int, int64_t>>> index_vector;
    std::string trace_uuid_dir = _hdfs_data_name + "/" + _trace_dir_uuid;
    std::vector<std::pair<int64_t, int64_t>> trun_vector;

    if (!GetIndexInfo(_fs, trace_uuid_dir, _upd_dat_fd_vector, index_vector, _update_dir_num)) //get all the index information, these index information is the index of updated trace
            {
        return false;
    }
    if (!GetTruncateInfo(_fs, trace_uuid_dir, trun_vector)) //get all the truncate information
            {
        return false;
    }

    FilterTruncatedData(_idx_hash_map, index_vector, trun_vector); //filter the unavailable index information, index hash map store all the available index information

    return true;
}

NAMESPACE_END_SEISFS
}
