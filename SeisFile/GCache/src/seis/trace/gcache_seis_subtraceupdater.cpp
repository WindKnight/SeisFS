#include "gcache_seis_subtraceupdater.h"

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
SeisSubTraceUpdater::SeisSubTraceUpdater(hdfsFS fs, const std::string& hdfs_data_name,
        SharedPtr<MetaSeisHDFS> meta, int64_t trace_dir_idx) {
    _fs = fs;
    _meta = meta;
    _hdfs_data_name = hdfs_data_name;
    _is_close_flag = false;
    _current_trace_idx_fd = NULL;
    _current_trace_dat_fd = NULL;
    _trace_current_row = 0;
    _trace_dir_uuid = _meta->trace_dir_uuid_array[trace_dir_idx];
    _trace_dir_idx = trace_dir_idx;
    _update_dir_num = -1;
}

/**
 * ~SeisSubTraceUpdater
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                destruct a object.
 * @return              no return.
 */
SeisSubTraceUpdater::~SeisSubTraceUpdater() {
    Close();
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
bool SeisSubTraceUpdater::Close() {
    if (!_is_close_flag) {
        if (_current_trace_idx_fd != NULL) {
            if (-1 == hdfsCloseFile(_fs, _current_trace_idx_fd)) {
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
                return false;
            }
        }
        if (_current_trace_dat_fd != NULL) {
            if (-1 == hdfsCloseFile(_fs, _current_trace_dat_fd)) {
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
                return false;
            }
        }
        _is_close_flag = true;
    }
    return true;
}

/**
 * Put
 *
 * @author              weibing
 * @version             0.2.7
 * @param               trace_idx is the index of trace.
 *                      trace is the trace data need to be written.
 * @func                update a trace by trace index.
 * @return              return true if put successfully, else return false.
 */
bool SeisSubTraceUpdater::Put(int64_t trace_idx, const void* trace) {
    int64_t trace_num = _meta->trace_dir_rows_array[_trace_dir_idx];
    if (trace_idx >= trace_num) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PARAMERR;
        return false;
    }

    if (_update_dir_num < 0) {
        _update_dir_num = trace_idx / _meta->update_block_num;
    }

    if (_trace_current_row == _meta->file_max_rows || _current_trace_idx_fd == NULL) //if the file is full or first append updated a trace, need to create a new file
    {

        if (_trace_current_row == _meta->file_max_rows
                || _update_dir_num != trace_idx / _meta->update_block_num) //current file is full, close current file
                        {
            int idx_rst = hdfsCloseFile(_fs, _current_trace_idx_fd);
            if (idx_rst == -1) {
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
                return false;
            }

            int dat_rst = hdfsCloseFile(_fs, _current_trace_dat_fd);
            if (dat_rst == -1) {
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
                return false;
            }
            _update_dir_num = trace_idx / _meta->update_block_num;
        }

        if(_trace_idx_map.find(_update_dir_num)==_trace_idx_map.end() || _trace_current_row == _meta->file_max_rows){
            std::string upd_file_name = GetUpdateFileName();
            std::string trace_updidx_name = _hdfs_data_name + "/" + _trace_dir_uuid
                    + "/Update_Index/No_" + Int642Str(trace_idx / _meta->update_block_num) + "/";
            std::string trace_upddat_name = _hdfs_data_name + "/" + _trace_dir_uuid + "/Update_Data/No_"
                    + Int642Str(trace_idx / _meta->update_block_num) + "/";

            if (hdfsCreateDirectory(_fs, trace_updidx_name.c_str()) == -1) {
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
                return false;
            }

            if (hdfsCreateDirectory(_fs, trace_upddat_name.c_str()) == -1) {
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
                return false;
            }

            _trace_updidx_name = trace_updidx_name + upd_file_name;
            _trace_upddat_name = trace_upddat_name + upd_file_name;
            _trace_idx_map[_update_dir_num]=_trace_updidx_name;
            _trace_dat_map[_update_dir_num]=_trace_upddat_name;
            _trace_current_row_map[_update_dir_num]=0;
            _trace_current_row = 0;
        }else{
            _trace_updidx_name=_trace_idx_map[_update_dir_num];
            _trace_upddat_name=_trace_dat_map[_update_dir_num];
            _trace_current_row = _trace_current_row_map[_update_dir_num];
        }


        _current_trace_dat_fd = hdfsOpenFile(_fs, _trace_upddat_name.c_str(),
        O_WRONLY | O_APPEND, 0, REPLICATION, 0); //create new updated file
        if (_current_trace_dat_fd == NULL) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
            return false;
        }

        _current_trace_idx_fd = hdfsOpenFile(_fs, _trace_updidx_name.c_str(),
        O_WRONLY | O_APPEND, 0, REPLICATION, 0); //create new index file
        if (_current_trace_idx_fd == NULL) {
            hdfsCloseFile(_fs, _current_trace_dat_fd);
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
            return false;
        }

        hdfsChmod(_fs, _trace_updidx_name.c_str(), 0644);
        hdfsChmod(_fs, _trace_upddat_name.c_str(), 0644);
    }

    int trace_idx_bytes = sizeof(trace_idx);
    int ret_idx = hdfsWrite(_fs, _current_trace_idx_fd, &trace_idx, trace_idx_bytes);
    if (ret_idx != trace_idx_bytes) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_WRITE, errno);
        return false;
    }

    int64_t ret_trace = SafeWrite(_fs, _current_trace_dat_fd, (char*) trace,
            _meta->trace_row_bytes);
    if (ret_trace != _meta->trace_row_bytes) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_WRITE, errno);
        return false;
    }

    _trace_current_row++;
    _trace_current_row_map[_update_dir_num]++;
    return true;
}

/**
 * Sync
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                transfers all modified in-core data of the file to the disk device where that file resides.
 * @return              return true if sync successfully, else return false.
 */
bool SeisSubTraceUpdater::Sync() {
    int idx_rst = hdfsSync(_fs, _current_trace_idx_fd);
    if (idx_rst == -1) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_SYNC, errno);
        return false;
    }

    int dat_rst = hdfsSync(_fs, _current_trace_dat_fd);
    if (dat_rst == -1) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_SYNC, errno);
        return false;
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
bool SeisSubTraceUpdater::Init() {
    return true;
}

NAMESPACE_END_SEISFS
}
