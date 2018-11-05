/*
 * gcache_seis_tracewriter.cpp
 *
 *  Created on: Jul 19, 2016
 *      Author: wb
 */
#include "gcache_seis_tracewriter.h"

//extern int GCACHE_seis_errno;
NAMESPACE_BEGIN_SEISFS
namespace file {
/**
 * SeisTraceWriter
 *
 * @author              weibing
 * @version             0.2.7
 * @param               fs is the hdfs file system handle.
 *                      hdfs_data_name is the full name of seis.
 *                      meta is the meta of seis.
 * @func                construct a object.
 * @return              no return.
 */
SeisTraceWriter::SeisTraceWriter(hdfsFS fs, const std::string& hdfs_data_name,
        SharedPtr<MetaSeisHDFS> meta, StorageSeis* storage_seis, std::string data_name) {

    _hdfs_data_trace_name = "";
    _hdfs_data_name = hdfs_data_name;
    _fs = fs;
    _meta = meta;
    _trace_fd = NULL;
    std::string trace_dir_uuid = meta->trace_dir_uuid_array[meta->trace_dir_size - 1];
    _trace_write_dir = _hdfs_data_name + "/" + trace_dir_uuid + "/Write_Data";
    _trace_current_row = 0;
    _is_close_flag = false;
    _data_name = data_name;
    _storage_seis = storage_seis;

}

/**
 * ~SeisTraceWriter
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                destruct a object.
 * @return              no return.
 */
SeisTraceWriter::~SeisTraceWriter() {
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
bool SeisTraceWriter::Close() {
    if (!_is_close_flag) {
        if (-1 == hdfsCloseFile(_fs, _trace_fd)) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
            return false;
        }
        _is_close_flag = true;
    }
    return true;
}

/**
 * Write
 *
 * @author              weibing
 * @version             0.2.7
 * @param               trace is the trace data need to be written.
 * @func                append a trace.
 * @return              return true if write successfully, else return false.
 */
bool SeisTraceWriter::Write(const void * trace) {

    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }

    if (_trace_current_row == _meta->file_max_rows) //current file is full, create a new file for writing.
            {
        if (hdfsCloseFile(_fs, _trace_fd) == -1) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
            return false;
        }
        _hdfs_data_trace_name = _trace_write_dir + "/" + GetFormatCurrentMillis() + "_SeisTrace";
        _trace_fd = hdfsOpenFile(_fs, _hdfs_data_trace_name.c_str(),
        O_WRONLY | O_APPEND, 0, REPLICATION, 0);
        if (_trace_fd == NULL) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
            return false;
        }
        hdfsChmod(_fs, _hdfs_data_trace_name.c_str(), 0644);
        _trace_current_row = 0;
    }
    int64_t ret_tracelen = 0;
    ret_tracelen = SafeWrite(_fs, _trace_fd, (char*) trace, _meta->trace_row_bytes);
    if (ret_tracelen != _meta->trace_row_bytes) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_WRITE, errno);
        return false;
    }
    _trace_current_row++;             //the number of current file rows increase
    _meta->trace_total_rows++;         //the number of total trace rows increase
    _meta->trace_dir_rows_array[_meta->trace_dir_size - 1]++; //the number of last trace directory rows increase

    return true;
}

/**
 * Pos
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                get the position that data is written to.
 * @return              return the position that data is written to.
 */
int64_t SeisTraceWriter::Pos() {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return -1;
    }

    return _meta->trace_total_rows;
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
bool SeisTraceWriter::Sync() {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }

    if (hdfsSync(_fs, _trace_fd) == -1) {
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
bool SeisTraceWriter::Init() {
    std::string last_file_name;
    int64_t last_file_size;
    GetLastFileInfo(_fs, _trace_write_dir, last_file_name, last_file_size);

    if (last_file_name == "-1")       //failed to get directory information
            {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_LISTDIRECTORY, errno);
        return false;
    } else if (last_file_name == "0") //there is no file for writing, create a new file name
            {
        _hdfs_data_trace_name = _trace_write_dir + "/" + GetFormatCurrentMillis() + "_SeisTrace";
        _trace_current_row = 0;
    } else                           //get last file for writing
    {
        _hdfs_data_trace_name = last_file_name;
        _trace_current_row = last_file_size / _meta->trace_row_bytes;
    }

    _trace_fd = hdfsOpenFile(_fs, _hdfs_data_trace_name.c_str(),
    O_WRONLY | O_APPEND, 0, REPLICATION, 0);
    if (_trace_fd == NULL) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
        return false;
    }
    hdfsChmod(_fs, _hdfs_data_trace_name.c_str(), 0644);
    return true;
}

bool SeisTraceWriter::Truncate(int64_t traces) {
    if (!Close()) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
        return false;
    }
    SeisTruncater* truncater = new SeisTruncater(_fs, _hdfs_data_name, _meta);
    if (!truncater->TruncateTrace(traces)) {
        delete truncater;
        return false;
    }
    delete truncater;

    if (!_storage_seis->PutMeta(_meta, _data_name)) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PUTMETA;
        return false;
    }

    TouchSeisModifyTime(_fs, _hdfs_data_name);
    return Init();
}

NAMESPACE_END_SEISFS
}
