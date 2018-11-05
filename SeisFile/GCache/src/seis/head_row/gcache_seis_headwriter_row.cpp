/*
 * gcache_seis_headwriter_row.cpp
 *
 *  Created on: Jul 19, 2016
 *      Author: wb
 */

#include "gcache_seis_headwriter_row.h"

//extern int GCACHE_seis_errno;
NAMESPACE_BEGIN_SEISFS
namespace file {
/**
 * SeisHeadWriterRow
 *
 * @author              weibing
 * @version             0.2.7
 * @param               fs is the hdfs file system handle.
 *                      hdfs_data_name is the full name of seis.
 *                      meta is the meta of seis.
 * @func                construct a object.
 * @return              no return.
 */
SeisHeadWriterRow::SeisHeadWriterRow(hdfsFS fs, const std::string& hdfs_data_name,
        SharedPtr<MetaSeisHDFS> meta, StorageSeis* storage_seis, std::string data_name) {

    _hdfs_data_head_name = "";
    _hdfs_data_name = hdfs_data_name;
    _fs = fs;
    _meta = meta;
    _head_fd = NULL;
    std::string head_dir_uuid = meta->head_dir_uuid_array[meta->head_dir_size - 1];
    _head_write_dir = _hdfs_data_name + "/" + head_dir_uuid + "/Write_Data";
    _head_current_row = 0;
    _is_close_flag = false;
    _data_name = data_name;
    _storage_seis = storage_seis;
}

/**
 * ~SeisHeadWriterRow
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                destruct a object.
 * @return              no return.
 */
SeisHeadWriterRow::~SeisHeadWriterRow() {
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
bool SeisHeadWriterRow::Close() {
    if (!_is_close_flag) {
        if (-1 == hdfsCloseFile(_fs, _head_fd)) {
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
 * @param               head is the head data need to be written.
 * @func                append a head.
 * @return              return true if write successfully, else return false.
 */
bool SeisHeadWriterRow::Write(const void* head) {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }

    if (_head_current_row == _meta->file_max_rows) //current file is full, create a new file for writing.
            {
        if (hdfsCloseFile(_fs, _head_fd) == -1) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
            return false;
        }
        _hdfs_data_head_name = _head_write_dir + "/" + GetFormatCurrentMillis() + "_Seishead";
        _head_fd = hdfsOpenFile(_fs, _hdfs_data_head_name.c_str(),
        O_WRONLY | O_APPEND, 0, REPLICATION, 0);
        if (_head_fd == NULL) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
            return false;
        }
        hdfsChmod(_fs, _hdfs_data_head_name.c_str(), 0644);
        _head_current_row = 0;
    }
    int64_t ret_headlen = 0;
    ret_headlen = SafeWrite(_fs, _head_fd, (char*) head, _meta->head_row_bytes);
    if (ret_headlen != _meta->head_row_bytes) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_WRITE, errno);
        return false;
    }
    _head_current_row++;              //the number of current file rows increase
    _meta->head_total_rows++;           //the number of total head rows increase
    _meta->head_dir_rows_array[_meta->head_dir_size - 1]++; //the number of last head directory rows increase

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
int64_t SeisHeadWriterRow::Pos() {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return -1;
    }

    return _meta->head_total_rows;
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
bool SeisHeadWriterRow::Sync() {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }

    if (hdfsSync(_fs, _head_fd) == -1) {
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
bool SeisHeadWriterRow::Init() {
    std::string last_file_name;
    int64_t last_file_size;
    GetLastFileInfo(_fs, _head_write_dir, last_file_name, last_file_size);

    if (last_file_name == "-1")      //failed to get directory information
            {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_LISTDIRECTORY, errno);
        return false;
    } else if (last_file_name == "0") //there is no file for writing, create a new file name
            {
        _hdfs_data_head_name = _head_write_dir + "/" + GetFormatCurrentMillis() + "_Seishead";
        _head_current_row = 0;
    } else                          //get last file for writing
    {
        _hdfs_data_head_name = last_file_name;
        _head_current_row = last_file_size / _meta->head_row_bytes;
    }

    _head_fd = hdfsOpenFile(_fs, _hdfs_data_head_name.c_str(),
    O_WRONLY | O_APPEND, 0, REPLICATION, 0);
    if (_head_fd == NULL) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
        return false;
    }
    hdfsChmod(_fs, _hdfs_data_head_name.c_str(), 0644);
    _is_close_flag = false;
    return true;
}

bool SeisHeadWriterRow::Truncate(int64_t heads) {
    if (!Close()) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
        return false;
    }

    SeisTruncater* truncater = new SeisTruncater(_fs, _hdfs_data_name, _meta);
    if (!truncater->TruncateHead(heads)) {
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
