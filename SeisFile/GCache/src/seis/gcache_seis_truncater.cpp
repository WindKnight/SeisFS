/*
 * gcache_seis_truncater.cpp
 *
 *  Created on: Jul 19, 2016
 *      Author: wb
 */
#include "gcache_seis_truncater.h"
#include "gcache_seis_error.h"
#include "gcache_io.h"
#include "gcache_hdfs_utils.h"
#include "gcache_seis_utils.h"
#include "string.h"
#include <unistd.h>
#include <iostream>

//extern int GCACHE_seis_errno;
NAMESPACE_BEGIN_SEISFS
namespace file {
/**
 * SeisTruncater
 *
 * @author              weibing
 * @version             0.2.7
 * @param               fs is the hdfs file system handle.
 *                      hdfs_data_name is the full name of seis.
 *                      meta is the meta of seis.
 * @func                construct a object.
 * @return              no return.
 */
SeisTruncater::SeisTruncater(hdfsFS fs, const std::string& hdfs_data_name,
        SharedPtr<MetaSeisHDFS> meta) {
    _fs = fs;
    _hdfs_data_name = hdfs_data_name;
    _meta = meta;
}

/**
 * ~SeisTruncater
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                destruct a object.
 * @return              no return.
 */
SeisTruncater::~SeisTruncater() {
}

/**
 * TruncateHead
 *
 * @author              weibing
 * @version             0.2.7
 * @param               heads the row the file will be truncated to.
 * @func                truncate  head file.
 * @return              return true if truncate successfully, else return false.
 */
bool SeisTruncater::TruncateHead(int64_t heads) {

    if (heads < 0 || heads > _meta->head_total_rows) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PARAMERR;
        return false;
    }

    if (heads == _meta->head_total_rows) {
        return true;
    }

    int64_t head_dir_idx = GetDirIndex(_meta->head_dir_front_array, _meta->head_dir_size, heads);
    int64_t head_dir_front = _meta->head_dir_front_array[head_dir_idx];
    int64_t del_dir_begin = head_dir_idx + 1;
    int64_t new_head_dir_size = head_dir_idx + 1;

    if (heads == head_dir_front) {
        del_dir_begin = head_dir_idx;
        new_head_dir_size = head_dir_idx;
    }

    for (int64_t i = del_dir_begin; i < _meta->head_dir_size; i++) //remove unused directory
            {
        std::string head_dir = _hdfs_data_name + "/" + _meta->head_dir_uuid_array[i];
        if (hdfsDelete(_fs, head_dir.c_str(), 1) == -1) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_DELETE, errno);
            return false;
        }
    }

    if (heads == 0) //all the head directory was removed so create the first directory
            {
        new_head_dir_size = 1;
        std::string head_dir = _hdfs_data_name + "/" + _meta->head_dir_uuid_array[head_dir_idx];
        if (!CreateAllDir(_fs, _hdfs_data_name, _meta->head_dir_uuid_array[head_dir_idx])) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CREATEDIR, errno);
            return false;
        }
    }

    if (heads != head_dir_front) //we need to truncate the corresponding original write file
            {
        switch (_meta->head_placement) {
            case BY_ROW: {
                //find the corresponding file
                int64_t head_idx = heads - head_dir_front;
                int64_t file_idx = head_idx / _meta->file_max_rows;
                int64_t file_end_pos = (head_idx % _meta->file_max_rows) * _meta->head_row_bytes;

                int count = 0;
                std::string dest_dir = _hdfs_data_name + "/"
                        + _meta->head_dir_uuid_array[head_dir_idx] + "/Write_Data";
                hdfsFileInfo * listFileInfo = hdfsListDirectory(_fs, dest_dir.c_str(), &count);
                if (listFileInfo == NULL) {
                    GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_LISTDIRECTORY,
                    errno);
                    return false;
                }

                //remove all the files contain rows bigger than input traces
                for (int i = (int) file_idx + 1; i < count; ++i) {
                    if (hdfsDelete(_fs, listFileInfo[i].mName, 0) == -1) {
                        hdfsFreeFileInfo(listFileInfo, count);
                        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_DELETE,
                        errno);
                        return false;
                    }
                }
                std::string trun_file_name = listFileInfo[file_idx].mName;
                if (hdfsTruncateWait(_fs, listFileInfo[file_idx].mName, file_end_pos) == -1) {
                    hdfsFreeFileInfo(listFileInfo, count);
                    GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_TRUNCATE, errno);
                    return false;
                }
                hdfsFreeFileInfo(listFileInfo, count);

                //store truncate information
                std::string hdfs_trun_file_name = _hdfs_data_name + "/"
                        + _meta->head_dir_uuid_array[head_dir_idx] + "/Truncate_Data" + "/Data";
                hdfsFile fd = hdfsOpenFile(_fs, hdfs_trun_file_name.c_str(),
                O_WRONLY | O_APPEND, 0, REPLICATION, 0);
                if (NULL == fd) {
                    GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
                    return false;
                }

                std::string trun_data = GetFormatCurrentMillis() + "|"
                        + Int642Str(heads - head_dir_front);
                char trun_data_str[TRUN_ROW_SIZE];
                memcpy(trun_data_str, trun_data.c_str(), trun_data.size());
                if (hdfsWrite(_fs, fd, trun_data_str,
                TRUN_ROW_SIZE) != TRUN_ROW_SIZE) {
                    hdfsCloseFile(_fs, fd);
                    GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_WRITE, errno);
                    return false;
                }

                if (hdfsCloseFile(_fs, fd) == -1) {
                    GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
                    return false;
                }

            }
                break;
            case BY_COLUMN: {
                int64_t cur_rows = _meta->head_dir_rows_array[head_dir_idx]; //total row number
                int64_t head_idx = heads - head_dir_front; //number to truncate into
                int64_t cache_num = cur_rows % _meta->column_turn;
                int64_t store_num = cur_rows - cache_num;
                std::string head_cache_name = _hdfs_data_name + "/"
                        + _meta->head_dir_uuid_array[head_dir_idx] + "/Cache_Data/Cache";
                if (head_idx > store_num) { //truncate cachefile
                    int64_t length = _meta->column_turn * _meta->head_row_bytes + sizeof(int64_t);
                    char buffer[length];
                    hdfsFile fd = hdfsOpenFile(_fs, (head_cache_name).c_str(), O_RDONLY, 0,
                            REPLICATION, 0);
                    if (fd == NULL) {
                        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_TRUNCATE,
                        errno);
                        return false;
                    }
                    int64_t ret = SafeRead(_fs, fd, buffer, length);
                    if (ret != length) {
                        hdfsCloseFile(_fs, fd);
                        return false;
                    }
                    hdfsCloseFile(_fs, fd);
                    int64_t left_size = head_idx - store_num;
                    memcpy(buffer, (char*) left_size, sizeof(int64_t));
                    fd = hdfsOpenFile(_fs, (head_cache_name).c_str(), O_WRONLY, 0, REPLICATION, 0);
                    if (fd == NULL) {
                        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_TRUNCATE,
                        errno);
                        return false;
                    }
                    ret = SafeWrite(_fs, fd, buffer, length);
                    if (ret != length) {
                        hdfsCloseFile(_fs, fd);
                        return false;
                    }
                    hdfsCloseFile(_fs, fd);
                } else if (head_idx == store_num) {
                    std::string head_cache_name = _hdfs_data_name + "/"
                            + _meta->head_dir_uuid_array[head_dir_idx] + "/Cache_Data/Cache";
                    if (hdfsDelete(_fs, head_cache_name.c_str(), 0) == -1) {
                        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_DELETE,
                        errno);
                        return false;
                    }
                } else {
                    int64_t head_bytes = _meta->head_row_bytes;
                    int file_idx = head_idx / _meta->file_max_rows;
                    int64_t trun_cache_num = head_idx % _meta->column_turn;
                    int count = 0;
                    std::string dest_dir = _hdfs_data_name + "/"
                            + _meta->head_dir_uuid_array[head_dir_idx] + "/Write_Data";
                    hdfsFileInfo * listFileInfo = hdfsListDirectory(_fs, dest_dir.c_str(), &count);
                    if (listFileInfo == NULL) {
                        return false;
                    }

                    //remove all the files contain rows bigger than input traces
                    for (int i = (int) file_idx + 1; i < count; ++i) {
                        if (hdfsDelete(_fs, listFileInfo[i].mName, 0) == -1) {
                            hdfsFreeFileInfo(listFileInfo, count);
                            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_DELETE,
                            errno);
                            return false;
                        }
                    }

                    //need to recache file in the orignal file ,by zhounan
                    int64_t length = _meta->column_turn * _meta->head_row_bytes;
                    char* head_name = listFileInfo[file_idx].mName;
                    hdfsFile read_head_fd = hdfsOpenFile(_fs, head_name,
                    O_RDONLY, 0,
                    REPLICATION, 0);
                    if (read_head_fd == NULL) {
                        hdfsFreeFileInfo(listFileInfo, count);
                        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE,
                        errno);
                        return false;
                    }
                    int64_t start_offset = trun_cache_num * head_bytes;
                    if (-1 == hdfsSeek(_fs, read_head_fd, start_offset)) {
                        hdfsFreeFileInfo(listFileInfo, count);
                        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_SEEK,
                        errno);
                        return false;
                    }
                    char * cache = new char[length];
                    int64_t ret_read = SafeRead(_fs, read_head_fd, (char*) cache, length);
                    if (ret_read != length) {
                        hdfsFreeFileInfo(listFileInfo, count);
                        delete[] cache;
                        return false;
                    }

                    if (hdfsCloseFile(_fs, read_head_fd) != 0) {
                        delete[] cache;
                        hdfsFreeFileInfo(listFileInfo, count);
                        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE,
                        errno);
                        return false;
                    }

                    hdfsFile write_cache_fd = hdfsOpenFile(_fs, head_cache_name.c_str(),
                    O_WRONLY, 0,
                    REPLICATION, 0);
                    if (write_cache_fd == NULL) {
                        hdfsFreeFileInfo(listFileInfo, count);
                        delete[] cache;
                        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE,
                        errno);
                        return false;
                    }
                    if (sizeof(int64_t)
                            != SafeWrite(_fs, write_cache_fd, (char *) &cache_num,
                                    sizeof(int64_t))) {
                        hdfsFreeFileInfo(listFileInfo, count);
                        delete[] cache;
                        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE,
                        errno);
                        return false;
                    }
                    if (length != SafeWrite(_fs, write_cache_fd, cache, length)) {
                        hdfsFreeFileInfo(listFileInfo, count);
                        delete[] cache;
                        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE,
                        errno);
                        return false;
                    }
                    if (hdfsCloseFile(_fs, write_cache_fd) == -1) {
                        hdfsFreeFileInfo(listFileInfo, count);
                        delete[] cache;
                        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE,
                        errno);
                        return false;
                    }
                    delete[] cache;

                    if (-1 == hdfsTruncateWait(_fs, head_name, trun_cache_num * head_bytes)) {
                        hdfsFreeFileInfo(listFileInfo, count);
                        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_TRUNCATE,
                        errno);
                        return false;
                    }
                    hdfsFreeFileInfo(listFileInfo, count);

                }
                //store truncate information
                std::string hdfs_trun_file_name = _hdfs_data_name + "/"
                        + _meta->head_dir_uuid_array[head_dir_idx] + "/Truncate_Data" + "/Data";
                hdfsFile fd = hdfsOpenFile(_fs, hdfs_trun_file_name.c_str(),
                O_WRONLY | O_APPEND, 0, REPLICATION, 0);
                if (NULL == fd) {
                    GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
                    return false;
                }

                std::string trun_data = GetFormatCurrentMillis() + "|"
                        + Int642Str(heads - head_dir_front);
                char trun_data_str[TRUN_ROW_SIZE];
                memcpy(trun_data_str, trun_data.c_str(), trun_data.size());
                if (hdfsWrite(_fs, fd, trun_data_str,
                TRUN_ROW_SIZE) != TRUN_ROW_SIZE) {
                    hdfsCloseFile(_fs, fd);
                    GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_WRITE, errno);
                    return false;
                }

                if (hdfsCloseFile(_fs, fd) == -1) {
                    GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
                    return false;
                }
            }
                break;
            default: {
                return false;
            }
                break;
        }

    }

    //delete old directory information, use new directory information
    int64_t* new_head_dir_front_array = new int64_t[new_head_dir_size];
    memcpy(new_head_dir_front_array, _meta->head_dir_front_array,
            new_head_dir_size * sizeof(int64_t));
    delete[] _meta->head_dir_front_array;
    _meta->head_dir_front_array = new_head_dir_front_array;

    int64_t* new_head_dir_rows_array = new int64_t[new_head_dir_size];
    memcpy(new_head_dir_rows_array, _meta->head_dir_rows_array,
            new_head_dir_size * sizeof(int64_t));
    delete[] _meta->head_dir_rows_array;
    _meta->head_dir_rows_array = new_head_dir_rows_array;
    _meta->head_dir_rows_array[new_head_dir_size - 1] = heads
            - _meta->head_dir_front_array[new_head_dir_size - 1];

    char **new_head_dir_uuid_array = new char*[new_head_dir_size];
    for (int64_t i = 0; i < new_head_dir_size; i++) {
        new_head_dir_uuid_array[i] = _meta->head_dir_uuid_array[i];
    }
    for (int64_t i = new_head_dir_size; i < _meta->head_dir_size; i++) {
        delete[] _meta->head_dir_uuid_array[i];
    }
    delete[] _meta->head_dir_uuid_array;
    _meta->head_dir_uuid_array = new_head_dir_uuid_array;

    _meta->head_total_rows = heads;
    _meta->head_dir_size = new_head_dir_size;

    return true;
}

/**
 * TruncateTrace
 *
 * @author              weibing
 * @version             0.2.7
 * @param               traces the row the file will be truncated to.
 * @func                truncate  trace file.
 * @return              return true if truncate successfully, else return false.
 */
bool SeisTruncater::TruncateTrace(int64_t traces) {
    if (traces < 0 || traces >= _meta->trace_total_rows) {
        return false;
    }

    int64_t trace_dir_idx = GetDirIndex(_meta->trace_dir_front_array, _meta->trace_dir_size,
            traces);
    int64_t trace_dir_front = _meta->trace_dir_front_array[trace_dir_idx];
    int64_t del_dir_begin = trace_dir_idx + 1;
    int64_t new_trace_dir_size = trace_dir_idx + 1;

    if (traces == trace_dir_front) {
        del_dir_begin = trace_dir_idx;
        new_trace_dir_size = trace_dir_idx;
    }

    for (int64_t i = del_dir_begin; i < _meta->trace_dir_size; i++) //remove unused directory
            {
        std::string trace_dir = _hdfs_data_name + "/" + _meta->trace_dir_uuid_array[i];
        if (hdfsDelete(_fs, trace_dir.c_str(), 1) == -1) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_DELETE, errno);
            return false;
        }
    }

    if (traces == 0) //all the trace directory was removed so create the first directory
            {
        new_trace_dir_size = 1;
        std::string trace_dir = _hdfs_data_name + "/" + _meta->trace_dir_uuid_array[trace_dir_idx];
        if (!CreateAllDir(_fs, _hdfs_data_name, _meta->trace_dir_uuid_array[trace_dir_idx])) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CREATEDIR, errno);
            return false;
        }
    }

    if (traces != trace_dir_front) //we need to truncate the corresponding original write file
            {

        //find the corresponding file
        int64_t trace_idx = traces - trace_dir_front;
        int64_t file_idx = trace_idx / _meta->file_max_rows;
        int64_t file_end_pos = (trace_idx % _meta->file_max_rows) * _meta->trace_row_bytes;

        int count = 0;
        std::string dest_dir = _hdfs_data_name + "/" + _meta->trace_dir_uuid_array[trace_dir_idx]
                + "/Write_Data";
        hdfsFileInfo * listFileInfo = hdfsListDirectory(_fs, dest_dir.c_str(), &count);
        if (listFileInfo == NULL) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_LISTDIRECTORY,
            errno);
            return false;
        }

        //remove all the files contain rows bigger than input traces
        for (int i = (int) file_idx + 1; i < count; ++i) {
            if (hdfsDelete(_fs, listFileInfo[i].mName, 0) == -1) {
                hdfsFreeFileInfo(listFileInfo, count);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_DELETE, errno);
                return false;
            }
        }

        std::string trun_file_name = listFileInfo[file_idx].mName;
        if (hdfsTruncateWait(_fs, listFileInfo[file_idx].mName, file_end_pos) == -1) {
            hdfsFreeFileInfo(listFileInfo, count);
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_TRUNCATE, errno);
            return false;
        }
        hdfsFreeFileInfo(listFileInfo, count);

        //store truncate information
        std::string hdfs_trun_file_name = _hdfs_data_name + "/"
                + _meta->trace_dir_uuid_array[trace_dir_idx] + "/Truncate_Data" + "/Data";
        hdfsFile fd = hdfsOpenFile(_fs, hdfs_trun_file_name.c_str(),
        O_WRONLY | O_APPEND, 0, REPLICATION, 0);
        if (NULL == fd) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
            return false;
        }
        std::string trun_data = GetFormatCurrentMillis() + "|"
                + Int642Str(traces - trace_dir_front);
        char trun_data_str[TRUN_ROW_SIZE];
        memcpy(trun_data_str, trun_data.c_str(), trun_data.size());
        if (hdfsWrite(_fs, fd, trun_data_str, TRUN_ROW_SIZE) != TRUN_ROW_SIZE) {
            hdfsCloseFile(_fs, fd);
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_WRITE, errno);
            return false;
        }

        if (hdfsCloseFile(_fs, fd) == -1) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
            return false;
        }

    }

    //delete old directory information, use new directory information
    int64_t* new_trace_dir_front_array = new int64_t[new_trace_dir_size];
    memcpy(new_trace_dir_front_array, _meta->trace_dir_front_array,
            new_trace_dir_size * sizeof(int64_t));
    delete[] _meta->trace_dir_front_array;
    _meta->trace_dir_front_array = new_trace_dir_front_array;

    int64_t* new_trace_dir_rows_array = new int64_t[new_trace_dir_size];
    memcpy(new_trace_dir_rows_array, _meta->trace_dir_rows_array,
            new_trace_dir_size * sizeof(int64_t));
    delete[] _meta->trace_dir_rows_array;
    _meta->trace_dir_rows_array = new_trace_dir_rows_array;
    _meta->trace_dir_rows_array[new_trace_dir_size - 1] = traces
            - _meta->trace_dir_front_array[new_trace_dir_size - 1];

    char **new_trace_dir_uuid_array = new char*[new_trace_dir_size];
    for (int64_t i = 0; i < new_trace_dir_size; i++) {
        new_trace_dir_uuid_array[i] = _meta->trace_dir_uuid_array[i];
    }
    for (int64_t i = new_trace_dir_size; i < _meta->trace_dir_size; i++) {
        delete[] _meta->trace_dir_uuid_array[i];
    }
    delete[] _meta->trace_dir_uuid_array;
    _meta->trace_dir_uuid_array = new_trace_dir_uuid_array;

    _meta->trace_total_rows = traces;
    _meta->trace_dir_size = new_trace_dir_size;

    return true;
}
NAMESPACE_END_SEISFS
}
