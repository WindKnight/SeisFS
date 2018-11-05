/*
 * gcache_seis_headwriter_column.cpp
 *
 *  Created on: Jul 19, 2016
 *      Author: wb
 */

#include "gcache_seis_headwriter_column.h"

//#define NEXTINDEX(cur,range,size) ((cur)+(range)*(size))   //same position in next line(range is line column)
//#define SETPOS(offset,range,size) ((int64_t)((offset)/(range)) * ((size)*(range)) +(((offset) % (range)) * (size)))

//extern int GCACHE_seis_errno;
NAMESPACE_BEGIN_SEISFS
namespace file {
SeisHeadWriterColumn::SeisHeadWriterColumn(hdfsFS fs, const std::string& hdfs_data_name,
        SharedPtr<MetaSeisHDFS> meta, StorageSeis* storage_seis, std::string data_name) {

    _hdfs_data_head_name = "";
    _hdfs_data_name = hdfs_data_name;
    _fs = fs;
    _meta = meta;
    _head_fd = NULL;
    _head_cache_fd = NULL;
    std::string head_dir_uuid = meta->head_dir_uuid_array[meta->head_dir_size - 1];
    _head_write_dir = _hdfs_data_name + "/" + head_dir_uuid + "/Write_Data";
    _head_cache_dir = _hdfs_data_name + "/" + head_dir_uuid + "/Cache_Data";
    _hdfs_head_cache_name = _head_cache_dir + "/Cache";
    _head_current_row = 0;
    _is_close_flag = false;

    _pid = 0;
    _cnt = 0;
    _column_cache1 = new char[_meta->column_turn * _meta->head_row_bytes];
    _column_cache2 = new char[_meta->column_turn * _meta->head_row_bytes];
//    _cur_cache = _column_cache1;
    _first_flush = true;
    _thread_false = false;
    _cache_flag = false;
    pthread_mutex_init(&_thread_mutex, NULL);

    _data_name = data_name;
    _storage_seis = storage_seis;
}

SeisHeadWriterColumn::~SeisHeadWriterColumn() {
//    _meta->modify_time_ms = GetCurrentMillis();
    if (!_is_close_flag) {
        Close();
    }
    delete[] _column_cache1;
    delete[] _column_cache2;
    pthread_mutex_destroy(&_thread_mutex);
}

/**
 * Append a trace head
 */
bool SeisHeadWriterColumn::Write(const void * head) {

    //---------ice 每次写入成功需要以下操作,当前写入的行数自增
    //    _meta->head_total_rows++;
    //    _meta->head_dir_rows_array[_meta->head_dir_size-1]++;

    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }
    if (_thread_false == true) {
        Err("flush thread error.\n");
        return false;
    }
    char* temp_cur_cache = _column_cache1;  //get current cache pointer
    char* temp_cur_head = (char*) head;
    // for each key feature in head, copy to the transposed position in cache
    for (uint32_t i = 0; i < _meta->head_length; i++) {
        temp_cur_cache += _meta->head_sizes[i] * _cnt;
        memcpy(temp_cur_cache, temp_cur_head, _meta->head_sizes[i]);
        temp_cur_cache += _meta->head_sizes[i] * (_meta->column_turn - _cnt);
        temp_cur_head += _meta->head_sizes[i];
    }
    _cnt++;

    if (_cnt == _meta->column_turn) {

        //Serial method
//        if (_head_current_row == _meta->file_exact_rows) {
//            hdfsCloseFile(_fs, _head_fd);
//            _hdfs_data_head_name = _head_write_dir + "/" + GetFormatCurrentMillis() + "_Seishead";
//            _head_fd = hdfsOpenFile(_fs, _hdfs_data_head_name.c_str(),
//            O_WRONLY | O_APPEND, 0, REPLICATION, 0);
//            if (_head_fd == NULL) {
//                return false;
//            }
//            hdfsChmod(_fs, _hdfs_data_head_name.c_str(), 0644);
//            _head_current_row = 0;
//        }
//        int64_t write_size = _meta->column_turn * _meta->head_row_bytes;
//        if (SafeWrite(_fs, _head_fd, (char*)_column_cache1, write_size) != write_size) {
//            cout<<__LINE__<<endl;
//            cout<<hdfsGetLastError()<<endl;
//            return false;
//        }
//        _head_current_row += _meta->column_turn;
//        if (_cache_flag == true) {
//            if (hdfsDelete(_fs, _hdfs_head_cache_name.c_str(), 0) != 0) {
//                cout<<__LINE__<<endl;
//                return false;
//            }
//            _cache_flag = false;
//        }
//        _cnt = 0;

        //Concurrent method
        //join process
        pthread_mutex_lock(&_thread_mutex);
        char* cache_tmp;
        cache_tmp = _column_cache2;
        _column_cache2 = _column_cache1;
        _column_cache1 = cache_tmp;
        //start the process of flushing
        pthread_create(&_pid, NULL, Flush, (void*) this);
        pthread_detach(_pid);
        _cnt = 0;
    }
    _meta->head_total_rows++;
    _meta->head_dir_rows_array[_meta->head_dir_size - 1]++;
    return true;
}

/**
 * Returns the position that data is written to.
 */
int64_t SeisHeadWriterColumn::Pos() {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return -1;
    }

    return _meta->head_total_rows;
}

/**
 * Transfers all modified in-core data of the file to the disk device where that file resides.
 */
bool SeisHeadWriterColumn::Sync() {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }
    if (_thread_false == true) {
        Err("flush thread error.\n");
        return false;
    }
    pthread_mutex_lock(&_thread_mutex);
    if (_cnt < _meta->column_turn && _cnt != 0) {
        _cache_flag = true;
        _head_cache_fd = hdfsOpenFile(_fs, _hdfs_head_cache_name.c_str(),
        O_WRONLY, 0, REPLICATION, 0);
        if (hdfsWrite(_fs, _head_cache_fd, (char*) &_cnt, sizeof(int64_t)) != sizeof(int64_t)
                || SafeWrite(_fs, _head_cache_fd, (char*) _column_cache1,
                        _meta->column_turn * _meta->head_row_bytes)
                        != _meta->column_turn * _meta->head_row_bytes) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_SYNC, errno);
            hdfsCloseFile(_fs, _head_cache_fd);
            hdfsDelete(_fs, _hdfs_head_cache_name.c_str(), 0);
            _cache_flag = false;
            pthread_mutex_unlock(&_thread_mutex);
            return false;
        }
        hdfsFlush(_fs, _head_cache_fd);
        hdfsCloseFile(_fs, _head_cache_fd);
    }
    if (hdfsSync(_fs, _head_fd) != 0) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_SYNC, errno);
        pthread_mutex_unlock(&_thread_mutex);
        return false;
    }
    pthread_mutex_unlock(&_thread_mutex);
    return true;
}

bool SeisHeadWriterColumn::Init() {
    std::string last_file_name;
    int64_t last_file_size;
    GetLastFileInfo(_fs, _head_write_dir, last_file_name, last_file_size);

    if (last_file_name == "-1")      //failed to get directory information
            {
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

    //Cache File Handle Init
    if (-1 == hdfsExists(_fs, _head_cache_dir.c_str())) {
        if (-1 == hdfsCreateDirectory(_fs, _head_cache_dir.c_str())) {
            return false;
        }
        _cache_flag = false;
    } else if (0 == hdfsExists(_fs, _hdfs_head_cache_name.c_str())) {
        //Init _cnt
        hdfsFileInfo * cacheFileInfo = hdfsGetPathInfo(_fs, _hdfs_head_cache_name.c_str());
        if (cacheFileInfo == NULL) {
            hdfsCloseFile(_fs, _head_fd);
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_STAT, errno);
            return false;
        }

        //deal with erro file
        int64_t cache_file_size = cacheFileInfo->mSize;
        hdfsFreeFileInfo(cacheFileInfo, 1);
        if (cache_file_size
                != _meta->column_turn * _meta->head_row_bytes + (int64_t) sizeof(int64_t)) {
            if (hdfsDelete(_fs, _hdfs_head_cache_name.c_str(), 0) != 0) {
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_DELETE,
                errno);
                return false;
            }
            _cache_flag = false;
        } else {
            hdfsFile cache_file = hdfsOpenFile(_fs, _hdfs_head_cache_name.c_str(),
            O_RDONLY, 0, REPLICATION, 0);
            int64_t ret = SafeRead(_fs, cache_file, (char*) _cnt, sizeof(int64_t));
            if (ret != sizeof(int64_t)) {
                hdfsCloseFile(_fs, cache_file);
                hdfsCloseFile(_fs, _head_fd);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
                return false;
            }
            ret = SafeRead(_fs, cache_file, _column_cache1, cache_file_size);
            if (ret != cache_file_size) {
                hdfsCloseFile(_fs, cache_file);
                hdfsCloseFile(_fs, _head_fd);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
                return false;
            }
            hdfsCloseFile(_fs, cache_file);
            _cache_flag = true;
        }
    } else {
        _cache_flag = false;
    }
    return true;
}

void* SeisHeadWriterColumn::Flush(void* tmp) {
    SeisHeadWriterColumn* p = (SeisHeadWriterColumn*) tmp;
    if (p->_head_current_row == p->_meta->file_max_rows) {
        hdfsCloseFile(p->_fs, p->_head_fd);
        p->_hdfs_data_head_name = p->_head_write_dir + "/" + GetFormatCurrentMillis() + "_Seishead";
        p->_head_fd = hdfsOpenFile(p->_fs, p->_hdfs_data_head_name.c_str(),
        O_WRONLY | O_APPEND, 0, REPLICATION, 0);
        if (p->_head_fd == NULL) {
//            std::cout<<__LINE__<<std::endl;
            p->_thread_false = true;
            pthread_mutex_unlock(&p->_thread_mutex);
            pthread_exit(NULL);
        }
        hdfsChmod(p->_fs, p->_hdfs_data_head_name.c_str(), 0644);
        p->_head_current_row = 0;
    }
    int64_t write_size = p->_meta->column_turn * p->_meta->head_row_bytes;
    if (hdfsWrite(p->_fs, p->_head_fd, (char*) p->_column_cache2, write_size) != write_size) {
//        std::cout<<__LINE__<<std::endl;
//        std::cout<<hdfsGetLastError()<<std::endl;
        p->_thread_false = true;
        pthread_mutex_unlock(&p->_thread_mutex);
        pthread_exit(NULL);
    }
    p->_head_current_row += p->_meta->column_turn;
    if (p->_cache_flag == true) {
        if (hdfsDelete(p->_fs, p->_hdfs_head_cache_name.c_str(), 0) != 0) {
//            std::cout<<__LINE__<<std::endl;
            p->_thread_false = true;
            pthread_mutex_unlock(&p->_thread_mutex);
            pthread_exit(NULL);
        }
        p->_cache_flag = false;
    }
    pthread_mutex_unlock(&p->_thread_mutex);
    pthread_exit(NULL);
}

/**
 *  Close file.
 */
bool SeisHeadWriterColumn::Close() {
    if (_cnt < _meta->column_turn) {
        _cache_flag = true;
    }
    if (!Sync()) {
        return false;
    }
    hdfsCloseFile(_fs, _head_fd);
    return true;
}

bool SeisHeadWriterColumn::Truncate(int64_t heads) {
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
