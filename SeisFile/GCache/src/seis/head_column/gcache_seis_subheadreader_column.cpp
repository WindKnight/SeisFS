/*
 * gcache_seis_headreader_column.cpp
 *
 *  Created on: Jul 19, 2016
 *      Author: wb
 */

#include "gcache_seis_subheadreader_column.h"

#define NEXTINDEX(cur,range) ((cur)+(range))   //same position in next line(range is line column)
#define SETPOS1(trace_idx,range,size,rand_idx) ((int64_t)((trace_idx)/(range)) * ((size)) +((rand_idx) * (_meta->head_sizes[0])))
#define SETPOS0(t,range,size) ((int64_t)((t)/(range))*((range) * (size)))

//extern int GCACHE_seis_errno;

NAMESPACE_BEGIN_SEISFS
namespace file {
SeisSubHeadReaderColumn::SeisSubHeadReaderColumn(hdfsFS fs, const std::string& hdfsDataName,
        SharedPtr<MetaSeisHDFS> meta, int64_t head_dir_idx) {
    _fs = fs;
    _meta = meta;
    //------------------ice
    _head_dir_idx = head_dir_idx;
    _head_dir_uuid = _meta->head_dir_uuid_array[head_dir_idx];
    //-------------------
    _is_close_flag = false;

    // 需要修改相应路径   ----------ice
    _hdfs_data_name = hdfsDataName + "/";
    _hdfs_head_cache_name = _hdfs_data_name + _head_dir_uuid + "/Cache_Data/Cache";
    _cache_dat_fd = NULL;
    _key_list = NULL;
    _head_offset = NULL;
    _mem_cache = NULL;
    _mem_cache_begin = -1;
    _cache_file_store_rows = _meta->head_dir_rows_array[_head_dir_idx] % _meta->column_turn;
    _file_store_rows = _meta->head_dir_rows_array[_head_dir_idx] - _cache_file_store_rows;
    _update_dir_num = -1;
}

SeisSubHeadReaderColumn::~SeisSubHeadReaderColumn() {
    delete[] _mem_cache;
    Close();
}

bool SeisSubHeadReaderColumn::Get(int64_t head_idx, void* head) {
    //----------------------ice
    int64_t head_num = _meta->head_dir_rows_array[_head_dir_idx];
    if (head_idx >= head_num) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PARAMERR;
        return false;
    }

    if (_update_dir_num < 0 || _update_dir_num != head_idx / _meta->update_block_num) {
        _update_dir_num = head_idx / _meta->update_block_num;
        if(_idx_hash_map.size()>=HASH_MAP_MAX_SIZE){
            _idx_hash_map.clear();
        }
        if(!LoadIndex()){
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
            return false;
        }
    }

    bool is_last_file_item;
    if (FindIndexItem(head_idx, is_last_file_item)) {
        return GetUpdateData(head);
    } else {
        return GetWriteData(head_idx, head);
    }
    //----------------------
}

/**
 * Return true if there are more traces. The routine usually accompany with NextTrace
 */

bool SeisSubHeadReaderColumn::Close() {

    if (!_is_close_flag) {
        if (!CloseAllFile(_fs, _upd_dat_fd_vector) || !CloseAllFile(_fs, _wrt_dat_fd_vector)) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
            return false;
        }
        hdfsCloseFile(_fs, _cache_dat_fd);
        _idx_hash_map.clear();
        _upd_dat_fd_vector.clear();
        _wrt_dat_fd_vector.clear();
        _upd_file_number_cache.clear();
        _wrt_file_number_cache.clear();
        _is_close_flag = true;

    }
    return true;
}
bool SeisSubHeadReaderColumn::Init() {

    //-----------------------ice
    //index init
    std::vector<std::pair<int64_t, std::pair<int, int64_t>>> index_vector;
    std::string head_uuid_dir = _hdfs_data_name + "/" + _head_dir_uuid;
    std::vector<std::pair<int64_t, int64_t>> trun_vector;

    if (!GetIndexInfo(_fs, head_uuid_dir, _upd_dat_fd_vector, index_vector, 0)) {
        return false;
    }
    if (!GetTruncateInfo(_fs, head_uuid_dir, trun_vector)) {
        return false;
    }
    FilterTruncatedData(_idx_hash_map, index_vector, trun_vector);
    //--------------------------

    std::string head_wrt_dat_dir = head_uuid_dir + "/Write_Data";
    if (!OpenAllFile(_fs, head_wrt_dat_dir, _wrt_dat_fd_vector)) //open all the original write file
            {
        return false;
    }
    if (hdfsExists(_fs, _hdfs_head_cache_name.c_str()) == 0) {
        _cache_dat_fd = hdfsOpenFile(_fs, _hdfs_head_cache_name.c_str(),
        O_RDONLY, 0, REPLICATION, 0);
        if (NULL == _cache_dat_fd) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
            return false;
        }
    }

    return true;
}

//void SeisSubHeadReaderColumn::AddNewIndexItem(int64_t head_idx,
//        hdfsFile upd_dat_fd, int64_t file_rows) {
//    if (upd_dat_fd != NULL) {
//        _idx_hash_map[head_idx] = std::pair<int, int64_t>(
//                (int) _upd_dat_fd_vector.size(), file_rows);
//        _upd_dat_fd_vector.push_back(upd_dat_fd);
//    } else {
//        _idx_hash_map[head_idx] = std::pair<int, int64_t>(
//                (int) _upd_dat_fd_vector.size() - 1, file_rows);
//    }
//}
void SeisSubHeadReaderColumn::AddNewIndexItem(int64_t head_idx, std::string new_file_name,
        int64_t file_rows) {    //ice--change
    if (new_file_name != "") {
        _idx_hash_map[head_idx] = std::pair<int, int64_t>((int) _upd_dat_fd_vector.size(),
                file_rows);
        RichFileHandle rich_fd;
        rich_fd.access_time = GetCurrentMillis();
        rich_fd.fd = NULL;
        rich_fd.file_name = new_file_name;
        _upd_dat_fd_vector.push_back(rich_fd);
    } else {
        _idx_hash_map[head_idx] = std::pair<int, int64_t>((int) _upd_dat_fd_vector.size() - 1,
                file_rows);
    }
}

bool SeisSubHeadReaderColumn::FindIndexItem(int64_t head_idx, bool& is_last_file_item) {
    _hash_find_rst = _idx_hash_map.find(head_idx);
    if (_hash_find_rst != _idx_hash_map.end()) {
        int64_t upd_dat_fd_index = _hash_find_rst->second.first; //indicate the number of file
        is_last_file_item =
                (upd_dat_fd_index == (int64_t) _upd_dat_fd_vector.size() - 1) ? true : false;
        return true;
    }
    is_last_file_item = false;
    return false;
    //5/19 jie
//    return true;
}

bool SeisSubHeadReaderColumn::GetUpdateData(void* head) {
    int upd_dat_fd_index = _hash_find_rst->second.first; //indicate the number of file
    int64_t upd_dat_offset = _meta->head_row_bytes
            * (_hash_find_rst->second.second % _meta->file_max_rows); //指示所在文件位置
    hdfsFile fd = GetFileHandle(_fs, _upd_dat_fd_vector, _upd_file_number_cache, upd_dat_fd_index);
    int ret_seek = hdfsSeek(_fs, fd, upd_dat_offset);
    if (ret_seek == -1) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_SEEK, errno);
        return false;
    }
    if (_key_list != NULL && _key_list->size() > 0 && _key_list->size() < _meta->head_length) //get part of a head row
            {
        char* headArray = new char[_meta->head_row_bytes];
        int64_t ret_read = SafeRead(_fs, fd, headArray, _meta->head_row_bytes);
        if (ret_read != _meta->head_row_bytes) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
            return false;
        }
        int64_t head_offset = 0;
        for (std::list<int>::iterator i = _key_list->begin(); i != _key_list->end(); i++) {
            memcpy((char*) head + head_offset, headArray + _head_offset[(*i)],
                    _meta->head_sizes[(*i)]);
            head_offset += _meta->head_sizes[(*i)];
        }
        delete[] headArray;
    } else {
        if (head == NULL) {
            head = new char[_meta->head_row_bytes];
        }
        int64_t ret_read = SafeRead(_fs, fd, head, _meta->head_row_bytes);
        if (ret_read != _meta->head_row_bytes) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
            return false;
        }
    }

    return true;
}

bool SeisSubHeadReaderColumn::GetWriteData(int64_t head_idx, void* head) {
    if (_mem_cache == NULL) {
        _mem_cache = new char[_meta->column_turn * _meta->head_row_bytes];
    }
    int64_t off_idx = head_idx % _meta->file_max_rows;
    int64_t idx = head_idx / _meta->file_max_rows;
    int64_t cache_off_idx = off_idx % _meta->column_turn;
    if (_mem_cache_begin > head_idx || head_idx > _mem_cache_begin + _meta->column_turn - 1
            || _mem_cache_begin == -1) { //replace mem_cache
        hdfsFile data_head_fd;
        int64_t seek_pos;
        if (head_idx < _file_store_rows) { // data in conventional file
            data_head_fd = GetFileHandle(_fs, _wrt_dat_fd_vector, _wrt_file_number_cache, idx);
            seek_pos = (off_idx - cache_off_idx) * _meta->head_row_bytes;
        } else { //data in cache file
            data_head_fd = _cache_dat_fd;
            seek_pos = sizeof(int64_t);
        }
        if (_key_list != NULL && _key_list->size() > 0 && _key_list->size() < _meta->head_length) {
            int64_t read_offset = 0;
            for (std::list<int>::iterator it = _key_list->begin(); it != _key_list->end(); it++) {
                int retSeek = hdfsSeek(_fs, data_head_fd,
                        seek_pos + _meta->column_turn * _head_offset[*it]);
                if (retSeek == -1) {
                    GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_SEEK, errno);
                    return false;
                }
                int64_t ret_read = SafeRead(_fs, data_head_fd, (void*) (_mem_cache + read_offset),
                        _meta->column_turn * _meta->head_sizes[*it]);
                if (ret_read != _meta->column_turn * _meta->head_sizes[*it]) {
                    GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
                    return false;
                }
                read_offset += ret_read;
            }
        } else {
            int retSeek = hdfsSeek(_fs, data_head_fd, seek_pos);
            if (retSeek == -1) {
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_SEEK, errno);
                return false;
            }
            int64_t ret_read = SafeRead(_fs, data_head_fd, (void*) _mem_cache,
                    _meta->column_turn * _meta->head_row_bytes);
            if (ret_read != _meta->column_turn * _meta->head_row_bytes) {
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
                return false;
            }
        }
        _mem_cache_begin = head_idx - cache_off_idx;
    }
    if (_key_list != NULL && _key_list->size() > 0 && _key_list->size() < _meta->head_length) {
        char* tmp_p = _mem_cache;
        char* tmp_h = (char*) head;
        for (std::list<int>::iterator it = _key_list->begin(); it != _key_list->end(); it++) {
            memcpy(tmp_h, tmp_p + cache_off_idx * _meta->head_sizes[*it], _meta->head_sizes[*it]);
            tmp_h += _meta->head_sizes[*it];
            tmp_p += _meta->head_sizes[*it] * _meta->column_turn;
        }
    } else {
        char* tmp_p = _mem_cache;
        char* tmp_h = (char*) head;
        for (uint32_t i = 0; i < _meta->head_length; i++) {
            memcpy(tmp_h, tmp_p + cache_off_idx * _meta->head_sizes[i], _meta->head_sizes[i]);
            tmp_h += _meta->head_sizes[i];
            tmp_p += _meta->head_sizes[i] * _meta->column_turn;
        }
    }
    return true;
}

void SeisSubHeadReaderColumn::SetKeyList(std::list<int> *key_list) {
    _key_list = key_list;
}
void SeisSubHeadReaderColumn::SetHeadOffset(int64_t* head_offset) {
    _head_offset = head_offset;
}

bool SeisSubHeadReaderColumn::LoadIndex() {
    std::vector<std::pair<int64_t, std::pair<int, int64_t>>> index_vector;
    std::string head_uuid_dir = _hdfs_data_name + "/" + _head_dir_uuid;
    std::vector<std::pair<int64_t, int64_t>> trun_vector;

    if (!GetIndexInfo(_fs, head_uuid_dir, _upd_dat_fd_vector, index_vector, _update_dir_num)) //get all the index information, these index information is the index of updated trace
            {
        return false;
    }
    if (!GetTruncateInfo(_fs, head_uuid_dir, trun_vector)) //get all the truncate information
            {
        return false;
    }

    FilterTruncatedData(_idx_hash_map, index_vector, trun_vector); //filter the unavailable index information, index hash map store all the available index information

    return true;
}

NAMESPACE_END_SEISFS
}
