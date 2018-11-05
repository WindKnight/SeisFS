/*
 * gcache_seis_headreader_row.cpp
 *
 *  Created on: Jul 19, 2016
 *      Author: wb
 */

#include "gcache_seis_subheadreader_row.h"

//extern int GCACHE_seis_errno;
NAMESPACE_BEGIN_SEISFS
namespace file {
/**
 * SeisSubHeadReaderRow
 *
 * @author              weibing
 * @version             0.2.7
 * @param               fs is the hdfs file system handle.
 *                      hdfs_data_name is the full name of seis.
 *                      meta is the meta of seis.
 *                      head_dir_idx is the index of this sub head updater.
 * @func                construct a object.
 * @return              no return.
 */
SeisSubHeadReaderRow::SeisSubHeadReaderRow(hdfsFS fs, const std::string& hdfs_data_name,
        SharedPtr<MetaSeisHDFS> meta, int64_t head_dir_idx) {
    _fs = fs;
    _meta = meta;
    _hdfs_data_name = hdfs_data_name;
    _is_close_flag = false;
    _head_dir_idx = head_dir_idx;
    _head_dir_uuid = _meta->head_dir_uuid_array[head_dir_idx];
    _update_dir_num = -1;

}

/**
 * ~SeisSubHeadReaderRow
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                destruct a object.
 * @return              no return.
 */
SeisSubHeadReaderRow::~SeisSubHeadReaderRow() {
    Close();
}

/**
 * FindIndexItem
 *
 * @author              weibing
 * @version             0.2.7
 * @param               head_idx is the index of head.
 *                      is_last_file_item indicates whether the head need to be read in last updated log or not.
 * @func                find a index item by hash map.
 * @return              return the length of head.
 */
bool SeisSubHeadReaderRow::FindIndexItem(int64_t head_idx, bool& is_last_file_item) {
    _hash_find_rst = _idx_hash_map.find(head_idx);
    if (_hash_find_rst != _idx_hash_map.end()) {
        int64_t upd_dat_fd_index = _hash_find_rst->second.first; //indicate the number of file
        is_last_file_item =
                (upd_dat_fd_index == (int64_t) _upd_dat_fd_vector.size() - 1) ? true : false;
        return true;
    }
    is_last_file_item = false;
    return false;
}

/**
 * GetUpdateData
 *
 * @author              weibing
 * @version             0.2.7
 * @param               head store the update data that user need.
 * @func                get the update data in log.
 * @return              return true if get successfully, else return false.
 */
bool SeisSubHeadReaderRow::GetUpdateData(void* head) {
    int upd_dat_fd_index = _hash_find_rst->second.first; //indicate the updated data file handle
    int64_t upd_dat_offset = _meta->head_row_bytes
            * (_hash_find_rst->second.second % _meta->file_max_rows); //indicate the offset of the head that user need

    hdfsFile fd = GetFileHandle(_fs, _upd_dat_fd_vector, _upd_file_number_cache, upd_dat_fd_index);
    if (fd == NULL) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_GETFILEHANDLE;
        return false;
    }

    int ret_seek = hdfsSeek(_fs, fd, upd_dat_offset);
    if (ret_seek == -1) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_SEEK, errno);
        return false;
    }

    int64_t ret_read = SafeRead(_fs, fd, (char*) head, _meta->head_row_bytes);
    if (ret_read != _meta->head_row_bytes) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
        return false;
    }
    return true;
}

/**
 * GetWriteData
 *
 * @author              weibing
 * @version             0.2.7
 * @param               head_idx the index of head.
 *                      head store the update data that user need.
 * @func                get the original write data.
 * @return              return true if get successfully, else return false.
 */
bool SeisSubHeadReaderRow::GetWriteData(int64_t head_idx, void* head) {
    int64_t file_idx = head_idx / _meta->file_max_rows; //indicate the index of file
    int64_t offset = _meta->head_row_bytes * (head_idx % _meta->file_max_rows); //read require head from this offset

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

    int64_t ret_read = SafeRead(_fs, fd, (char*) head, _meta->head_row_bytes);
    if (ret_read != _meta->head_row_bytes) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
        return false;
    }
    return true;
}

/**
 * Get
 *
 * @author              weibing
 * @version             0.2.7
 * @param               head_idx is the index of head.
 *                      head store the update data that user need.
 * @func                get a head by head index.
 * @return              return true if get successfully, else return false.
 */
bool SeisSubHeadReaderRow::Get(int64_t head_idx, void* head) {
    int64_t head_num = _meta->head_dir_rows_array[_head_dir_idx]; //find corresponding head dir
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
    if (FindIndexItem(head_idx, is_last_file_item)) //the require head data is in log
            {
        return GetUpdateData(head);              //get the require head data
    } else {
        return GetWriteData(head_idx, head); //the require head data is in original write file
    }
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
bool SeisSubHeadReaderRow::Close() {
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
 * AddNewIndexItem
 *
 * @author              weibing
 * @version             0.2.7
 * @param               head_idx is the index of head.
 *                      upd_dat_fd is the latest updated log's file handle.
 *                      file_rows is the head row number in latest updated file.
 * @func                add a new index item to hash map.
 * @return              no return.
 */
void SeisSubHeadReaderRow::AddNewIndexItem(int64_t head_idx, std::string new_file_name,
        int64_t file_rows) {
    if (new_file_name != "")  //add the new create update log's file handle
            {
        _idx_hash_map[head_idx] = std::pair<int, int64_t>((int) _upd_dat_fd_vector.size(),
                file_rows);
        RichFileHandle rich_fd;
        rich_fd.fd = NULL;
        rich_fd.file_name = new_file_name;
        rich_fd.access_time = GetCurrentMillis();
        _upd_dat_fd_vector.push_back(rich_fd);
    } else     //add the last update log's file handle
    {
        _idx_hash_map[head_idx] = std::pair<int, int64_t>((int) _upd_dat_fd_vector.size() - 1,
                file_rows);
    }
}

/**
 * Init
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                preliminary work for reading data.
 * @return              return true if init successfully, else return false.
 */
bool SeisSubHeadReaderRow::Init() {
    //index init
    std::vector<std::pair<int64_t, std::pair<int, int64_t>>> index_vector;
    std::string head_uuid_dir = _hdfs_data_name + "/" + _head_dir_uuid;
    std::vector<std::pair<int64_t, int64_t>> trun_vector;

//    if (!GetIndexInfo(_fs, head_uuid_dir, _upd_dat_fd_vector, index_vector,0)) //get all the index information, these index information is the index of updated head
//            {
//        return false;
//    }
//    if (!GetTruncateInfo(_fs, head_uuid_dir, trun_vector)) //get all the truncate information
//            {
//        return false;
//    }
//    FilterTruncatedData(_idx_hash_map, index_vector, trun_vector); //filter the unavailable index information, index hash map store all the available index information

    std::string head_wrt_dat_dir = head_uuid_dir + "/Write_Data";
    if (!OpenAllFile(_fs, head_wrt_dat_dir, _wrt_dat_fd_vector)) //open all the original write file
            {
        return false;
    }

    return true;
}

bool SeisSubHeadReaderRow::GetLocation(const int64_t head_idx, std::vector<std::string> &hostname) {
    hostname.clear();
    int64_t head_num = _meta->head_dir_rows_array[_head_dir_idx];
    if (head_idx >= head_num) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PARAMERR;
        return false;
    }
    char *** hostInfos;
    unordered_map<int64_t, pair<int, int64_t>>::iterator rst = _idx_hash_map.find(head_idx);
    if (rst != _idx_hash_map.end())  //the require head is in log
            {
        int upd_dat_fd_index = rst->second.first; //indicate the updated data file handle
        int64_t upd_dat_offset = _meta->head_row_bytes
                * (rst->second.second % _meta->file_max_rows); //indicate the offset of the head that user need
        std::string filePath = _upd_dat_fd_vector[upd_dat_fd_index].file_name;
        hostInfos = hdfsGetHosts(_fs, _upd_dat_fd_vector[upd_dat_fd_index].file_name.c_str(),
                upd_dat_offset, 1);

    } else  //the require head is in original write file
    {
        int64_t file_idx = head_idx / _meta->file_max_rows; //indicate the index of file
        int64_t offset = _meta->head_row_bytes * (head_idx % _meta->file_max_rows); //read require head from this offset

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

bool SeisSubHeadReaderRow::LoadIndex() {
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
