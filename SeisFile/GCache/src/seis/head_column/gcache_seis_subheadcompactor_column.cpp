#include "gcache_seis_subheadcompactor_column.h"

#define COMPACT_DATA_SIZE_COLUMN 1024*1024*1024

//extern int GCACHE_seis_errno;
NAMESPACE_BEGIN_SEISFS
namespace file {
/**
 * SeisSubHeadCompactorColumn
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
SeisSubHeadCompactorColumn::SeisSubHeadCompactorColumn(hdfsFS fs, const std::string& hdfs_data_name,
        SharedPtr<MetaSeisHDFS> meta, int64_t head_dir_idx) {
    _fs = fs;
    _meta = meta;
    _hdfs_data_name = hdfs_data_name;
    _head_dir_idx = head_dir_idx;
    _head_dir_uuid = _meta->head_dir_uuid_array[head_dir_idx];
    _hdfs_cache_dir = _hdfs_data_name + "/" + _head_dir_uuid + "/Cache_Data";
    _cache_flag = false;
    _index_vector_array.clear();
    // current dir total rows
    _head_total_rows = _meta->head_dir_rows_array[head_dir_idx];
    int64_t mod = _head_total_rows % _meta->column_turn;
    _write_column = _head_total_rows - mod;
    _update_dir_num = 0;

}

/**
 * ~SeisSubHeadCompactorColumn
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                destruct a object.
 * @return              no return.
 */
SeisSubHeadCompactorColumn::~SeisSubHeadCompactorColumn() {
    CloseAllFile(_fs, _upd_dat_fd_vector);
    _upd_dat_fd_vector.clear();
    _upd_file_number_cache.clear();
    for (uint64_t i = 0; i < _index_vector_array.size(); i++) {
        if (_index_vector_array[i]) {
            _index_vector_array[i]->clear();
            delete _index_vector_array[i];
        }
    }
    _index_vector_array.clear();
}

bool SeisSubHeadCompactorColumn::Compact() {
    int count = 0;
    std::string head_uuid_dir = _hdfs_data_name + "/" + _head_dir_uuid;
    std::string new_file_name = head_uuid_dir + "/Write_Data" + "/" + "new_file";
    hdfsFileInfo * listFileInfo = hdfsListDirectory(_fs,
            (head_uuid_dir + "/" + "Write_Data").c_str(), &count);
    if (listFileInfo == NULL) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_LISTDIRECTORY, errno);
        return false;
    }

//	for (int64_t i = 0; i < count; i++) {
//		_index_vector_array.push_back(NULL);
//	}
//	if(_head_total_rows-_write_column<_meta->column_turn){
//		_index_vector_array.push_back(NULL);
//	}

    int64_t compact_dat_size = COMPACT_DATA_SIZE_COLUMN;
    int64_t compact_rows = compact_dat_size / _meta->head_row_bytes;
    //int64_t t = compact_dat_size / (_COLUMN*_meta->head_row_bytes);
    int64_t max_read_rows = 0;
    if (compact_rows <= _meta->column_turn) {
        max_read_rows = _meta->column_turn;     // 变成0
    } else {
        max_read_rows = compact_rows - (compact_rows % _meta->column_turn);
    }
//    int64_t max_read_rows = (compact_dat_size / _meta->head_row_bytes);
    vector<pair<int64_t, pair<int, int64_t>>> cache_vector;
    for (int64_t i = 0; i < count; i++) {
        if (i * _meta->file_max_rows >= (int64_t) _update_dir_num * _meta->update_block_num) {
            if(_index_vector_array.size()>=HASH_MAP_MAX_SIZE){
                _index_vector_array.clear();
            }
            if(!LoadIndex()){
                hdfsFreeFileInfo(listFileInfo, count);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
                return false;
            }
            _update_dir_num++;
        }
        if (_index_vector_array[i]) {
            //read old file
            hdfsFile read_fd = hdfsOpenFile(_fs, listFileInfo[i].mName,
            O_RDONLY, 0, REPLICATION, 0);
            if (NULL == read_fd) {
                hdfsFreeFileInfo(listFileInfo, count);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
                return false;
            }

            //write new file
            hdfsFile write_fd = hdfsOpenFile(_fs, new_file_name.c_str(),
            O_WRONLY, 0, REPLICATION, 0);
            if (NULL == write_fd) {
                hdfsCloseFile(_fs, read_fd);
                hdfsFreeFileInfo(listFileInfo, count);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
                return false;
            }

            int64_t file_front_index = i * _meta->file_max_rows;
            vector<vector<pair<int64_t, pair<int, int64_t>>*>*> row_part_vector;

            int64_t row_parts = (int64_t) ceil(
                    (double) _meta->file_max_rows / (double) (max_read_rows));

            for (int64_t j = 0; j < row_parts; j++) {
                row_part_vector.push_back(NULL);
            }

            for (uint64_t j = 0; j < _index_vector_array[i]->size(); j++) {
                int64_t write_column_index = (*_index_vector_array[i])[j].first;
                int64_t write_row_index = write_column_index - file_front_index;
                if (write_column_index >= _write_column) {
                    cache_vector.push_back(((*_index_vector_array[i])[j]));
                } else {
                    int64_t order = write_row_index / max_read_rows;
                    if (!row_part_vector[order]) {
                        row_part_vector[order] = new vector<pair<int64_t, pair<int, int64_t>>*>();
                    }
                    row_part_vector[order]->push_back(&((*_index_vector_array[i])[j]));
                }
            }

            int64_t left_rows = listFileInfo[i].mSize / _meta->head_row_bytes;
            int64_t roll = 0;
            while (left_rows > 0) {
                int64_t read_rows = left_rows > max_read_rows ? max_read_rows : left_rows;
                int64_t read_size = read_rows * _meta->head_row_bytes;
                char *newFileBuff = new char[read_size];
                int64_t read_ret = SafeRead(_fs, read_fd, newFileBuff, read_size);
                if (read_ret != read_size) {
                    delete[] newFileBuff;
                    for (int64_t j = 0; j < row_parts; j++) {
                        if (row_part_vector[j]) {
                            delete row_part_vector[j];
                        }
                    }
                    row_part_vector.clear();
//                  for (int64_t k = 0; k < cache_vector.size(); k++) {
//
//                          delete cache_vector[k];
//                      }
                    cache_vector.clear();
                    hdfsCloseFile(_fs, write_fd);
                    hdfsCloseFile(_fs, read_fd);
                    hdfsFreeFileInfo(listFileInfo, count);
                    GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ,
                    errno);
                    return false;
                }
                int64_t relative_rows = roll * max_read_rows;
                if (row_part_vector[roll]) {
                    for (uint64_t j = 0; j < row_part_vector[roll]->size(); j++) {
                        int64_t write_column_index = (*row_part_vector[roll])[j]->first
                                - file_front_index - relative_rows;
                        //read updated data
                        int upd_dat_fd_index = (*row_part_vector[roll])[j]->second.first; //indicate the updated data file handle
                        int64_t upd_dat_offset =
                                _meta->head_row_bytes
                                        * ((*row_part_vector[roll])[j]->second.second
                                                % _meta->file_max_rows); //indicate the offset of the head that user need
                        hdfsFile upd_dat_fd = GetFileHandle(_fs, _upd_dat_fd_vector,
                                _upd_file_number_cache, upd_dat_fd_index);
                        if (upd_dat_fd == NULL) {
                            delete[] newFileBuff;
                            for (int64_t k = 0; k < row_parts; k++) {
                                if (row_part_vector[k]) {
                                    delete row_part_vector[k];
                                }
                            }
                            row_part_vector.clear();
                            //                      for (int64_t k = 0; k < cache_vector.size(); k++) {
                            //
                            //                              delete cache_vector[k];
                            //                          }
                            cache_vector.clear();
                            hdfsCloseFile(_fs, write_fd);
                            hdfsCloseFile(_fs, read_fd);
                            hdfsFreeFileInfo(listFileInfo, count);
                            GCACHE_seis_errno = GCACHE_SEIS_ERR_GETFILEHANDLE;
                            return false;
                        }
                        int ret_seek = hdfsSeek(_fs, upd_dat_fd, upd_dat_offset);
                        if (ret_seek == -1) {
                            delete[] newFileBuff;
                            for (int64_t k = 0; k < row_parts; k++) {
                                if (row_part_vector[k]) {
                                    delete row_part_vector[k];
                                }
                            }
                            row_part_vector.clear();
                            //                      for (int64_t k = 0; k < cache_vector.size(); k++) {
                            //
                            //                              delete cache_vector[k];
                            //                          }
                            cache_vector.clear();
                            hdfsCloseFile(_fs, write_fd);
                            hdfsCloseFile(_fs, read_fd);
                            hdfsFreeFileInfo(listFileInfo, count);
                            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_SEEK,
                            errno);
                            return false;
                        }
                        //read update dat
                        char* headColumn = new char[_meta->head_row_bytes];
                        int64_t ret_read = SafeRead(_fs, upd_dat_fd, headColumn,
                                _meta->head_row_bytes);
                        if (ret_read != _meta->head_row_bytes) {
                            delete[] headColumn;
                            delete[] newFileBuff;
                            for (int64_t k = 0; k < row_parts; k++) {
                                if (row_part_vector[k]) {
                                    delete row_part_vector[k];
                                }
                            }
                            row_part_vector.clear();
                            //                      for (int64_t k = 0; k < cache_vector.size(); k++) {
                            //
                            //                              delete cache_vector[k];
                            //                          }
                            cache_vector.clear();
                            hdfsCloseFile(_fs, write_fd);
                            hdfsCloseFile(_fs, read_fd);
                            hdfsFreeFileInfo(listFileInfo, count);
                            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ,
                            errno);
                            return false;
                        }
                        // transfer
                        int64_t start = write_column_index / _meta->column_turn;
                        int64_t start_offset = _meta->head_row_bytes * start * _meta->column_turn;
                        int64_t start_cpy = write_column_index % _meta->column_turn;
                        int64_t tmp = 0;
                        int64_t tmp3 = 0;
                        for (uint32_t i = 0; i < _meta->head_length; i++) {
                            int64_t tmp2 = _meta->head_sizes[i];

                            memcpy(newFileBuff + start_offset + start_cpy * tmp2 + tmp,
                                    headColumn + tmp3, tmp2);
                            tmp += tmp2 * _meta->column_turn;
                            tmp3 += tmp2;
                        }
                        delete[] headColumn;
                    }

                }

                int64_t write_len = read_size;
                if (write_len != SafeWrite(_fs, write_fd, newFileBuff, write_len)) {
                    delete[] newFileBuff;
                    for (int64_t k = 0; k < row_parts; k++) {
                        if (row_part_vector[k]) {
                            delete row_part_vector[k];
                        }
                    }
                    row_part_vector.clear();
//                  for (int64_t k = 0; k < cache_vector.size(); k++) {
//
//                          delete cache_vector[k];
//                      }
                    cache_vector.clear();
                    hdfsCloseFile(_fs, write_fd);
                    hdfsCloseFile(_fs, read_fd);
                    hdfsFreeFileInfo(listFileInfo, count);
                    GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_WRITE,
                    errno);
                    return false;
                }

                left_rows -= read_rows;
                delete[] newFileBuff;
                roll++;
            } //while(loop)

            for (int64_t k = 0; k < row_parts; k++) {
                if (row_part_vector[k]) {
                    delete row_part_vector[k];
                }
            }
            row_part_vector.clear();

            hdfsCloseFile(_fs, read_fd);
            hdfsCloseFile(_fs, write_fd);

            if (hdfsDelete(_fs, listFileInfo[i].mName, 0) == -1) {
                hdfsFreeFileInfo(listFileInfo, count);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_DELETE,
                errno);
                return false;
            }

            if (hdfsRename(_fs, new_file_name.c_str(), listFileInfo[i].mName) == -1) {
                hdfsFreeFileInfo(listFileInfo, count);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_RENAME,
                errno);
                return false;
            }
        } //if update
    } //for
    if (_cache_flag == true) {
        if (_write_column == _update_dir_num * _meta->update_block_num) {
            if(_index_vector_array.size()>=HASH_MAP_MAX_SIZE){
                _index_vector_array.clear();
            }
            if(!LoadIndex()){
                hdfsFreeFileInfo(listFileInfo, count);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
                return false;
            }
        }
        if (_index_vector_array[_index_vector_array.size() - 1] != NULL) {
            for (vector<pair<int64_t, pair<int, int64_t>>>::iterator it =
                    _index_vector_array[_index_vector_array.size() - 1]->begin();
                    it != _index_vector_array[_index_vector_array.size() - 1]->end(); it++) {
                if (it->first >= _write_column && it->first < _head_total_rows) {
                    cache_vector.push_back(*it);
                }
            }
        }

        std::string hdfs_cache_name = _hdfs_cache_dir + "/Cache";
        char* cacheFileBuff = NULL;
        int64_t cached_num; //indicate the first "sizeof(int64_t)" B of the cachefile, which means the actual rows stored in cachefile
        int64_t cache_size = 0;
        if (_cache_flag == true) {
            hdfsFileInfo * cacheFileInfo = NULL;
            cacheFileInfo = hdfsGetPathInfo(_fs, hdfs_cache_name.c_str());
            if (cacheFileInfo == NULL) {
                hdfsFreeFileInfo(listFileInfo, count);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_STAT, errno);
                return false;
            }
            int64_t read_len = cacheFileInfo->mSize;
            cache_size = read_len;
            cacheFileBuff = new char[read_len];
            if (!ReadFile(_fs, hdfs_cache_name, cacheFileBuff, read_len, cached_num)) {
                hdfsFreeFileInfo(listFileInfo, count);
                hdfsFreeFileInfo(cacheFileInfo, 1);
                delete[] cacheFileBuff;
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
                return false;
            }
            hdfsFreeFileInfo(cacheFileInfo, 1);
        }

        char* headColumn = new char[_meta->head_row_bytes]; //for buffer of one row of seishead

        for (uint64_t j = 0; j < cache_vector.size(); j++) {
            int64_t write_column_index = cache_vector[j].first - _write_column;
            //read updated data
            int upd_dat_fd_index = cache_vector[j].second.first; //indicate the updated data file handle
            int64_t upd_dat_offset = _meta->head_row_bytes
                    * (cache_vector[j].second.second % _meta->file_max_rows); //indicate the offset of the head that user need
            hdfsFile upd_dat_fd = GetFileHandle(_fs, _upd_dat_fd_vector, _upd_file_number_cache,
                    upd_dat_fd_index);
            if (upd_dat_fd == NULL) {
                delete[] cacheFileBuff;
//              for (int64_t k = 0; k < cache_vector.size(); k++) {
//
//                  delete cache_vector[k];
//              }
                cache_vector.clear();
                hdfsFreeFileInfo(listFileInfo, count);
                GCACHE_seis_errno = GCACHE_SEIS_ERR_GETFILEHANDLE;
                return false;
            }
            int ret_seek = hdfsSeek(_fs, upd_dat_fd, upd_dat_offset);
            if (ret_seek == -1) {
                delete[] cacheFileBuff;
//              for (int64_t k = 0; k < cache_vector.size(); k++) {
//
//                  delete cache_vector[k];
//              }
                cache_vector.clear();
                hdfsFreeFileInfo(listFileInfo, count);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_SEEK,
                errno);
                return false;
            }
            //read update dat
            int64_t ret_read = SafeRead(_fs, upd_dat_fd, headColumn, _meta->head_row_bytes);
            if (ret_read != _meta->head_row_bytes) {
                delete[] cacheFileBuff;
//              for (int64_t k = 0; k < cache_vector.size(); k++) {
//
//                      delete cache_vector[k];
//                  }
                cache_vector.clear();
                hdfsFreeFileInfo(listFileInfo, count);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ,
                errno);
                return false;
            }

            char* temp_cur_cache = cacheFileBuff;
            char* temp_cur_head = (char*) headColumn;
            for (uint32_t i = 0; i < _meta->head_length; i++) {
                temp_cur_cache += _meta->head_sizes[i] * write_column_index;
                memcpy(temp_cur_cache, temp_cur_head, _meta->head_sizes[i]);
                temp_cur_cache += _meta->head_sizes[i] * (_meta->column_turn - write_column_index);
                temp_cur_head += _meta->head_sizes[i];
            }
        }
//      for (int64_t k = 0; k < cache_vector.size(); k++) {
//
//          delete cache_vector[k];
//      }
        cache_vector.clear();
        hdfsFile write_cache_fd = hdfsOpenFile(_fs, hdfs_cache_name.c_str(),
        O_WRONLY, 0,
        REPLICATION, 0);
        if (NULL == write_cache_fd) {
            hdfsFreeFileInfo(listFileInfo, count);
            delete[] cacheFileBuff;
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
            return false;
        }
        if (sizeof(int64_t)
                != SafeWrite(_fs, write_cache_fd, (char*) &cached_num, sizeof(int64_t))) {
            hdfsFreeFileInfo(listFileInfo, count);
            delete[] cacheFileBuff;
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
            return false;
        }

        int64_t write_len = cache_size;
        if (write_len != SafeWrite(_fs, write_cache_fd, cacheFileBuff, write_len)) {
            hdfsFreeFileInfo(listFileInfo, count);
            delete[] cacheFileBuff;
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
            return false;
        }

        delete[] cacheFileBuff;
        hdfsCloseFile(_fs, write_cache_fd);
    }

    hdfsFreeFileInfo(listFileInfo, count);
    std::string head_dir = _hdfs_data_name + "/" + _head_dir_uuid;
    std::string hdfs_idx_dir = head_dir + "/" + "Update_Index";
    std::string hdfs_dat_dir = head_dir + "/" + "Update_Data";
    std::string hdfs_trun_dir = head_dir + "/" + "Truncate_Data";
    if (hdfsDelete(_fs, hdfs_idx_dir.c_str(), 1) == -1
            || hdfsDelete(_fs, hdfs_dat_dir.c_str(), 1) == -1
            || hdfsDelete(_fs, hdfs_trun_dir.c_str(), 1) == -1) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_DELETE, errno);
        return false;
    }

    if (!CreateAllDir(_fs, _hdfs_data_name, _head_dir_uuid.c_str())) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CREATEDIR,
        errno);
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
bool SeisSubHeadCompactorColumn::Init() {
//index init
    std::vector<std::pair<int64_t, std::pair<int, int64_t>>> index_vector;
    std::string head_uuid_dir = _hdfs_data_name + "/" + _head_dir_uuid;
    std::vector<std::pair<int64_t, int64_t>> trun_vector;
    std::string hdfs_cache_name = _hdfs_cache_dir + "/Cache";
    int64_t total_file_size = GetTotalFileSize(_fs, head_uuid_dir + "/" + "Write_Data")
            + GetTotalFileSize(_fs, head_uuid_dir + "/" + "Cache_Data");
    if (total_file_size == -1) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_LISTDIRECTORY, errno);
        return false;
    }
    int64_t file_number = (int64_t) ceil(
            (double) total_file_size / (_meta->file_max_rows * _meta->head_row_bytes));
    for (int64_t i = 0; i < file_number; i++) {
        _index_vector_array.push_back(NULL);
    }

//    if (!GetIndexInfo(_fs, head_uuid_dir, _upd_dat_fd_vector, index_vector,(int64_t)0)) //get all the index information, these index information is the index of updated head
//            {
//        return false;
//    }
//    if (!GetTruncateInfo(_fs, head_uuid_dir, trun_vector)) //get all the truncate information
//            {
//        return false;
//    }
//    FilterTruncatedData(_index_vector_array, index_vector, trun_vector, _meta->file_exact_rows); //filter the unavailable index information, index hash map store all the available index information
    if (0 == hdfsExists(_fs, hdfs_cache_name.c_str())) {
        _cache_flag = true;
    }
    return true;
}

bool SeisSubHeadCompactorColumn::ReadFile(hdfsFS fs, const std::string& file_name, char* cache,
        int64_t len, int64_t & cached_num) {

    hdfsFile read_fd = hdfsOpenFile(_fs, file_name.c_str(),
    O_RDONLY, 0, REPLICATION, 0);
    if (NULL == read_fd) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
        return false;
    }
    if (sizeof(int64_t) != SafeRead(_fs, read_fd, &cached_num, sizeof(int64_t))) {
        hdfsCloseFile(_fs, read_fd);
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
        return false;
    }
    len = len - (int64_t) sizeof(int64_t);
    if (len != SafeRead(_fs, read_fd, cache, len)) {
        hdfsCloseFile(_fs, read_fd);
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
        return false;
    }
    hdfsCloseFile(_fs, read_fd);
    return true;
}

bool SeisSubHeadCompactorColumn::LoadIndex() {
    std::vector<std::pair<int64_t, std::pair<int, int64_t>>> index_vector;
    std::string trace_uuid_dir = _hdfs_data_name + "/" + _head_dir_uuid;
    std::vector<std::pair<int64_t, int64_t>> trun_vector;

    for (int64_t i = 0; i < (int64_t) _index_vector_array.size(); i++) {
        if (_index_vector_array[i] != NULL) {
            delete _index_vector_array[i];
            _index_vector_array[i] = NULL;
        }
    }

    if (!GetIndexInfo(_fs, trace_uuid_dir, _upd_dat_fd_vector, index_vector, _update_dir_num)) //get all the index information, these index information is the index of updated trace
            {
        return false;
    }
    if (!GetTruncateInfo(_fs, trace_uuid_dir, trun_vector)) //get all the truncate information
            {
        return false;
    }

    FilterTruncatedData(_index_vector_array, index_vector, trun_vector, _meta->file_max_rows); //filter the unavailable index information, index hash map store all the available index information

    return true;
}

NAMESPACE_END_SEISFS
}
