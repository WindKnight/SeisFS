#include "gcache_seis_subheadcompactor_row.h"

#define COMPACT_DATA_SIZE 1024*1024*1024

//extern int GCACHE_seis_errno;
NAMESPACE_BEGIN_SEISFS
namespace file {
/**
 * SeisSubHeadCompactorRow
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
SeisSubHeadCompactorRow::SeisSubHeadCompactorRow(hdfsFS fs, const std::string& hdfs_data_name,
        SharedPtr<MetaSeisHDFS> meta, int64_t head_dir_idx) {
    _fs = fs;
    _meta = meta;
    _hdfs_data_name = hdfs_data_name;
    _head_dir_idx = head_dir_idx;
    _head_dir_uuid = _meta->head_dir_uuid_array[head_dir_idx];
    _index_vector_array.clear();
    _update_dir_num = 0;
}

/**
 * ~SeisSubHeadCompactorRow
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                destruct a object.
 * @return              no return.
 */
SeisSubHeadCompactorRow::~SeisSubHeadCompactorRow() {
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

/**
 * ~SeisSubHeadCompactorRow
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                compact log.
 * @return              return true if compact successfully, else return false.
 */
bool SeisSubHeadCompactorRow::Compact() {

    int count = 0;
    std::string head_uuid_dir = _hdfs_data_name + "/" + _head_dir_uuid;
    std::string new_file_name = head_uuid_dir + "/Write_Data" + "/" + "new_file";
    hdfsFileInfo * listFileInfo = hdfsListDirectory(_fs,
            (head_uuid_dir + "/" + "Write_Data").c_str(), &count);
    if (listFileInfo == NULL) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_LISTDIRECTORY, errno);
        return false;
    }

    int64_t max_read_rows = (COMPACT_DATA_SIZE / _meta->head_row_bytes);

    for (int64_t i = 0; i < (int64_t) _index_vector_array.size(); i++) {
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
                int64_t write_row_index = (*_index_vector_array[i])[j].first - file_front_index;
                int64_t order = write_row_index / max_read_rows;
                if (!row_part_vector[order]) {
                    row_part_vector[order] = new vector<pair<int64_t, pair<int, int64_t>>*>();
                }
                row_part_vector[order]->push_back(&((*_index_vector_array[i])[j]));
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
                    hdfsCloseFile(_fs, write_fd);
                    hdfsCloseFile(_fs, read_fd);
                    hdfsFreeFileInfo(listFileInfo, count);
                    GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ, errno);
                    return false;
                }

                int64_t relative_rows = roll * max_read_rows;
                if (row_part_vector[roll]) {
                    for (uint64_t j = 0; j < row_part_vector[roll]->size(); j++) {
                        int64_t write_row_index = (*row_part_vector[roll])[j]->first
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
                            hdfsCloseFile(_fs, write_fd);
                            hdfsCloseFile(_fs, read_fd);
                            hdfsFreeFileInfo(listFileInfo, count);
                            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_SEEK,
                            errno);
                            return false;
                        }

                        char* headRow = new char[_meta->head_row_bytes];
                        int64_t ret_read = SafeRead(_fs, upd_dat_fd, headRow,
                                _meta->head_row_bytes);
                        if (ret_read != _meta->head_row_bytes) {
                            delete[] headRow;
                            delete[] newFileBuff;
                            for (int64_t k = 0; k < row_parts; k++) {
                                if (row_part_vector[k]) {
                                    delete row_part_vector[k];
                                }
                            }
                            row_part_vector.clear();
                            hdfsCloseFile(_fs, write_fd);
                            hdfsCloseFile(_fs, read_fd);
                            hdfsFreeFileInfo(listFileInfo, count);
                            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_READ,
                            errno);
                            return false;
                        }
                        memcpy(newFileBuff + write_row_index * _meta->head_row_bytes, headRow,
                                _meta->head_row_bytes);
                        delete[] headRow;
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
            }

            for (int64_t k = 0; k < row_parts; k++) {
                if (row_part_vector[k]) {
                    delete row_part_vector[k];
                }
            }
            row_part_vector.clear();

            if (hdfsCloseFile(_fs, read_fd) == -1) {
                hdfsCloseFile(_fs, write_fd);
                hdfsFreeFileInfo(listFileInfo, count);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
                return false;
            }

            if (hdfsCloseFile(_fs, write_fd) == -1) {
                hdfsFreeFileInfo(listFileInfo, count);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
                return false;
            }

            if (hdfsDelete(_fs, listFileInfo[i].mName, 0) == -1) {
                hdfsFreeFileInfo(listFileInfo, count);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_DELETE, errno);
                return false;
            }

            if (hdfsRename(_fs, new_file_name.c_str(), listFileInfo[i].mName) == -1) {
                hdfsFreeFileInfo(listFileInfo, count);
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_RENAME, errno);
                return false;
            }
        }
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
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CREATEDIR, errno);
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
bool SeisSubHeadCompactorRow::Init() {
    //index init
    std::vector<std::pair<int64_t, std::pair<int, int64_t>>> index_vector;
    std::string head_uuid_dir = _hdfs_data_name + "/" + _head_dir_uuid;
    std::vector<std::pair<int64_t, int64_t>> trun_vector;

    int64_t file_number = GetTotalFileNumber(_fs, head_uuid_dir + "/" + "Write_Data");
    if (file_number == -1) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_LISTDIRECTORY, errno);
        return false;
    }
    for (int64_t i = 0; i < file_number; i++) {
        _index_vector_array.push_back(NULL);
    }

//    if (!GetIndexInfo(_fs, head_uuid_dir, _upd_dat_fd_vector, index_vector,0)) //get all the index information, these index information is the index of updated head
//            {
//        return false;
//    }
//    if (!GetTruncateInfo(_fs, head_uuid_dir, trun_vector)) //get all the truncate information
//            {
//        return false;
//    }
//    FilterTruncatedData(_index_vector_array, index_vector, trun_vector, _meta->file_max_rows); //filter the unavailable index information, index hash map store all the available index information

    return true;
}

bool SeisSubHeadCompactorRow::LoadIndex() {
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
