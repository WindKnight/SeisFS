#include "gcache_seis_subheadupdater_row.h"

//extern int GCACHE_seis_errno;
NAMESPACE_BEGIN_SEISFS
namespace file {
/**
 * SeisSubHeadUpdaterRow
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
SeisSubHeadUpdaterRow::SeisSubHeadUpdaterRow(hdfsFS fs, const std::string& hdfs_data_name,
        SharedPtr<MetaSeisHDFS> meta, int64_t head_dir_idx) {
    _fs = fs;
    _meta = meta;
    _hdfs_data_name = hdfs_data_name;
    _is_close_flag = false;
    _current_head_idx_fd = NULL;
    _current_head_dat_fd = NULL;
    _head_current_row = 0;
    _head_dir_uuid = _meta->head_dir_uuid_array[head_dir_idx];
    _head_dir_idx = head_dir_idx;
    _head_updidx_name = "";
    _head_upddat_name = "";
    _update_dir_num = -1;

}

/**
 * ~SeisSubHeadUpdaterRow
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                destruct a object.
 * @return              no return.
 */
SeisSubHeadUpdaterRow::~SeisSubHeadUpdaterRow() {
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
bool SeisSubHeadUpdaterRow::Close() {
    if (!_is_close_flag) {
        if (_current_head_idx_fd != NULL) {
            if (-1 == hdfsCloseFile(_fs, _current_head_idx_fd)) {
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
                return false;
            }
        }
        if (_current_head_dat_fd != NULL) {
            if (-1 == hdfsCloseFile(_fs, _current_head_dat_fd)) {
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
                return false;
            }
        }
        _is_close_flag = true;
    }
    return true;
}

/**
 * ReOpenUpdatedFile
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                close updated file then open updated file.
 * @return              return true if reopen successfully, else return false.
 */
bool SeisSubHeadUpdaterRow::ReOpenUpdatedFile() {
    if (_current_head_idx_fd == NULL) {
        return true;
    }

    if (-1 == hdfsCloseFile(_fs, _current_head_idx_fd)
            || -1 == hdfsCloseFile(_fs, _current_head_dat_fd)) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
        return false;
    }

    _current_head_dat_fd = hdfsOpenFile(_fs, _head_upddat_name.c_str(),
    O_WRONLY | O_APPEND, 0, REPLICATION, 0);   //reopen updated file
    if (_current_head_dat_fd == NULL) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
        return false;
    }

    _current_head_idx_fd = hdfsOpenFile(_fs, _head_updidx_name.c_str(),
    O_WRONLY | O_APPEND, 0, REPLICATION, 0);   //reopen index file
    if (_current_head_idx_fd == NULL) {
        hdfsCloseFile(_fs, _current_head_dat_fd);
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
        return false;
    }

    return true;
}

/**
 * Put
 *
 * @author              weibing
 * @version             0.2.7
 * @param               head_idx is the index of head.
 *                      head is the head data need to be written.
 *                      subHeadReader is the sub head reader, when create a new log the file handle need to be add to sub head reader,so the latest updated head can be read.
 * @func                update a head by head index.
 * @return              return true if put successfully, else return false.
 */
bool SeisSubHeadUpdaterRow::Put(int64_t head_idx, const void* head,
        SeisSubHeadReaderRow* subHeadReader) {
    int64_t head_num = _meta->head_dir_rows_array[_head_dir_idx];
    if (head_idx >= head_num) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PARAMERR;
        return false;
    }

    if (_update_dir_num < 0) {
        _update_dir_num = head_idx / _meta->update_block_num;
    }

    if (_head_current_row == _meta->file_max_rows || _current_head_idx_fd == NULL) //if the file is full or first append updated a head, need to create a new file
    {
        if (_head_current_row == _meta->file_max_rows
                || _update_dir_num != head_idx / _meta->update_block_num) //current file is full, close current file
                        {
            int idx_rst = hdfsCloseFile(_fs, _current_head_idx_fd);
            if (idx_rst == -1) {
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
                return false;
            }

            int dat_rst = hdfsCloseFile(_fs, _current_head_dat_fd);
            if (dat_rst == -1) {
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_CLOSE, errno);
                return false;
            }
            _update_dir_num = head_idx / _meta->update_block_num;
        }

        bool flag_new=true;
        if(_head_idx_map.find(_update_dir_num)==_head_idx_map.end() || _head_current_row == _meta->file_max_rows){
            std::string upd_file_name = GetUpdateFileName();
            std::string head_updidx_name = _hdfs_data_name + "/" + _head_dir_uuid + "/Update_Index/No_"
                    + Int642Str(head_idx / _meta->update_block_num) + "/";
            std::string head_upddat_name = _hdfs_data_name + "/" + _head_dir_uuid + "/Update_Data/No_"
                    + Int642Str(head_idx / _meta->update_block_num) + "/";

            if (hdfsCreateDirectory(_fs, head_updidx_name.c_str()) == -1) {
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
                return false;
            }

            if (hdfsCreateDirectory(_fs, head_upddat_name.c_str()) == -1) {
                GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
                return false;
            }

            _head_updidx_name = head_updidx_name + upd_file_name;
            _head_upddat_name = head_upddat_name + upd_file_name;
            _head_idx_map[_update_dir_num]=_head_updidx_name;
            _head_dat_map[_update_dir_num]=_head_upddat_name;
            _head_current_row_map[_update_dir_num]=0;
            _head_current_row = 0;
        }else{
            _head_updidx_name=_head_idx_map[_update_dir_num];
            _head_upddat_name=_head_dat_map[_update_dir_num];
            _head_current_row = _head_current_row_map[_update_dir_num];
            flag_new=false;
        }

        _current_head_dat_fd = hdfsOpenFile(_fs, _head_upddat_name.c_str(),
        O_WRONLY | O_APPEND, 0, REPLICATION, 0); //create new updated file
        if (_current_head_dat_fd == NULL) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
            return false;
        }

        _current_head_idx_fd = hdfsOpenFile(_fs, _head_updidx_name.c_str(),
        O_WRONLY | O_APPEND, 0, REPLICATION, 0); //create new index file
        if (_current_head_idx_fd == NULL) {
            GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_OPFILE, errno);
            return false;
        }

        hdfsChmod(_fs, _head_updidx_name.c_str(), 0644);
        hdfsChmod(_fs, _head_upddat_name.c_str(), 0644);

        if (subHeadReader && flag_new) {
            //when update some field of a head,we need a read and a write, the lasted updated file need to be open for reading
            subHeadReader->AddNewIndexItem(head_idx, _head_upddat_name, 0); // add lasted index item to hash map
        }

    } else {
        if (subHeadReader) {
            subHeadReader->AddNewIndexItem(head_idx, "", _head_current_row); // add lasted index item to hash map
        }
    }

    int head_idx_bytes = sizeof(head_idx);
    int ret_idx = hdfsWrite(_fs, _current_head_idx_fd, &head_idx, head_idx_bytes); //write index data
    if (ret_idx != head_idx_bytes) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_WRITE, errno);
        return false;
    }

    int64_t ret_head = SafeWrite(_fs, _current_head_dat_fd, (char*) head, _meta->head_row_bytes); //write updated head
    if (ret_head != _meta->head_row_bytes) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_WRITE, errno);
        return false;
    }

    _head_current_row++;
    _head_current_row_map[_update_dir_num]++;
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
bool SeisSubHeadUpdaterRow::Sync() {
    int idx_rst = hdfsSync(_fs, _current_head_idx_fd);
    if (idx_rst == -1) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_SYNC, errno);
        return false;
    }

    int dat_rst = hdfsSync(_fs, _current_head_dat_fd);
    if (dat_rst == -1) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_SYNC, errno);
        return false;
    }

    return true;
}

/**
 * HFlush
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                HFlush update data, so the reader can get the lasted updated head, in some time, when update a head we need to read the lasted updated head.
 * @return              return true if hflush successfully, else return false.
 */
bool SeisSubHeadUpdaterRow::HFlush() {
    if (_current_head_dat_fd == NULL) //when first update,the last log file is closed, so return true
    {
        return true;
    }

    int dat_rst = hdfsHFlush(_fs, _current_head_dat_fd);
    if (dat_rst == -1) {
        GCACHE_seis_errno = GcacheSeisCombineErrno(GCACHE_SEIS_ERR_HFLUSH, errno);
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
bool SeisSubHeadUpdaterRow::Init() {
    return true;
}

NAMESPACE_END_SEISFS
}
