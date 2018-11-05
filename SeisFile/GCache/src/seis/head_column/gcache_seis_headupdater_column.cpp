/*
 * gcache_seis_headupdater_column.cpp
 *
 *  Created on: Jul 19, 2016
 *      Author: wb
 */

#include "gcache_seis_headupdater_column.h"

NAMESPACE_BEGIN_SEISFS
namespace file {
/**
 * SeisHeadUpdaterColumn
 *
 * @author              weibing
 * @version             0.2.7
 * @param               fs is the hdfs file system handle.
 *                      hdfs_data_name is the full name of seis.
 *                      meta is the meta of seis.
 * @func                construct a object.
 * @return              no return.
 */
SeisHeadUpdaterColumn::SeisHeadUpdaterColumn(hdfsFS fs, const std::string& hdfs_data_name,
        SharedPtr<MetaSeisHDFS> meta, bool is_compact_flag) {
    _meta = meta;
    _fs = fs;
    _hdfs_data_name = hdfs_data_name;
    _is_close_flag = false;
    _key_list.clear();
    _head_offset = GetHeadOffset(meta->head_sizes, meta->head_length);
    _head_slider.v_id = -1;
    _head_slider.v_in_id = -1;
    _interval_list.clear();
    _interval_list.push_back(RowScope(0, _meta->trace_total_rows));
    GetNextSlider(_interval_list, _head_slider);
    _is_update_part_flag = false;
    _update_count = 0;
    _is_compact_flag = is_compact_flag;
}

/**
 * ~SeisHeadUpdaterColumn
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                destruct a object.
 * @return              no return.
 */
SeisHeadUpdaterColumn::~SeisHeadUpdaterColumn() {
//    _meta->modify_time_ms=GetCurrentMillis();
    delete[] _head_offset;
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
bool SeisHeadUpdaterColumn::Close() {
    if (!_is_close_flag) {
        for (uint64_t i = 0; i < _sub_head_updater_vector.size(); ++i) {
            delete _sub_head_updater_vector[i];
        }
        _sub_head_updater_vector.clear();

        for (uint64_t i = 0; i < _sub_head_reader_vector.size(); ++i) {
            delete _sub_head_reader_vector[i];
        }
        _sub_head_reader_vector.clear();

        _is_close_flag = true;
    }
    return true;
}

/**
 * SetFilter
 *
 * @author              weibing
 * @version             0.2.7
 * @param               filter has a number of intervals and a number of head keys, the intervals contain the index of head than user need, the keys is the head key need to be updated.
 * @func                set a filter.
 * @return              return true if set filter successfully, else return false.
 */
bool SeisHeadUpdaterColumn::SetHeadFilter(const HeadFilter& filter) {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }

    _key_list = filter.GetKeyList();
    if (_key_list.size() > 0 && _key_list.size() < _meta->head_length && !_is_update_part_flag) {
        for (int64_t i = 0; i < _meta->head_dir_size; ++i) {
            if (!_sub_head_updater_vector[i]->ReOpenUpdatedFile()) {
                return false;
            }
        }

        for (uint64_t i = 0; i < _sub_head_reader_vector.size(); ++i) {
            delete _sub_head_reader_vector[i];
        }
        _sub_head_reader_vector.clear();

        for (int64_t i = 0; i < _meta->head_dir_size; ++i) {
            SeisSubHeadReaderColumn* reader = new SeisSubHeadReaderColumn(_fs, _hdfs_data_name,
                    _meta, i);
            if (!reader->Init()) {
                delete reader;
                return false;
            }
            _sub_head_reader_vector.push_back(reader);
        }
        _is_update_part_flag = true;
    }

    if (_key_list.size() == _meta->head_length || _key_list.size() == 0) {
        for (uint64_t i = 0; i < _sub_head_reader_vector.size(); ++i) {
            delete _sub_head_reader_vector[i];
        }
        _sub_head_reader_vector.clear();
        _is_update_part_flag = false;
    }

    Seek(0);
    return true;
}

bool SeisHeadUpdaterColumn::SetRowFilter(const RowFilter& filter) {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }
    _interval_list.clear();
    _interval_list = filter.GetAllScope();
    for (std::vector<RowScope>::iterator it = _interval_list.begin(); it != _interval_list.end();
            ++it) {
        if (it->GetStartTrace() == -1) {
            it->SetStartTrace(_meta->head_total_rows);
        }
        if (it->GetStopTrace() == -1) {
            it->SetStopTrace(_meta->head_total_rows);
        }
    }
    Seek(0);
    return true;
}

/**
 * GetKeys
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                get the length of head.
 * @return              return the length of head.
 */
//int SeisHeadUpdaterColumn::GetKeys() {
//    if (_is_close_flag) {
//        Err("object is closed, not to be used.\n");
//        return -1;
//    }
//
//    return _meta->head_length;
//}

/**
 * Seek
 *
 * @author              weibing
 * @version             0.2.7
 * @param               head_idx is the index of head.
 * @func                move the read offset to head_idx(th) head.
 * @return              return true if seek successfully, else return false.
 */
bool SeisHeadUpdaterColumn::Seek(int64_t head_idx) {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }

    return GetSeekSlider(_interval_list, head_idx, _head_slider);
}

/**
 * Put
 *
 * @author              weibing
 * @version             0.2.7
 * @param               head is the head data need to be written.
 * @func                update a head by filter.
 * @return              return true if put successfully, else return false.
 */
bool SeisHeadUpdaterColumn::Put(const void* head) {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }

//    int64_t t = _key_list.size();
    if (_key_list.size() > 0 && _key_list.size() < _meta->head_length) //we need read old head, then create new head, write new head
            {
        char *newData = new char[_meta->head_row_bytes];
        bool is_last_file_item;
        int64_t head_dir_idx = GetDirIndex(_meta->head_dir_front_array, _meta->head_dir_size,
                _head_slider.v_in_id);
        int64_t head_dir_front = _meta->head_dir_front_array[head_dir_idx];
        int64_t sub_head_idx = _head_slider.v_in_id - head_dir_front;
        bool hash_find_rst = _sub_head_reader_vector[head_dir_idx]->FindIndexItem(sub_head_idx,
                is_last_file_item);
        if (hash_find_rst)   //the old head is in log
        {
            if (is_last_file_item) //the old head is in the latest log, it is necessary to hflush the latest log so that the sub reader can read latest update data
            {
                if (!_sub_head_updater_vector[head_dir_idx]->HFlush()) {
                    delete[] newData;
                    return false;
                }
            }

            if (!_sub_head_reader_vector[head_dir_idx]->GetUpdateData(newData)) // the old head is not in the latest log
                    {
                delete[] newData;
                return false;
            }
        } else  //the old head is in original write file
        {
            if (!_sub_head_reader_vector[head_dir_idx]->GetWriteData(sub_head_idx, newData)) {
                delete[] newData;
                return false;
            }
        }

        int64_t sum_size = 0;
        for (std::list<int>::iterator i = _key_list.begin(); i != _key_list.end(); i++) //merge old head and new head field to create new head
                {
            memcpy(newData + _head_offset[(*i)], (char*) head + sum_size, _meta->head_sizes[(*i)]);
            sum_size += _meta->head_sizes[(*i)];
        }

        if (!Put(_head_slider.v_in_id, newData))   //put new head
                {
            delete[] newData;
            return false;
        }
        delete[] newData;

    } else  //put new head, there is no need to read old data
    {
        if (!Put(_head_slider.v_in_id, head)) {
            return false;
        }
    }

    GetNextSlider(_interval_list, _head_slider);
    return true;
}

/**
 * Put
 *
 * @author              weibing
 * @version             0.2.7
 * @param               head_idx is the index of head.
 *                      head is the head data need to be written.
 * @func                update a head by head index.
 * @return              return true if put successfully, else return false.
 */
bool SeisHeadUpdaterColumn::Put(int64_t head_idx, const void * head) {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }

    if (_is_compact_flag && _update_count >= MAX_UPDATED_ROW) {
        if (!AutoCompact()) {
            return false;
        }
        _update_count = 0;
    }

    if (head_idx < 0) {
        return false;
    }
    int64_t head_dir_idx = GetDirIndex(_meta->head_dir_front_array, _meta->head_dir_size, head_idx);
    int64_t head_dir_front = _meta->head_dir_front_array[head_dir_idx];
    int64_t sub_head_idx = head_idx - head_dir_front;

    if (_key_list.size() > 0 && _key_list.size() < _meta->head_length) {
        bool rst = _sub_head_updater_vector[head_dir_idx]->Put(sub_head_idx, head,
                _sub_head_reader_vector[head_dir_idx]);
        if (_is_compact_flag && rst) {
            _update_count++;
        }
        return rst;
    } else {
        bool rst = _sub_head_updater_vector[head_dir_idx]->Put(sub_head_idx, head, NULL);
        if (_is_compact_flag && rst) {
            _update_count++;
        }
        return rst;
    }
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
bool SeisHeadUpdaterColumn::Sync() {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }

    for (int64_t i = 0; i < _meta->head_dir_size; ++i) {
        if (!_sub_head_updater_vector[i]->Sync()) {
            return false;
        }
    }
    return true;
}

bool SeisHeadUpdaterColumn::AutoCompact() {

    for (uint64_t i = 0; i < _sub_head_updater_vector.size(); ++i) {
        delete _sub_head_updater_vector[i];
    }
    _sub_head_updater_vector.clear();

    for (uint64_t i = 0; i < _sub_head_reader_vector.size(); ++i) {
        delete _sub_head_reader_vector[i];
    }
    _sub_head_reader_vector.clear();

    SeisHeadCompactorColumn* headCompactor = new SeisHeadCompactorColumn(_fs, _hdfs_data_name,
            _meta);
    if (!headCompactor->Init()) {
        delete headCompactor;
        return false;
    }
    if (!headCompactor->Compact()) {
        delete headCompactor;
        return false;
    }
    delete headCompactor;

    for (int64_t i = 0; i < _meta->head_dir_size; ++i) {
        SeisSubHeadUpdaterColumn* updater = new SeisSubHeadUpdaterColumn(_fs, _hdfs_data_name,
                _meta, i);
        if (!updater->Init()) {
            delete updater;
            return false;
        }
        _sub_head_updater_vector.push_back(updater);
    }

    if (_is_update_part_flag) {
        for (int64_t i = 0; i < _meta->head_dir_size; ++i) {
            SeisSubHeadReaderColumn* reader = new SeisSubHeadReaderColumn(_fs, _hdfs_data_name,
                    _meta, i);
            if (!reader->Init()) {
                delete reader;
                return false;
            }
            _sub_head_reader_vector.push_back(reader);
        }
    }

    return true;
}

/**
 * Init
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                preliminary work for update data.
 * @return              return true if init successfully, else return false.
 */
bool SeisHeadUpdaterColumn::Init() {
//    if (_is_compact_flag) {
//        //get all the updated head row number
//        int64_t sum = 0;
//        for (int i = 0; i < _meta->head_dir_size; i++) {
//            std::vector<int64_t> index_vector;
//            std::vector<pair<int64_t, int64_t>> trun_vector;
//            std::string uuid_dir = _hdfs_data_name + "/" + _meta->head_dir_uuid_array[i];
//            if (!GetIndexInfo(_fs, uuid_dir, index_vector)) //get all the index information, these index information is the index of updated head
//                    {
//                return false;
//            }
//
//            if (!GetTruncateInfo(_fs, uuid_dir, trun_vector)) //get all the truncate information
//                    {
//                return false;
//            }
//
//            sum += GetUpdatedRow(index_vector, trun_vector);
//        }
//
//        if (sum >= MAX_UPDATED_ROW) {
//            if (!AutoCompact()) {
//                return false;
//            }
//        }
//    }

    for (int64_t i = 0; i < _meta->head_dir_size; ++i) {
        SeisSubHeadUpdaterColumn* updater = new SeisSubHeadUpdaterColumn(_fs, _hdfs_data_name,
                _meta, i);
        if (!updater->Init()) {
            delete updater;
            return false;
        }
        _sub_head_updater_vector.push_back(updater);
    }
    return true;
}
NAMESPACE_END_SEISFS
}
