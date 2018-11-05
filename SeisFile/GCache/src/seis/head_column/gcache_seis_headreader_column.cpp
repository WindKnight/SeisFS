/*
 * gcache_seis_headreader_column.cpp
 *
 *  Created on: Jul 19, 2016
 *      Author: wb
 */

#include "gcache_seis_headreader_column.h"

NAMESPACE_BEGIN_SEISFS
namespace file {
/**
 * SeisHeadReaderColumn
 *
 * @author              weibing
 * @version             0.2.7
 * @param               fs is the hdfs file system handle.
 *                      hdfs_data_name is the full name of seis.
 *                      meta is the meta of seis.
 * @func                construct a object.
 * @return              no return.
 */
SeisHeadReaderColumn::SeisHeadReaderColumn(hdfsFS fs, const std::string& hdfs_data_name,
        SharedPtr<MetaSeisHDFS> meta) {

    _meta = meta;
    _is_close_flag = false;
    _autoincrease_slider = 0;
    _head_slider.v_id = -1;
    _head_slider.v_in_id = -1;
    _interval_list.clear();
    _interval_list.push_back(RowScope(0, _meta->trace_total_rows));
    GetNextSlider(_interval_list, _head_slider);
    _key_list.clear();
    _idx_hash_map.clear();
    _head_offset = GetHeadOffset(meta->head_sizes, meta->head_length);
    for (int64_t i = 0; i < meta->head_dir_size; ++i) {
        SeisSubHeadReaderColumn* reader = new SeisSubHeadReaderColumn(fs, hdfs_data_name, meta, i);
        reader->SetHeadOffset(_head_offset);
        sub_head_reader_vector.push_back(reader);
    }

}

/**
 * ~SeisHeadReaderColumn
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                destruct a object.
 * @return              no return.
 */
SeisHeadReaderColumn::~SeisHeadReaderColumn() {
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
bool SeisHeadReaderColumn::Close() {
    if (!_is_close_flag) {
        for (uint64_t i = 0; i < sub_head_reader_vector.size(); ++i) {
            delete sub_head_reader_vector[i];
        }
        sub_head_reader_vector.clear();
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
bool SeisHeadReaderColumn::SetHeadFilter(const HeadFilter& head_filter) {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }

    _key_list = head_filter.GetKeyList();
    for (int64_t i = 0; i < _meta->head_dir_size; ++i) {
        sub_head_reader_vector[i]->SetKeyList(&_key_list);
        sub_head_reader_vector[i]->SetHeadOffset(_head_offset);
    }
    Seek(0);
    return true;
}

bool SeisHeadReaderColumn::SetRowFilter(const RowFilter& row_filter) {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }

    _interval_list.clear();
    _interval_list = row_filter.GetAllScope();
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
 * GetHeadNum
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                get the total head rows of seis.
 * @return              return the total head rows of seis.
 */
int64_t SeisHeadReaderColumn::GetTraceNum() {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return -1;
    }

    return _meta->head_total_rows;
}

/**
 * GetHeadSize
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                get the head length.
 * @return              return the length of head.
 */
int SeisHeadReaderColumn::GetHeadSize() {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return -1;
    }

    // if filter is set, return the needed size
    if(_key_list.size()>0){
        int64_t tmp = 0;
        for(std::list<int>::iterator it = _key_list.begin();it!=_key_list.end();it++){
            tmp+=_meta->head_sizes[*it];
        }
        return tmp;
    }

    return _meta->head_row_bytes;
}

//int SeisHeadReaderColumn::GetKeyNum() {
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
bool SeisHeadReaderColumn::Seek(int64_t head_idx) {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }

    if(GetSeekSlider(_interval_list, head_idx, _head_slider)){
        _autoincrease_slider = head_idx;
        return true;
    }
    return false;
}

int64_t SeisHeadReaderColumn::Tell(){
    return _autoincrease_slider;
}

/**
 * Get
 *
 * @author              weibing
 * @version             0.2.7
 * @param               head_idx is the index of head.
 *                      head is the head to be read.
 * @func                get a head by head index.
 * @return              return true if get successfully, else return false.
 */
bool SeisHeadReaderColumn::Get(int64_t head_idx, void* head) {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }

    if (head_idx < 0 || head_idx > _meta->head_total_rows) {
        return false;
    }
    int64_t head_dir_idx = GetDirIndex(_meta->head_dir_front_array, _meta->head_dir_size, head_idx);
    int64_t head_dir_front = _meta->head_dir_front_array[head_dir_idx];
    int64_t sub_head_idx = head_idx - head_dir_front;
    return sub_head_reader_vector[head_dir_idx]->Get(sub_head_idx, head);
}

/**
 * HasNext
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                to determine whether next head exist.
 * @return              return true if next head exist, else return false.
 */
bool SeisHeadReaderColumn::HasNext() {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }

    Slider tmp = _head_slider;
    return GetNextSlider(_interval_list, tmp);
}

/**
 * Next
 *
 * @author              weibing
 * @version             0.2.7
 * @param               head is the head to be read by filter.
 * @func                get a head by filter.
 * @return              return true if get next head successfully, else return false.
 */
bool SeisHeadReaderColumn::Next(void* head) {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }
    if (!Get(_head_slider.v_in_id, head)) {
        return false;
    }
    GetNextSlider(_interval_list, _head_slider);
    _autoincrease_slider++;
    return true;
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
bool SeisHeadReaderColumn::Init() {
    for (int64_t i = 0; i < _meta->head_dir_size; ++i) {
        if (!sub_head_reader_vector[i]->Init()) {
            return false;
        }
    }
    _autoincrease_slider = 0;
    return true;
}

bool SeisHeadReaderColumn::GetLocations(const std::vector<int64_t> &head_id,
        std::vector<std::vector<std::string>> &hostInfos) {
    Err("GetLocations is not support when headType is BY_COLUMN");
    return true;
}

NAMESPACE_END_SEISFS
}
