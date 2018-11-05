/*
 * gcache_seis_traceupdater.cpp
 *
 *  Created on: Jul 19, 2016
 *      Author: wb
 */

#include "gcache_seis_traceupdater.h"

//extern int GCACHE_seis_errno;
NAMESPACE_BEGIN_SEISFS
namespace file {
/**
 * SeisTraceUpdater
 *
 * @author              weibing
 * @version             0.2.7
 * @param               fs is the hdfs file system handle.
 *                      hdfs_data_name is the full name of seis.
 *                      meta is the meta of seis.
 * @func                construct a object.
 * @return              no return.
 */
SeisTraceUpdater::SeisTraceUpdater(hdfsFS fs, const std::string& hdfs_data_name,
        SharedPtr<MetaSeisHDFS> meta, bool is_compact_flag) {
    _fs = fs;
    _hdfs_data_name = hdfs_data_name;
    _meta = meta;
    _trace_slider.v_id = -1;
    _trace_slider.v_in_id = -1;
    _interval_list.clear();
    _interval_list.push_back(RowScope(0, _meta->trace_total_rows));
    GetNextSlider(_interval_list, _trace_slider);
    _is_close_flag = false;
    _update_count = 0;
    _is_compact_flag = is_compact_flag;
}

/**
 * ~SeisTraceUpdater
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                destruct a object.
 * @return              no return.
 */
SeisTraceUpdater::~SeisTraceUpdater() {
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
bool SeisTraceUpdater::Close() {
    if (!_is_close_flag) {
        for (uint64_t i = 0; i < _sub_trace_updater_vector.size(); ++i) {
            delete _sub_trace_updater_vector[i];
        }
        _sub_trace_updater_vector.clear();
        _is_close_flag = true;
    }
    return true;
}

/**
 * SetFilter
 *
 * @author              weibing
 * @version             0.2.7
 * @param               filter has a number of intervals, the intervals contain the index of trace than user need.
 * @func                set a filter.
 * @return              return true if set filter successfully, else return false.
 */
bool SeisTraceUpdater::SetRowFilter(const RowFilter& filter) {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }
    _interval_list.clear();
    _interval_list = filter.GetAllScope();
    for (std::vector<RowScope>::iterator it = _interval_list.begin(); it != _interval_list.end();
            ++it) {
        if (it->GetStartTrace() == -1) {
            it->SetStartTrace(_meta->trace_total_rows);
        }
        if (it->GetStopTrace() == -1) {
            it->SetStopTrace(_meta->trace_total_rows);
        }
    }
    Seek(0);
    return true;
}

/**
 * Seek
 *
 * @author              weibing
 * @version             0.2.7
 * @param               trace_idx is the index of trace.
 * @func                move the read offset to trace_idx(th) trace.
 * @return              return true if seek successfully, else return false.
 */
bool SeisTraceUpdater::Seek(int64_t trace_idx) {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }

    return GetSeekSlider(_interval_list, trace_idx, _trace_slider);
}

/**
 * Put
 *
 * @author              weibing
 * @version             0.2.7
 * @param               trace is the trace data need to be written.
 * @func                update a trace by filter.
 * @return              return true if put successfully, else return false.
 */
bool SeisTraceUpdater::Put(const void* trace) {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }

    if (!Put(_trace_slider.v_in_id, trace)) {
        return false;
    }

    GetNextSlider(_interval_list, _trace_slider);
    return true;
}

/**
 * Put
 *
 * @author              weibing
 * @version             0.2.7
 * @param               trace_idx is the index of trace.
 *                      trace is the trace data need to be written.
 * @func                update a trace by trace index.
 * @return              return true if put successfully, else return false.
 */
bool SeisTraceUpdater::Put(int64_t trace_idx, const void* trace) {
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

    if (trace_idx < 0) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PARAMERR;
        return false;
    }
    int64_t trace_dir_idx = GetDirIndex(_meta->trace_dir_front_array, _meta->trace_dir_size,
            trace_idx);
    int64_t trace_dir_front = _meta->trace_dir_front_array[trace_dir_idx];
    int64_t sub_trace_idx = trace_idx - trace_dir_front;
    bool rst = _sub_trace_updater_vector[trace_dir_idx]->Put(sub_trace_idx, trace);
    if (_is_compact_flag && rst) {
        _update_count++;
    }
    return rst;
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
bool SeisTraceUpdater::Sync() {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }

    for (int64_t i = 0; i < _meta->trace_dir_size; ++i) {
        if (!_sub_trace_updater_vector[i]->Sync()) {
            return false;
        }
    }
    return true;
}

/**
 * AutoCompact
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                compact data when updated row reach a certain quantity.
 * @return              return true if compact successfully, else return false.
 */
bool SeisTraceUpdater::AutoCompact() {

    for (uint64_t i = 0; i < _sub_trace_updater_vector.size(); ++i) {
        delete _sub_trace_updater_vector[i];
    }
    _sub_trace_updater_vector.clear();

    SeisTraceCompactor* traceCompactor = new SeisTraceCompactor(_fs, _hdfs_data_name, _meta);
    if (!traceCompactor->Init()) {
        delete traceCompactor;
        return false;
    }
    if (!traceCompactor->Compact()) {
        delete traceCompactor;
        return false;
    }
    delete traceCompactor;

    for (int64_t i = 0; i < _meta->trace_dir_size; ++i) {
        SeisSubTraceUpdater* updater = new SeisSubTraceUpdater(_fs, _hdfs_data_name, _meta, i);
        if (!updater->Init()) {
            delete updater;
            return false;
        }
        _sub_trace_updater_vector.push_back(updater);
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
bool SeisTraceUpdater::Init() {

//    if (_is_compact_flag) {
//        //get all the updated head row number
//        int64_t sum = 0;
//        for (int i = 0; i < _meta->trace_dir_size; i++) {
//            std::vector<int64_t> index_vector;
//            std::vector<pair<int64_t, int64_t>> trun_vector;
//            std::string uuid_dir = _hdfs_data_name + "/" + _meta->trace_dir_uuid_array[i];
//            if (!GetIndexInfo(_fs, uuid_dir, index_vector)) //get all the index information, these index information is the index of updated trace
//                    {
//                return false;
//            }
//
//            if (!GetTruncateInfo(_fs, uuid_dir, trun_vector)) //get all the truncate information
//                    {
//                return false;
//            }
//            sum += GetUpdatedRow(index_vector, trun_vector);
//        }
//
//        if (sum >= MAX_UPDATED_ROW) {
//            if (!AutoCompact()) {
//                return false;
//            }
//        }
//
//    }

    for (int64_t i = 0; i < _meta->trace_dir_size; ++i) {
        SeisSubTraceUpdater* updater = new SeisSubTraceUpdater(_fs, _hdfs_data_name, _meta, i);
        if (!updater->Init()) {
            delete updater;
            return false;
        }
        _sub_trace_updater_vector.push_back(updater);
    }
    return true;
}

NAMESPACE_END_SEISFS
}
