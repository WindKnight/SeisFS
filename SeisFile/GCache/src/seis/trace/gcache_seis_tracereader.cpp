/*
 * gcache_seis_tracereader.cpp
 *
 *  Created on: Jul 19, 2016
 *      Author: wb
 */

#include "gcache_seis_tracereader.h"

#define PRINTL printf("file : %s, line : %d\n", __FILE__, __LINE__);fflush(stdout);

//extern int GCACHE_seis_errno;
NAMESPACE_BEGIN_SEISFS
namespace file {
/**
 * SeisTraceReader
 *
 * @author              weibing
 * @version             0.2.7
 * @param               fs is the hdfs file system handle.
 *                      hdfs_data_name is the full name of seis.
 *                      meta is the meta of seis.
 * @func                construct a object.
 * @return              no return.
 */
SeisTraceReader::SeisTraceReader(hdfsFS fs, const std::string& hdfs_data_name,
        SharedPtr<MetaSeisHDFS> meta) {

    _meta = meta;
    _is_close_flag = false;
    _autoincrease_slider = 0;
    _trace_slider.v_id = -1;
    _trace_slider.v_in_id = -1;
    _interval_list.clear();
    _interval_list.push_back(RowScope(0, _meta->trace_total_rows));
    GetNextSlider(_interval_list, _trace_slider);
    for (int64_t i = 0; i < meta->trace_dir_size; ++i) {
        SeisSubTraceReader* reader = new SeisSubTraceReader(fs, hdfs_data_name, meta, i);
        sub_trace_reader_vector.push_back(reader);
    }
}

/**
 * ~SeisTraceReader
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                destruct a object.
 * @return              no return.
 */
SeisTraceReader::~SeisTraceReader() {
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
bool SeisTraceReader::Close() {
    if (!_is_close_flag) {
        for (uint64_t i = 0; i < sub_trace_reader_vector.size(); ++i) {
            delete sub_trace_reader_vector[i];
        }
        sub_trace_reader_vector.clear();
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
bool SeisTraceReader::SetRowFilter(const RowFilter& filter) {
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
 * GetTraceNum
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                get the total trace rows of seis.
 * @return              return the total trace rows of seis.
 */
int64_t SeisTraceReader::GetTraceNum() {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return -1;
    }

    return _meta->trace_total_rows;
}

/**
 * GetSamplesLength
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                get the trace length.
 * @return              return the length of trace.
 */
int64_t SeisTraceReader::GetTraceSize() {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return -1;
    }

    return _meta->trace_row_bytes;
}

/**
 * Get
 *
 * @author              weibing
 * @version             0.2.7
 * @param               trace_idx is the index of trace.
 *                      trace is the trace to be read.
 * @func                get a trace by trace index.
 * @return              return true if get successfully, else return false.
 */
bool SeisTraceReader::Get(int64_t trace_idx, void* trace) {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }

    if (trace_idx < 0) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PARAMERR;
        return false;
    }

    //find the corresponding sub trace reader to do the real read work
    int64_t trace_dir_idx = GetDirIndex(_meta->trace_dir_front_array, _meta->trace_dir_size,
            trace_idx);
    int64_t trace_dir_front = _meta->trace_dir_front_array[trace_dir_idx];
    int64_t sub_trace_idx = trace_idx - trace_dir_front;
    return sub_trace_reader_vector[trace_dir_idx]->Get(sub_trace_idx, trace);
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
bool SeisTraceReader::Seek(int64_t trace_idx) {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }

    if(GetSeekSlider(_interval_list, trace_idx, _trace_slider)){
        _autoincrease_slider = trace_idx;
        return true;
    }
    return false;
}

int64_t SeisTraceReader::Tell(){
    return _autoincrease_slider;
}

/**
 * HasNext
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                to determine whether next trace exist.
 * @return              return true if next trace exist, else return false.
 */
bool SeisTraceReader::HasNext() {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }
    Slider tmp = _trace_slider;
    return GetNextSlider(_interval_list, tmp);
}

/**
 * Next
 *
 * @author              weibing
 * @version             0.2.7
 * @param               trace is the trace to be read by filter.
 * @func                get a trace by filter.
 * @return              return true if get next trace successfully, else return false.
 */
bool SeisTraceReader::Next(void* trace) {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }

    if (!Get(_trace_slider.v_in_id, trace)) {
        return false;
    }

    GetNextSlider(_interval_list, _trace_slider);
    _autoincrease_slider++;
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
bool SeisTraceReader::Init() {
    for (int64_t i = 0; i < _meta->trace_dir_size; ++i) {
        if (!sub_trace_reader_vector[i]->Init()) //each sub trace reader init, sub reader does the real read work.
        {
            return false;
        }
    }
    _autoincrease_slider = 0;
    return true;
}

bool SeisTraceReader::GetLocations(const std::vector<int64_t> &trace_id,
        std::vector<std::vector<std::string>> &hostInfos) {
    if (_is_close_flag) {
        Err("object is closed, not to be used.\n");
        return false;
    }

    hostInfos.clear();
    std::vector<std::string> hostname;
    for (std::vector<int64_t>::const_iterator it = trace_id.begin(); it != trace_id.end(); it++) {
        if (*it < 0) {
            GCACHE_seis_errno = GCACHE_SEIS_ERR_PARAMERR;
            return false;
        }

        //find the corresponding sub trace reader to do the real work
        int64_t trace_dir_idx = GetDirIndex(_meta->trace_dir_front_array, _meta->trace_dir_size,
                *it);
        int64_t trace_dir_front = _meta->trace_dir_front_array[trace_dir_idx];
        int64_t sub_trace_idx = *it - trace_dir_front;
        if (sub_trace_reader_vector[trace_dir_idx]->GetLocation(sub_trace_idx, hostname)) {
            hostInfos.push_back(hostname);
        }

    }
    return true;
}

NAMESPACE_END_SEISFS
}
