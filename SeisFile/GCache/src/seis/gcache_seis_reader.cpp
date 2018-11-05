/*
 * gcache_seis_reader.cpp
 *
 *  Created on: Jul 19, 2016
 *      Author: wb
 */

#include "gcache_seis_headreader_column.h"
#include "gcache_seis_reader.h"
#include "gcache_seis_error.h"
#include "gcache_io.h"
#include "gcache_seis_headreader_row.h"

NAMESPACE_BEGIN_SEISFS
namespace file {
SeisReader::SeisReader(hdfsFS fs, const std::string& hdfs_data_name, SharedPtr<MetaSeisHDFS> meta) {
    _headReader = NULL;
    _traceReader = NULL;
    _fs = fs;
    _hdfs_data_name = hdfs_data_name;
    _meta = meta;
}

SeisReader::~SeisReader() {
    delete _headReader;
    delete _traceReader;
}

bool SeisReader::SetRowFilter(const RowFilter& filter) {
    return _traceReader->SetRowFilter(filter) && _headReader->SetRowFilter(filter);
}
bool SeisReader::SetHeadFilter(const HeadFilter& filter) {
    return _headReader->SetHeadFilter(filter);
}

uint64_t SeisReader::GetTraceNum() {
    uint64_t trace_num = _traceReader->GetTraceNum();
    return trace_num;
}

//int SeisReader::GetKeyNum() {
//    int keys = _headReader->GetKeyNum();
//    return keys;
//}

int SeisReader::GetTraceSize() {
    int samples = _traceReader->GetTraceSize();
    return samples;
}

int SeisReader::GetHeadSize() {
    int samples = _headReader->GetHeadSize();
    return samples;
}

bool SeisReader::Get(int64_t trace_idx, void *head, void* trace) {
    bool head_read_flag = _headReader->Get(trace_idx, head);
    bool trace_read_flag = _traceReader->Get(trace_idx, trace);
    return head_read_flag && trace_read_flag;
}

/**
 * Move the read offset to trace_idx(th) trace.
 */
bool SeisReader::Seek(int64_t trace_idx) {
    bool head_seek_flag = _headReader->Seek(trace_idx);
    bool trace_seek_flag = _traceReader->Seek(trace_idx);
    return head_seek_flag && trace_seek_flag;
}

int64_t SeisReader::Tell(){
    int64_t tell=_headReader->Tell();
    return tell==_traceReader->Tell()?tell:-1;
}
/**
 * Return true if there are more traces. The routine usually accompany with NextTrace
 */
bool SeisReader::HasNext() {
    bool head_hasnext_flag = _headReader->HasNext();
    bool trace_hasnext_flag = _traceReader->HasNext();
    return head_hasnext_flag && trace_hasnext_flag;
}

bool SeisReader::Next(void* head, void* trace) {
    bool head_next_flag = _headReader->Next(head);
    bool trace_next_flag = _traceReader->Next(trace);
    return head_next_flag && trace_next_flag;
}

bool SeisReader::Close() {
    return _headReader->Close() && _traceReader->Close();

}

bool SeisReader::Init() {
    _traceReader = new SeisTraceReader(_fs, _hdfs_data_name, _meta);
    if (!_traceReader->Init()) {
        return false;
    }

    switch (_meta->head_placement) {
        case BY_ROW: {
            SeisHeadReaderRow* headReaderRow = new SeisHeadReaderRow(_fs, _hdfs_data_name, _meta);
            if (!headReaderRow->Init()) {
                delete headReaderRow;
                return false;
            }
            _headReader = headReaderRow;
            return true;
        }
            break;
        case BY_COLUMN: {
            SeisHeadReaderColumn* headReaderColumn = new SeisHeadReaderColumn(_fs, _hdfs_data_name,
                    _meta);
            if (!headReaderColumn->Init()) {
                delete headReaderColumn;
                return false;
            }
            _headReader = headReaderColumn;
            return true;
        }
            break;
        default: {
            return false;
        }
            break;
    }
}

bool SeisReader::GetLocations(const std::vector<int64_t> &trace_id,
        std::vector<std::vector<std::string>> &head_hostInfos,
        std::vector<std::vector<std::string>> &trace_hostInfos) {

    if (_meta->head_placement == BY_COLUMN) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_NOT_SUPPORT;
        return false;
    }
    if (_traceReader->GetLocations(trace_id, trace_hostInfos)
            && ((SeisHeadReaderRow*) _headReader)->GetLocations(trace_id, head_hostInfos)) {
        return true;
    }
    return false;
}

NAMESPACE_END_SEISFS
}
