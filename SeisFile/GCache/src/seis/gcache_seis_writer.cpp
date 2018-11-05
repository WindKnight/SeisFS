/*
 * gcache_seis_writer.cpp
 *
 *  Created on: Jul 19, 2016
 *      Author: wb
 */
#include "gcache_seis_writer.h"
#include "gcache_seis_error.h"
#include "gcache_io.h"
#include "gcache_seis_headwriter_row.h"
#include "gcache_seis_headwriter_column.h"

NAMESPACE_BEGIN_SEISFS
namespace file {

SeisWriter::SeisWriter(hdfsFS fs, const std::string& hdfs_data_name, SharedPtr<MetaSeisHDFS> meta,
        StorageSeis* storage_seis, std::string data_name) {

    _fs = fs;
    _hdfs_data_name = hdfs_data_name;
    _meta = meta;
    _traceWriter = NULL;
    _headWriter = NULL;
    _data_name = data_name;
    _storage_seis = storage_seis;
}

SeisWriter::~SeisWriter() {
    delete _headWriter;
    delete _traceWriter;

}

/**
 * Append a trace head
 */
bool SeisWriter::Write(const void* head, const void* trace) {
    return _headWriter->Write(head) && _traceWriter->Write(trace);
}

/**
 * Returns the position that data is written to.
 */
int64_t SeisWriter::Pos() {
    int64_t head_pos = _headWriter->Pos();
    int64_t trace_pos = _headWriter->Pos();
    if (head_pos != trace_pos) {
        return -1;
    }
    if (head_pos == -1) {
        return -1;
    }
    return head_pos;
}

/**
 * Transfers all modified in-core data of the file to the disk device where that file resides.
 */
bool SeisWriter::Sync() {
    return _headWriter->Sync() && _traceWriter->Sync();
}

/**
 * Close file.
 */
bool SeisWriter::Close() {
    return _headWriter->Close() && _traceWriter->Close();
}

bool SeisWriter::Init() {
    _traceWriter = new SeisTraceWriter(_fs, _hdfs_data_name, _meta, _storage_seis, _data_name);
    if (!_traceWriter->Init()) {
        return false;
    }
    switch (_meta->head_placement) {
        case BY_ROW: {
            SeisHeadWriterRow* headWriterRow = new SeisHeadWriterRow(_fs, _hdfs_data_name, _meta,
                    _storage_seis, _data_name);
            if (!headWriterRow->Init()) {
                delete headWriterRow;
                return false;
            }
            _headWriter = headWriterRow;
            return true;
        }
            break;
        case BY_COLUMN: {
            SeisHeadWriterColumn* headWriterColumn = new SeisHeadWriterColumn(_fs, _hdfs_data_name,
                    _meta, _storage_seis, _data_name);
            if (!headWriterColumn->Init()) {
                delete headWriterColumn;
                return false;
            }
            _headWriter = headWriterColumn;
            return true;
        }
            break;
        default: {
            return false;
        }
            break;
    }
}

bool SeisWriter::Truncate(int64_t heads) {
    SeisTruncater* truncater = new SeisTruncater(_fs, _hdfs_data_name, _meta);
    if (!truncater->TruncateHead(heads)) {
        delete truncater;
        return false;
    }
    if (!truncater->TruncateTrace(heads)) {
        delete truncater;
        return false;
    }
    delete truncater;

    if (!_storage_seis->PutMeta(_meta, _data_name)) {
        GCACHE_seis_errno = GCACHE_SEIS_ERR_PUTMETA;
        return false;
    }
    TouchSeisModifyTime(_fs, _hdfs_data_name);

    return Init();
}

NAMESPACE_END_SEISFS
}
