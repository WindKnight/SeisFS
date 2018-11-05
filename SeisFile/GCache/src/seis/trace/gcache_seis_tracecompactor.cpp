/*
 * gcache_seis_tracecompactor.cpp
 *
 *  Created on: Jul 19, 2016
 *      Author: wb
 */

#include "gcache_seis_tracecompactor.h"

NAMESPACE_BEGIN_SEISFS
namespace file {
/**
 * SeisTraceCompactor
 *
 * @author              weibing
 * @version             0.2.7
 * @param               fs is the hdfs file system handle.
 *                      hdfs_data_name is the full name of seis.
 *                      meta is the meta of seis.
 * @func                construct a object.
 * @return              no return.
 */
SeisTraceCompactor::SeisTraceCompactor(hdfsFS fs, const std::string& hdfs_data_name,
        SharedPtr<MetaSeisHDFS> meta) {

    _meta = meta;
    for (int64_t i = 0; i < meta->trace_dir_size; ++i) {
        SeisSubTraceCompactor* compactor = new SeisSubTraceCompactor(fs, hdfs_data_name, meta, i);
        sub_trace_compactor_vector.push_back(compactor);
    }
}

/**
 * Compact
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                compact log.
 * @return              return true if compact successfully, else return false.
 */
bool SeisTraceCompactor::Compact() {
    for (int64_t i = 0; i < _meta->trace_dir_size; ++i) {
        if (!sub_trace_compactor_vector[i]->Compact()) {
            return false;
        }
    }
    return true;
}

/**
 * ~SeisTraceCompactor
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                destruct a object.
 * @return              no return.
 */
SeisTraceCompactor::~SeisTraceCompactor() {
    for (uint64_t i = 0; i < sub_trace_compactor_vector.size(); ++i) {
        delete sub_trace_compactor_vector[i];
    }
    sub_trace_compactor_vector.clear();
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
bool SeisTraceCompactor::Init() {
    for (int64_t i = 0; i < _meta->trace_dir_size; ++i) {
        if (!sub_trace_compactor_vector[i]->Init()) //each sub trace compactor init, sub compactor does the real compact work.
        {
            return false;
        }
    }
    return true;
}

NAMESPACE_END_SEISFS
}
