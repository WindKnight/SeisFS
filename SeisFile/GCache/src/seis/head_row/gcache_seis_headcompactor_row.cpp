/*
 * gcache_seis_headcompactor_row.cpp
 *
 *  Created on: Jul 19, 2016
 *      Author: wb
 */

#include "gcache_seis_headcompactor_row.h"

NAMESPACE_BEGIN_SEISFS
namespace file {
/**
 * SeisHeadCompactorRow
 *
 * @author              weibing
 * @version             0.2.7
 * @param               fs is the hdfs file system handle.
 *                      hdfs_data_name is the full name of seis.
 *                      meta is the meta of seis.
 * @func                construct a object.
 * @return              no return.
 */
SeisHeadCompactorRow::SeisHeadCompactorRow(hdfsFS fs, const std::string& hdfs_data_name,
        SharedPtr<MetaSeisHDFS> meta) {

    _meta = meta;
    for (int64_t i = 0; i < meta->head_dir_size; ++i) {
        SeisSubHeadCompactorRow* compactor = new SeisSubHeadCompactorRow(fs, hdfs_data_name, meta,
                i);
        sub_head_compactor_vector.push_back(compactor);
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
bool SeisHeadCompactorRow::Compact() {
    for (int64_t i = 0; i < _meta->head_dir_size; ++i) {
        if (!sub_head_compactor_vector[i]->Compact()) {
            return false;
        }
    }
    return true;
}

/**
 * ~SeisHeadCompactorRow
 *
 * @author              weibing
 * @version             0.2.7
 * @param
 * @func                destruct a object.
 * @return              no return.
 */
SeisHeadCompactorRow::~SeisHeadCompactorRow() {

    for (uint64_t i = 0; i < sub_head_compactor_vector.size(); ++i) {
        delete sub_head_compactor_vector[i];
    }
    sub_head_compactor_vector.clear();
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
bool SeisHeadCompactorRow::Init() {
    for (int64_t i = 0; i < _meta->head_dir_size; ++i) {
        if (!sub_head_compactor_vector[i]->Init()) //each sub head compactor init, sub compactor does the real compact work.
        {
            return false;
        }
    }
    return true;
}

NAMESPACE_END_SEISFS
}
