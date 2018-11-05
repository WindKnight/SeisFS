/*
 * gcache_seis_headcompactor_column.h
 *
 *  Created on: Jul 8, 2016
 *      Author: wb
 */

#ifndef GCACHE_SEIS_HEADCOMPACTOR_COLUMN_H_
#define GCACHE_SEIS_HEADCOMPACTOR_COLUMN_H_

#include "gcache_global.h"
#include "gcache_seis_meta.h"
#include "util/seisfs_shared_pointer.h"
#include "gcache_seis_subheadcompactor_column.h"

using namespace __gnu_cxx;
NAMESPACE_BEGIN_SEISFS
namespace file {
class SeisHeadCompactorColumn {
public:

    /**
     *  Constructor.
     */
    SeisHeadCompactorColumn(hdfsFS fs, const std::string& hdfs_data_name,
            SharedPtr<MetaSeisHDFS> meta);

    /**
     * Destructor.
     */
    ~SeisHeadCompactorColumn();

    /**
     * Compact log.
     */
    bool Compact();

    /**
     *  Preliminary work for write data.
     */
    bool Init();

private:

    std::vector<SeisSubHeadCompactorColumn*> sub_head_compactor_vector; //the collection of sub head compactor, each sub compactor does the real read work
    SharedPtr<MetaSeisHDFS> _meta;                                //the meta of seis
};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_HEADCOMPACTOR_COLUMN_H_ */
