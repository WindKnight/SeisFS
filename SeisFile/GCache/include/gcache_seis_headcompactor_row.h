/*
 * gcache_seis_headcompactor_row.h
 *
 *  Created on: Jul 8, 2016
 *      Author: wb
 */

#ifndef GCACHE_SEIS_HEADCOMPACTOR_ROW_H_
#define GCACHE_SEIS_HEADCOMPACTOR_ROW_H_

#include "gcache_global.h"
#include "gcache_seis_meta.h"
#include "util/seisfs_shared_pointer.h"
#include "gcache_seis_subheadcompactor_row.h"
#include <list>
#include <hdfs.h>
#include <unordered_map>
#include <string.h>
#include <math.h>
#include "gcache_seis_error.h"
#include "gcache_io.h"
#include "gcache_hdfs_utils.h"
#include "gcache_string.h"

using namespace __gnu_cxx;
NAMESPACE_BEGIN_SEISFS
namespace file {
class SeisHeadCompactorRow {
public:

    /**
     *  Constructor.
     */
    SeisHeadCompactorRow(hdfsFS fs, const std::string& hdfs_data_name,
            SharedPtr<MetaSeisHDFS> meta);

    /**
     * Destructor.
     */
    ~SeisHeadCompactorRow();

    /**
     * Compact log.
     */
    bool Compact();

    /**
     *  Preliminary work for write data.
     */
    bool Init();

private:

    std::vector<SeisSubHeadCompactorRow*> sub_head_compactor_vector; //the collection of sub head compactor, each sub compactor does the real read work
    SharedPtr<MetaSeisHDFS> _meta;                                //the meta of seis
};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_HEADCOMPACTOR_ROW_H_ */
