/*
 * gcache_seis_tracecompactor.h
 *
 *  Created on: Jul 8, 2016
 *      Author: wb
 */

#ifndef GCACHE_SEIS_TRACECOMPACTOR_H_
#define GCACHE_SEIS_TRACECOMPACTOR_H_

#include "gcache_global.h"
#include "gcache_seis_meta.h"
#include "util/seisfs_shared_pointer.h"
#include "gcache_seis_subtracecompactor.h"
#include <list>
#include <hdfs.h>
#include <unordered_map>
#include "gcache_seis_error.h"
#include "gcache_io.h"
#include "gcache_hdfs_utils.h"
#include "gcache_string.h"
#include <string.h>

using namespace __gnu_cxx;
NAMESPACE_BEGIN_SEISFS
namespace file {
class SeisTraceCompactor {
public:

    /**
     *  Constructor.
     */
    SeisTraceCompactor(hdfsFS fs, const std::string& hdfs_data_name, SharedPtr<MetaSeisHDFS> meta);

    /**
     * Destructor.
     */
    ~SeisTraceCompactor();

    /**
     * Compact log.
     */
    bool Compact();

    /**
     *  Preliminary work for write data.
     */
    bool Init();

private:

    std::vector<SeisSubTraceCompactor*> sub_trace_compactor_vector; //the collection of sub trace compactor, each sub compactor does the real read work
    SharedPtr<MetaSeisHDFS> _meta;                                //the meta of seis
};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_TRACECOMPACTOR_H_ */
