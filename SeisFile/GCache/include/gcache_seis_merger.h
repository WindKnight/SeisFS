/*
 * gcache_seis_merger.h
 *
 *  Created on: Jul 7, 2016
 *      Author: wb
 */

#ifndef GCACHE_SEIS_MERGER_H_
#define GCACHE_SEIS_MERGER_H_

#include "gcache_global.h"
#include "util/seisfs_shared_pointer.h"
#include "gcache_seis_meta.h"
#include <hdfs.h>
#include <string>

NAMESPACE_BEGIN_SEISFS
namespace file {
class SeisMerger {
public:

    /**
     *  Constructor.
     */
    SeisMerger(hdfsFS fs, const std::string& hdfsDataName, SharedPtr<MetaSeisHDFS> meta);

    /**
     * Destructor.
     */
    ~SeisMerger();

    /**
     * Merge log data and original write data
     */
    bool Merge(SharedPtr<MetaSeisHDFS> src_meta, const std::string _root_dir,
            const std::string& src_data_name);

private:

    /**
     * Merge head log data and original head write data
     */
    bool MergeHead(SharedPtr<MetaSeisHDFS> src_meta, const std::string hdfs_src_data_name);

    /**
     * Merge trace log data and original trace write data
     */
    bool MergeTrace(SharedPtr<MetaSeisHDFS> src_meta, const std::string hdfs_src_data_name);

    hdfsFS _fs;   //hfds file system handle
    std::string _hdfs_data_name;                    //the full name of seis
    SharedPtr<MetaSeisHDFS> _meta;                      //the meta of seis
};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_MERGER_H_ */
