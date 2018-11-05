/*
 * gcache_seis_truncater.h
 *
 *  Created on: Jul 7, 2016
 *      Author: wb
 */

#ifndef GCACHE_SEIS_TRUNCATER_H_
#define GCACHE_SEIS_TRUNCATER_H_

#include "gcache_global.h"
#include "util/seisfs_shared_pointer.h"
#include "gcache_seis_meta.h"
#include <hdfs.h>
#include <gcache_seis_kvinfo.h>
#include <string>

NAMESPACE_BEGIN_SEISFS
namespace file {
class SeisTruncater {
public:

    /**
     *  Constructor.
     */
    SeisTruncater(hdfsFS fs, const std::string& hdfsDataName, SharedPtr<MetaSeisHDFS> meta);

    /**
     * Destructor.
     */
    ~SeisTruncater();

    /**
     * Merge head log data and original head write data
     */
    bool TruncateHead(int64_t heads);

    /**
     * Merge trace log data and original trace write data
     */
    bool TruncateTrace(int64_t traces);

private:

    hdfsFS _fs;      //hfds file system handle
    std::string _hdfs_data_name;                       //the full name of seis
    SharedPtr<MetaSeisHDFS> _meta;                         //the meta of seis
};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_TRUNCATER_H_ */
