/*
 * gcache_seis_reader.h
 *
 *  Created on: Jul 7, 2016
 *      Author: zch
 */

#ifndef GCACHE_SEIS_READER_H_
#define GCACHE_SEIS_READER_H_

#include <stdint.h>
#include "gcache_global.h"
#include "gcache_seis_meta.h"
#include "util/seisfs_shared_pointer.h"
#include "gcache_seis_tracereader.h"
#include "seisfile_headreader.h"
#include "seisfile_reader.h"
#include "seisfs_row_filter.h"
#include "seisfs_head_filter.h"
#include <hdfs.h>

NAMESPACE_BEGIN_SEISFS
namespace file {
class SeisReader: public Reader {
public:

    ~SeisReader();

    bool SetRowFilter(const RowFilter& filter);
    bool SetHeadFilter(const HeadFilter& filter);

    uint64_t GetTraceNum(); // return trace number

//    int GetKeyNum(); //return key number in head

    int GetHeadSize(); //return length of one trace, in bytes

    int GetTraceSize(); //return length of one head, in bytes

    bool Get(int64_t trace_idx, void *head, void* trace);

    /**
     * Move the read offset to trace_idx(th) trace.
     */
    bool Seek(int64_t trace_idx);
    int64_t Tell();
    /**
     * Return true if there are more traces. The routine usually accompany with NextTrace
     */
    bool HasNext();

    bool Next(void* head, void* trace);

    bool Close();

    //return hostname by row number
    bool GetLocations(const std::vector<int64_t> &trace_id,
            std::vector<std::vector<std::string>> &head_hostInfos,
            std::vector<std::vector<std::string>> &trace_hostInfos);

    friend class HDFS;

private:
    SeisReader(hdfsFS fs, const std::string& hdfs_data_name, SharedPtr<MetaSeisHDFS> meta);

    bool Init();

    SeisTraceReader* _traceReader;
    HeadReader* _headReader;

    hdfsFS _fs;
    std::string _hdfs_data_name;
    SharedPtr<MetaSeisHDFS> _meta;

};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_READER_H_ */
