/*
 * gcache_seis_tracereader.h
 *
 *  Created on: Jul 8, 2016
 *      Author: zch
 */

#ifndef GCACHE_SEIS_TRACEREADER_H_
#define GCACHE_SEIS_TRACEREADER_H_

#include "gcache_global.h"
#include "gcache_seis_meta.h"
#include "seisfile_tracereader.h"
#include "util/seisfs_shared_pointer.h"
#include "gcache_seis_subtracereader.h"
#include "seisfs_row_filter.h"
#include <list>
#include <hdfs.h>
#include <unordered_map>
#include "gcache_seis_error.h"
#include "gcache_io.h"
#include "gcache_hdfs_utils.h"
#include "gcache_string.h"
#include <string.h>
#include "gcache_seis_utils.h"
#include "gcache_log.h"

using namespace __gnu_cxx;
NAMESPACE_BEGIN_SEISFS
namespace file {
class SeisTraceReader: public TraceReader {
public:

    /**
     * Destructor.
     */
    ~SeisTraceReader();

    /**
     * Set a filter.
     */
    bool SetRowFilter(const RowFilter& filter);

    /**
     * Get the total trace rows of seis.
     */
    int64_t GetTraceNum();

    /**
     * Get the trace length.
     */
    int64_t GetTraceSize();

    /**
     * Move the read offset to trace_idx(th) trace.
     */
    bool Seek(int64_t trace_idx);

    int64_t Tell();

    /**
     * Get a trace by trace index.
     */
    bool Get(int64_t trace_idx, void* trace);

    /**
     * Return true if there are more traces. The routine usually accompany with NextTrace.
     */
    bool HasNext();

    /**
     * Get a trace by filter.
     */
    bool Next(void* trace);

    /**
     * Close file.
     */
    bool Close();

    //return hostname by trace number
    bool GetLocations(const std::vector<int64_t> &trace_id,
            std::vector<std::vector<std::string>> &hostInfos);

    friend class HDFS;

    friend class SeisReader;

private:

    /**
     *  Constructor.
     */
    SeisTraceReader(hdfsFS fs, const std::string& hdfs_data_name, SharedPtr<MetaSeisHDFS> meta);

    /**
     *  Preliminary work for write data.
     */
    bool Init();

    std::vector<SeisSubTraceReader*> sub_trace_reader_vector; //the collection of sub trace reader, each sub reader does the real read work
    SharedPtr<MetaSeisHDFS> _meta;                                //the meta of seis
    std::vector<RowScope> _interval_list; //the collection of interval, each interval contains the trace number that user need
    Slider _trace_slider;
    int64_t _autoincrease_slider; //auto increased slider start with 0, only for method Tell()
    bool _is_close_flag; //the flag indicate whether this object is closed or not
};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_TRACEREADER_H_ */
