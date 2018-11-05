/*
 * gcache_seis_traceupdater.h
 *
 *  Created on: Jul 8, 2016
 *      Author: zch
 */

#ifndef GCACHE_SEIS_TRACEUPDATER_H_
#define GCACHE_SEIS_TRACEUPDATER_H_

#include "gcache_global.h"
#include "gcache_seis_meta.h"
#include "util/seisfs_shared_pointer.h"
#include "gcache_seis_subtraceupdater.h"
#include "seisfile_traceupdater.h"
#include "seisfs_row_filter.h"
#include <hdfs.h>
#include <vector>
#include <uuid/uuid.h>
#include "gcache_seis_error.h"
#include "gcache_io.h"
#include "gcache_hdfs_utils.h"
#include "gcache_string.h"
#include "gcache_seis_utils.h"
#include "gcache_log.h"
#include "gcache_seis_tracecompactor.h"
#include <string.h>

NAMESPACE_BEGIN_SEISFS
namespace file {
class SeisTraceUpdater: public TraceUpdater {
public:

    /**
     * Destructor.
     */
    ~SeisTraceUpdater();

    /**
     * Set a filter.
     */
    bool SetRowFilter(const RowFilter& filter);

    /**
     * Move the read offset to trace_idx(th) trace.
     */
    bool Seek(int64_t trace_idx);

    /**
     * Update a trace by filter.
     */
    bool Put(const void* trace);

    /**
     * Update a trace by trace index.
     */
    bool Put(int64_t trace_idx, const void* trace);

    /**
     * Transfers all modified in-core data of the file to the disk device where that file resides.
     */
    bool Sync();

    /**
     * Close file.
     */
    bool Close();

    friend class HDFS;
    friend class SeisTraceWriter;

private:

    /**
     *  Constructor.
     */
    SeisTraceUpdater(hdfsFS fs, const std::string& hdfsDataName, SharedPtr<MetaSeisHDFS> meta,
            bool is_compact_flag);

    /**
     *  Preliminary work for write data.
     */
    bool Init();

    /**
     *  Auto compact data.
     */
    bool AutoCompact();

    hdfsFS _fs;      //hfds file system handle
    std::string _hdfs_data_name;                         //the full name of seis
    std::vector<SeisSubTraceUpdater*> _sub_trace_updater_vector; //the collection of sub trace updater, each sub updater does the real update work
    SharedPtr<MetaSeisHDFS> _meta;                                //the meta of seis
    std::vector<RowScope> _interval_list; //the collection of interval, each interval contains the trace number that user need
    Slider _trace_slider; //the slider of trace, when call Next() it will increase
    bool _is_close_flag; //the flag indicate whether this object is closed or not
    int64_t _update_count;                      //the number of update operation
    bool _is_compact_flag;                   //the flag indicate whether compact

};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_TRACEUPDATER_H_ */
