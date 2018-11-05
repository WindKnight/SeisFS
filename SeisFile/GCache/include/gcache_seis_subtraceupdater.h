#ifndef GCACHE_SEIS_SUBTRACEUPDATER_H_
#define GCACHE_SEIS_SUBTRACEUPDATER_H_

#include "gcache_global.h"
#include "gcache_seis_meta.h"
#include "util/seisfs_shared_pointer.h"
#include <hdfs.h>
#include <vector>
#include <uuid/uuid.h>
#include "gcache_seis_error.h"
#include "gcache_io.h"
#include "gcache_hdfs_utils.h"
#include "gcache_seis_utils.h"
#include "gcache_string.h"
#include <string.h>
#include <map>

NAMESPACE_BEGIN_SEISFS
namespace file {
class SeisSubTraceUpdater {
public:

    /**
     *  Constructor.
     */
    SeisSubTraceUpdater(hdfsFS fs, const std::string& hdfsDataName, SharedPtr<MetaSeisHDFS> meta,
            int64_t trace_dir_idx);

    /**
     * Destructor.
     */
    ~SeisSubTraceUpdater();

    /**
     * Update a trace by trace index.
     */
    bool Put(int64_t trace_idx, const void* trace);

    /**
     * Transfers all modified in-core data of the file to the disk device where that file resides.
     */
    bool Sync();

    /**
     *  Preliminary work for write data.
     */
    bool Init();

    /**
     * Close file.
     */
    bool Close();

private:

    hdfsFS _fs;     //hfds file system handle
    std::string _hdfs_data_name;                      //the full name of seis
    SharedPtr<MetaSeisHDFS> _meta;                        //the meta of seis
    hdfsFile _current_trace_idx_fd; //the file handle of current index file, the index file store the trace row number that indicates which trace row are updated
    hdfsFile _current_trace_dat_fd; //the file handle of current file to be written, the file is a log for writing updated data
    int64_t _trace_current_row; //the current row of current file, it will increase when write a update trace row to log
    std::string _trace_updidx_name;
    std::string _trace_upddat_name;
    std::map<int64_t, string> _trace_idx_map;
    std::map<int64_t, string> _trace_dat_map;
    std::map<int64_t, int64_t> _trace_current_row_map;
    std::string _trace_dir_uuid;              //the uuid of this trace directory
    int64_t _trace_dir_idx;                  //the index of this trace directory
    bool _is_close_flag; //the flag indicate whether this object is closed or not
    int64_t _update_dir_num;

};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_SUBTRACEUPDATER_H_ */
