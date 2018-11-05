#ifndef GCACHE_SEIS_SUBTRACEREADER_H_
#define GCACHE_SEIS_SUBTRACEREADER_H_

#include "gcache_global.h"
#include "gcache_seis_meta.h"
#include "util/seisfs_shared_pointer.h"
#include <list>
#include <hdfs.h>
#include <unordered_map>
#include "gcache_seis_helper.h"
#include "gcache_seis_error.h"
#include "gcache_io.h"
#include "gcache_hdfs_utils.h"
#include "gcache_string.h"
#include <string.h>
#include "gcache_seis_utils.h"

using namespace __gnu_cxx;
NAMESPACE_BEGIN_SEISFS
namespace file {
class SeisSubTraceReader {
public:

    /**
     *  Constructor.
     */
    SeisSubTraceReader(hdfsFS fs, const std::string& hdfs_data_name, SharedPtr<MetaSeisHDFS> meta,
            int64_t trace_dir_idx);

    /**
     * Destructor.
     */
    ~SeisSubTraceReader();

    /**
     * Get a trace by trace index.
     */
    bool Get(int64_t trace_idx, void* trace);

    /**
     *  Preliminary work for write data.
     */
    bool Init();

    /**
     * Close file.
     */
    bool Close();

    bool GetLocation(const int64_t trace_idx, std::vector<std::string> &hostname);

private:

    std::string _hdfs_data_name;                         //the full name of seis
    hdfsFS _fs;      //hfds file system handle
    SharedPtr<MetaSeisHDFS> _meta;                              //the meta of seis
    std::unordered_map<int64_t, pair<int, int64_t> >  _idx_hash_map; //hash map contains all the row number of updated trace
    std::vector<int64_t> _map_order_vector; // for LRU hash map
    std::vector<RichFileHandle> _upd_dat_fd_vector; //the file handle vector, used for reading the updated data
    std::vector<RichFileHandle> _wrt_dat_fd_vector; //the file handle vector, used for reading the original write data
    std::string _trace_dir_uuid;              //the uuid of this trace directory
    int64_t _trace_dir_idx;                  //the index of this trace directory
    bool _is_close_flag; //the flag indicate whether this object is closed or not
    vector<int> _upd_file_number_cache; //caching the file number of update file that is opened
    vector<int> _wrt_file_number_cache; //caching the file number of write file that is opened

    int64_t _update_dir_num;

    bool LoadIndex();
};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_SUBTRACEREADER_H_ */
