#ifndef GCACHE_SEIS_SUBTRACECOMPACTOR_H_
#define GCACHE_SEIS_SUBTRACECOMPACTOR_H_

#include "gcache_global.h"
#include "gcache_seis_meta.h"
#include "util/seisfs_shared_pointer.h"
#include <hdfs.h>
#include <unordered_map>
#include "gcache_seis_helper.h"
#include <math.h>
#include "gcache_seis_error.h"
#include "gcache_io.h"
#include "gcache_hdfs_utils.h"
#include "gcache_string.h"
#include "string.h"
#include "gcache_seis_utils.h"

using namespace __gnu_cxx;
NAMESPACE_BEGIN_SEISFS
namespace file {

class SeisSubTraceCompactor {
public:

    /**
     *  Constructor.
     */
    SeisSubTraceCompactor(hdfsFS fs, const std::string& hdfs_data_name,
            SharedPtr<MetaSeisHDFS> meta, int64_t trace_dir_idx);

    /**
     * Destructor.
     */
    ~SeisSubTraceCompactor();

    /**
     * Compact log.
     */
    bool Compact();

    /**
     *  Preliminary work for write data.
     */
    bool Init();

private:

    std::string _hdfs_data_name;                         //the full name of seis
    hdfsFS _fs;      //hfds file system handle
    SharedPtr<MetaSeisHDFS> _meta;                                //the meta of seis
    std::vector<RichFileHandle> _upd_dat_fd_vector; //the file handle vector, used for reading the updated data
    std::string _trace_dir_uuid;              //the uuid of this trace directory
    int64_t _trace_dir_idx;                  //the index of this trace directory
    vector<vector<pair<int64_t, pair<int, int64_t>>>*> _index_vector_array; //first is row  number, second is update file number, third is offset of update file correspond to row number
    vector<int> _upd_file_number_cache; //caching the file number of update file that is opened

    int64_t _update_dir_num;
    bool LoadIndex();
};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_SUBTRACECOMPACTOR_H_ */
