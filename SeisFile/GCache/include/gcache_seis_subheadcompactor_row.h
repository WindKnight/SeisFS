#ifndef GCACHE_SEIS_SEISSUBHEADCOMPACTOR_ROW_H_
#define GCACHE_SEIS_SEISSUBHEADCOMPACTOR_ROW_H_

#include "gcache_global.h"
#include "gcache_seis_meta.h"
#include "util/seisfs_shared_pointer.h"
#include <hdfs.h>
#include "gcache_seis_helper.h"
#include "gcache_seis_error.h"
#include "gcache_io.h"
#include "gcache_hdfs_utils.h"
#include "gcache_string.h"
#include "string.h"
#include "gcache_seis_utils.h"
#include <math.h>

using namespace __gnu_cxx;
NAMESPACE_BEGIN_SEISFS
namespace file {
class SeisSubHeadCompactorRow {
public:

    /**
     *  Constructor.
     */
    SeisSubHeadCompactorRow(hdfsFS fs, const std::string& hdfs_data_name,
            SharedPtr<MetaSeisHDFS> meta, int64_t head_dir_idx);

    /**
     * Destructor.
     */
    ~SeisSubHeadCompactorRow();

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
    std::string _head_dir_uuid;                //the uuid of this head directory
    int64_t _head_dir_idx;                    //the index of this head directory
    vector<vector<pair<int64_t, pair<int, int64_t>>>*> _index_vector_array; //the vector store vector pointer store row index, log index and offset(row) of log, used for compacting
    vector<int> _upd_file_number_cache; //caching the file number of update file that is opened

    int64_t _update_dir_num;
    bool LoadIndex();
};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_SEISSUBHEADCOMPACTOR_ROW_H_ */
