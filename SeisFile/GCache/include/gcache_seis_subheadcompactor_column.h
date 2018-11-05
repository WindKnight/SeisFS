#ifndef GCACHE_SEIS_SEISSUBHEADCOMPACTOR_COLUMN_H_
#define GCACHE_SEIS_SEISSUBHEADCOMPACTOR_COLUMN_H_

#include "gcache_global.h"
#include "gcache_seis_meta.h"
#include "util/seisfs_shared_pointer.h"
#include <hdfs.h>
#include <unordered_map>
#include <math.h>
#include "gcache_seis_helper.h"
#include "gcache_seis_error.h"
#include "gcache_io.h"
#include "gcache_string.h"
#include "string.h"
#include "gcache_seis_utils.h"

using namespace __gnu_cxx;
NAMESPACE_BEGIN_SEISFS
namespace file {
class SeisSubHeadCompactorColumn {
public:

    /**
     *  Constructor.
     */
    SeisSubHeadCompactorColumn(hdfsFS fs, const std::string& hdfs_data_name,
            SharedPtr<MetaSeisHDFS> meta, int64_t head_dir_idx);

    /**
     * Destructor.
     */
    ~SeisSubHeadCompactorColumn();

    /**
     * Compact log.
     */
    bool Compact();

    /**
     *  Preliminary work for write data.
     */
    bool Init();

private:
    bool ReadFile(hdfsFS fs, const std::string& file_name, char* cache, int64_t len,
            int64_t & cached_num);
    std::string _hdfs_data_name;                         //the full name of seis
    std::string _hdfs_cache_dir;
    bool _cache_flag;
    hdfsFS _fs;      //hfds file system handle
    SharedPtr<MetaSeisHDFS> _meta;                                //the meta of seis
//    std::vector<hdfsFile> _upd_dat_fd_vector;                             //the file handle vector, used for reading the updated data
    std::string _head_dir_uuid;                //the uuid of this head directory
    int64_t _head_dir_idx;                    //the index of this head directory
    vector<vector<pair<int64_t, pair<int, int64_t>>>*> _index_vector_array; //

    int64_t _head_total_rows;
    int64_t _write_column;
    //fd cache
    vector<int> _upd_file_number_cache; //caching the file number of update file that is opened       ice--change
    std::vector<RichFileHandle> _upd_dat_fd_vector;

    int64_t _update_dir_num;
    bool LoadIndex();
};

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_SEIS_SEISSUBHEADCOMPACTOR_COLUMN_H_ */
