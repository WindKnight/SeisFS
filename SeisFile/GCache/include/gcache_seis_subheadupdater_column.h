#ifndef GCACHE_SEIS_SUBUPDATER_COLUMN_H_
#define GCACHE_SEIS_SUBUPDATER_COLUMN_H_

#include "gcache_global.h"
#include "gcache_seis_meta.h"
#include "util/seisfs_shared_pointer.h"
#include <hdfs.h>
#include "gcache_seis_subheadreader_column.h"
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
class SeisSubHeadUpdaterColumn {
public:

    SeisSubHeadUpdaterColumn(hdfsFS fs, const std::string& hdfsDataName,
            SharedPtr<MetaSeisHDFS> meta, int64_t head_dir_idx);

    ~SeisSubHeadUpdaterColumn();

//    int GetKeys();

    /**
     * update a head
     */
    bool Put(int64_t head_idx, const void* head, SeisSubHeadReaderColumn* subHeadReader);

    /**
     * Transfers all modified in-core data of the file to the disk device where that file resides.
     */
    bool Sync();

    bool HFlush();

    bool Init();

    bool Close();
    bool ReOpenUpdatedFile();

private:

    hdfsFS _fs;
    std::string _hdfs_data_name;
    SharedPtr<MetaSeisHDFS> _meta;

    //------------ice
    int64_t _head_current_column;
    std::map<int64_t, string> _head_idx_map;
    std::map<int64_t, string> _head_dat_map;
    std::map<int64_t, int64_t> _head_current_column_map;
    hdfsFile _current_head_idx_fd;
    hdfsFile _current_head_dat_fd;
    std::string _head_dir_uuid;
    int64_t _head_dir_idx;
    bool _is_close_flag;

    std::string _head_upddat_name;
    std::string _head_updidx_name;

    int64_t _update_dir_num;
};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_SUBUPDATER_COLUMN_H_ */
