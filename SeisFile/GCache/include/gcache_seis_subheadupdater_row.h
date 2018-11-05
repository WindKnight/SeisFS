#ifndef GCACHE_SEIS_SUBUPDATER_ROW_H_
#define GCACHE_SEIS_SUBUPDATER_ROW_H_

#include "gcache_global.h"
#include "gcache_seis_meta.h"
#include "util/seisfs_shared_pointer.h"
#include "gcache_seis_subheadreader_row.h"
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
class SeisSubHeadUpdaterRow {
public:

    /**
     *  Constructor.
     */
    SeisSubHeadUpdaterRow(hdfsFS fs, const std::string& hdfsDataName, SharedPtr<MetaSeisHDFS> meta,
            int64_t head_dir_idx);

    /**
     * Destructor.
     */
    ~SeisSubHeadUpdaterRow();

    /**
     * Update a head by head index.
     */
    bool Put(int64_t head_idx, const void* head, SeisSubHeadReaderRow* subHeadReader);

    /**
     * Transfers all modified in-core data of the file to the disk device where that file resides.
     */
    bool Sync();

    /**
     *  HFlush update data, so the reader can get the lasted updated head, in some time, when update a head we need to read the lasted updated head.
     */
    bool HFlush();

    /**
     *  Preliminary work for write data.
     */
    bool Init();

    /**
     * Close file.
     */
    bool Close();

    /**
     * Close updated file then open updated file.
     */
    bool ReOpenUpdatedFile();

private:

    hdfsFS _fs;   //hfds file system handle
    std::string _hdfs_data_name;                    //the full name of seis
    SharedPtr<MetaSeisHDFS> _meta;                      //the meta of seis
    std::map<int64_t, string> _head_idx_map;
    std::map<int64_t, string> _head_dat_map;
    std::map<int64_t, int64_t> _head_current_row_map;
    int64_t _head_current_row; //the current row of current file, it will increase when write a update head row to log
    hdfsFile _current_head_idx_fd; //the file handle of current index file, the index file store the head row number that indicates which head row are updated
    hdfsFile _current_head_dat_fd; //the file handle of current file to be written, the file is a log for writing updated data
    std::string _head_dir_uuid;                //the uuid of this head directory
    int64_t _head_dir_idx;                    //the index of this head directory
    bool _is_close_flag; //the flag indicate whether this object is closed or not
    std::string _head_updidx_name;              //the name of current index file
    std::string _head_upddat_name; //the name of current file to be written, the file is a log for writing updated data

    int64_t _update_dir_num;
};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_SUBUPDATER_ROW_H_ */
