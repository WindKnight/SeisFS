#ifndef GCACHE_SEIS_SUBHEADREADER_ROW_H_
#define GCACHE_SEIS_SUBHEADREADER_ROW_H_

#include <boost/smart_ptr/shared_ptr.hpp>
#include <list>
#include "gcache_global.h"
#include "util/seisfs_shared_pointer.h"
#include "gcache_seis_meta.h"
#include <hdfs.h>
#include <unordered_map>
#include "gcache_seis_helper.h"
#include "gcache_seis_error.h"
#include "gcache_io.h"
#include "gcache_string.h"
#include "gcache_hdfs_utils.h"

#include "gcache_seis_utils.h"
#include <string.h>

using namespace __gnu_cxx;
NAMESPACE_BEGIN_SEISFS
namespace file {
class SeisSubHeadReaderRow {
public:

    /**
     *  Constructor.
     */
    SeisSubHeadReaderRow(hdfsFS fs, const std::string& hdfs_data_name, SharedPtr<MetaSeisHDFS> meta,
            int64_t head_dir_idx);

    /**
     * Destructor.
     */
    ~SeisSubHeadReaderRow();

    /**
     *  Preliminary work for read data.
     */
    bool Init();

    /**
     * Get a head by head index.
     */
    bool Get(int64_t head_idx, void* head);

    /**
     * Close file.
     */
    bool Close();

    /**
     * Add a new index item to hash map.
     */
    void AddNewIndexItem(int64_t head_idx, std::string new_file_name, int64_t file_rows);

    /**
     * Find a index item by hash map.
     */
    bool FindIndexItem(int64_t head_idx, bool& is_last_file_item);

    /**
     * Get updated data in log.
     */
    bool GetUpdateData(void* head);

    /**
     * Get write data in original write file.
     */
    bool GetWriteData(int64_t head_idx, void* head);

    bool GetLocation(const int64_t head_idx, std::vector<std::string> &hostname);

private:

    std::string _hdfs_data_name;                         //the full name of seis
    hdfsFS _fs;      //hfds file system handle
    SharedPtr<MetaSeisHDFS> _meta;                                //the meta of seis
    std::unordered_map<int64_t, pair<int, int64_t> > _idx_hash_map; //hash map contains all the row number of updated head
    std::vector<RichFileHandle> _upd_dat_fd_vector; //the file handle vector, used for reading the updated data
    std::vector<RichFileHandle> _wrt_dat_fd_vector; //the file handle vector, used for reading the original write data
    std::string _head_dir_uuid;                //the uuid of this head directory
    int64_t _head_dir_idx;                    //the index of this head directory
    bool _is_close_flag; //the flag indicate whether this object is closed or not
    std::unordered_map<int64_t, pair<int, int64_t>>::iterator _hash_find_rst; //the find result of hash map
    vector<int> _upd_file_number_cache; //caching the file number of update file that is opened
    vector<int> _wrt_file_number_cache; //caching the file number of write file that is opened

    int64_t _update_dir_num;
    bool LoadIndex();

};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_SUBHEADREADER_ROW_H_ */
