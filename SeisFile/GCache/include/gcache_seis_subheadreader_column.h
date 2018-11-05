/*
 * gcache_seis_subheadreader_column.h
 *
 *  Created on: Jul 19, 2016
 *      Author: wb
 */

#ifndef GCACHE_SEIS_SUBHEADREADER_COLUMN_H_
#define GCACHE_SEIS_SUBHEADREADER_COLUMN_H_

#include <list>
#include "gcache_global.h"
#include "gcache_seis_meta.h"
#include "util/seisfs_shared_pointer.h"
#include <hdfs.h>
#include <unordered_map>
#include <string.h>
#include "gcache_seis_helper.h"
#include "gcache_seis_error.h"
#include "gcache_io.h"
#include "gcache_string.h"
#include "gcache_hdfs_utils.h"
#include "gcache_seis_utils.h"

using namespace __gnu_cxx;

NAMESPACE_BEGIN_SEISFS
namespace file {
class SeisSubHeadReaderColumn {
public:
    SeisSubHeadReaderColumn(hdfsFS fs, const std::string& hdfsDataName,
            SharedPtr<MetaSeisHDFS> meta, int64_t head_dir_idx);

    ~SeisSubHeadReaderColumn();
    bool Get(int64_t trace_idx, void* head);
    bool Close();
    bool Init();
    //-----------------ice
    void AddNewIndexItem(int64_t head_idx, std::string new_file_name, int64_t file_rows);
//     void AddNewIndexItem(int64_t head_idx, hdfsFile upd_dat_fd, int64_t file_rows);
    bool FindIndexItem(int64_t head_idx, bool& is_last_file_item);
    bool GetUpdateData(void* head);
    bool GetWriteData(int64_t head_idx, void* head);
    //----------------

    //for column using only
    void SetKeyList(std::list<int> *key_list);
    void SetHeadOffset(int64_t* head_offset);

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

    //for column using only
    std::string _hdfs_head_cache_name;  //cache data file name
    hdfsFile _cache_dat_fd;  //cache data file fd
    std::list<int> *_key_list;
    int64_t* _head_offset;
    char* _mem_cache;
    int64_t _mem_cache_begin;
    int64_t _cache_file_store_rows;
    int64_t _file_store_rows;

    int64_t _update_dir_num;
    bool LoadIndex();
};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_SUBHEADREADER_COLUMN_H_ */
