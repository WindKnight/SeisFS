/*
 * gcache_seis_headreader_row.h
 *
 *  Created on: Jul 19, 2016
 *      Author: wb
 */

#ifndef GCACHE_SEIS_HEADREADER_ROW_H_
#define GCACHE_SEIS_HEADREADER_ROW_H_

#include <list>
#include "util/seisfs_shared_pointer.h"
#include "gcache_seis_meta.h"
#include "seisfs_head_filter.h"
#include "seisfs_row_filter.h"
//#include "seisfile_headreader.h"
#include "gcache_seis_headreader.h"
#include "gcache_seis_subheadreader_row.h"
#include <hdfs.h>
#include <unordered_map>
#include "gcache_seis_error.h"
#include "gcache_io.h"
#include "gcache_string.h"
#include "gcache_hdfs_utils.h"
#include "gcache_seis_utils.h"
#include "gcache_log.h"
#include <string.h>

using namespace __gnu_cxx;
NAMESPACE_BEGIN_SEISFS
namespace file {
class SeisHeadReaderRow: public SeisHeadReader {
public:

    /**
     * Destructor.
     */
    virtual ~SeisHeadReaderRow();

    /**
     * Set a filter.
     */
    virtual bool SetHeadFilter(const HeadFilter& head_filter);

    virtual bool SetRowFilter(const RowFilter& row_filter);

    /**
     * Get the total head rows of seis.
     */
    virtual int64_t GetTraceNum();

//    virtual int GetKeyNum();
    /**
     * Get the head length.
     */
    virtual int GetHeadSize();

    /**
     * Move the read offset to head_idx(th) head.
     */
    virtual bool Seek(int64_t head_idx);

    virtual int64_t Tell();

    /**
     * Get a head by head index.
     */
    virtual bool Get(int64_t head_idx, void* head);

    /**
     * Return true if there are more heads. The routine usually accompany with NextHead
     */
    virtual bool HasNext();

    /**
     * Get a head by filter.
     */
    virtual bool Next(void* head);

    /**
     * Close file.
     */
    virtual bool Close();

    //return hostname by head number
    virtual bool GetLocations(const std::vector<int64_t> &head_id,
            std::vector<std::vector<std::string>> &hostInfos);

    friend class HDFS;

    friend class SeisReader;

    friend class SeisHeadUpdaterRow;

private:

    /**
     *  Constructor.
     */
    SeisHeadReaderRow(hdfsFS fs, const std::string& hdfs_data_name, SharedPtr<MetaSeisHDFS> meta);

    /**
     *  Preliminary work for write data.
     */
    bool Init();

    std::list<int> _key_list;            //store the key that need to be updated
    int64_t * _head_offset;          //store the head field offset in a head row
    std::unordered_map<int64_t, pair<int64_t, int64_t> > _idx_hash_map; //hash map contains all the row number of updated head
    std::vector<SeisSubHeadReaderRow*> sub_head_reader_vector; //the collection of sub head reader, each sub reader does the real read work
    SharedPtr<MetaSeisHDFS> _meta;                                //the meta of seis
    std::vector<RowScope> _interval_list; //the collection of interval, each interval contains the head number that user need
    Slider _head_slider; //the slider of head, when call Next() it will increase
    int64_t _autoincrease_slider; //auto increased slider start with 0, only for method Tell()
    bool _is_close_flag; //the flag indicate whether this object is closed or not
};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_HEADREADER_ROW_H_ */
