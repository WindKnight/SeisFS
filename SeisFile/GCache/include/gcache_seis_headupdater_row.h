/*
 * gcache_seis_headupdater_row.h
 *
 *  Created on: Jul 19, 2016
 *      Author: wb
 */

#ifndef GCACHE_SEIS_UPDATER_ROW_H_
#define GCACHE_SEIS_UPDATER_ROW_H_

#include "gcache_global.h"
#include "gcache_seis_meta.h"
#include "util/seisfs_shared_pointer.h"
#include "gcache_seis_headreader_row.h"
#include "gcache_seis_subheadupdater_row.h"
#include "seisfile_headupdater.h"
#include "seisfs_head_filter.h"
#include <hdfs.h>
#include "gcache_seis_error.h"
#include "gcache_io.h"
#include "gcache_log.h"
#include "gcache_string.h"
#include "gcache_hdfs_utils.h"
#include "gcache_seis_utils.h"
#include "gcache_seis_headcompactor_row.h"
#include <string.h>

NAMESPACE_BEGIN_SEISFS
namespace file {
class SeisHeadUpdaterRow: public HeadUpdater {
public:

    /**
     * Destructor.
     */
    virtual ~SeisHeadUpdaterRow();

    /**
     * Set a filter.
     */
    virtual bool SetHeadFilter(const HeadFilter& filter);

    virtual bool SetRowFilter(const RowFilter& filter);
    /**
     * Get the length of head.
     */
//    virtual int GetKeys();

    /**
     * Move the read offset to head_idx(th) head.
     */
    virtual bool Seek(int64_t head_idx);

    /**
     * update a head
     */
    virtual bool Put(const void* head);

    /**
     * update a head
     */
    virtual bool Put(int64_t head_idx, const void* head);

    /**
     * Transfers all modified in-core data of the file to the disk device where that file resides.
     */
    virtual bool Sync();

    /**
     *  close file.
     */
    virtual bool Close();

    friend class HDFS;
    friend class SeisHeadWriterRow;

private:

    /**
     *  Constructor.
     */
    SeisHeadUpdaterRow(hdfsFS fs, const std::string& hdfs_data_name, SharedPtr<MetaSeisHDFS> meta,
            bool is_compact_flag);

    /**
     *  Preliminary work for write data.
     */
    bool Init();

    /**
     * Auto compact data.
     */
    bool AutoCompact();

    hdfsFS _fs;      //hfds file system handle
    std::vector<SeisSubHeadReaderRow*> _sub_head_reader_vector; //the collection of sub head reader, each sub reader does the real read work
    std::vector<SeisSubHeadUpdaterRow*> _sub_head_updater_vector; //the collection of sub head updater, each sub updater does the real update work
    SharedPtr<MetaSeisHDFS> _meta;                                //the meta of seis
    std::vector<RowScope> _interval_list; //the collection of interval, each interval contains the head number that user need
    std::string _hdfs_data_name;                         //the full name of seis
    Slider _head_slider;                                   //the slider of head
    int64_t * _head_offset;          //store the head field offset in a head row
    std::list<int> _key_list;            //store the key that need to be updated
    bool _is_close_flag; //the flag indicate whether this object is closed or not
    bool _is_update_part_flag; //the flag indicate whether update only part of a head or not
    int64_t _update_count;                      //the number of update operation
    bool _is_compact_flag;                   //the flag indicate whether compact

};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_UPDATER_ROW_H_ */
