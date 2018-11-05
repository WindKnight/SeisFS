/*
 * gcache_seis_headupdater_column.h
 *
 *  Created on: Jul 19, 2016
 *      Author: wb
 */

#ifndef GCACHE_SEIS_UPDATER_COLUMN_H_
#define GCACHE_SEIS_UPDATER_COLUMN_H_

#include <string.h>
#include "gcache_global.h"
#include "gcache_log.h"
#include "gcache_seis_utils.h"
#include "util/seisfs_shared_pointer.h"
#include "gcache_seis_headcompactor_column.h"
#include "gcache_seis_subheadreader_column.h"
#include "gcache_seis_subheadupdater_column.h"
#include "gcache_seis_meta.h"

#include "seisfs_head_filter.h"
#include "seisfile_headupdater.h"

NAMESPACE_BEGIN_SEISFS
namespace file {
class SeisHeadUpdaterColumn: public HeadUpdater {
public:

    virtual ~SeisHeadUpdaterColumn();

    virtual bool SetHeadFilter(const HeadFilter& filter);

    virtual bool SetRowFilter(const RowFilter& filter);

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
    friend class SeisHeadWriterColumn;

private:

    SeisHeadUpdaterColumn(hdfsFS fs, const std::string& hdfs_data_name,
            SharedPtr<MetaSeisHDFS> meta, bool is_compact_flag);

    bool Init();

    /**
     * Auto compact data.
     */
    bool AutoCompact();

    hdfsFS _fs;
    std::vector<SeisSubHeadReaderColumn*> _sub_head_reader_vector;
    std::vector<SeisSubHeadUpdaterColumn*> _sub_head_updater_vector;
    SharedPtr<MetaSeisHDFS> _meta;
    std::vector<RowScope> _interval_list;
    std::string _hdfs_data_name;
    Slider _head_slider;
    int64_t * _head_offset;
    std::list<int> _key_list;
    bool _is_close_flag;
    bool _is_update_part_flag;
    int64_t _update_count;
    bool _is_compact_flag;

};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_UPDATER_COLUMN_H_ */
