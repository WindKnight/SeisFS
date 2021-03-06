/*
 * seisfile_nas_headreader.h
 *
 *  Created on: Mar 26, 2018
 *      Author: cssl
 */

#ifndef SEISFILE_NAS_HEADREADER_H_
#define SEISFILE_NAS_HEADREADER_H_

#include "seisfile_headreader.h"
#include <seisfs_head_filter.h>
#include <seisfs_row_filter.h>
#include <seisfile_headreader.h>
#include <GEFile.h>
#include <seisfile_nas_meta.h>

namespace seisfs {

class HeadFilter;
class RowFilter;

namespace file {

class MetaNAS;
class Headtype;

class HeadReaderNAS : public HeadReader {
public:

    virtual ~HeadReaderNAS();

    HeadReaderNAS(const std::string& filename, MetaNAS *meta);

    /*
     * caller should not delete the pointer of filter after calling this function
     */
    bool SetHeadFilter(const HeadFilter& head_filter);

    /*
     * caller should not delete the pointer of filter after calling this function
     */
    bool SetRowFilter(const RowFilter& row_filter);

    int GetHeadSize(); //return length of one head, in bytes

    int64_t GetTraceNum();

    /**
     * Move the read offset to head_idx(th) head.
     */
    bool Seek(int64_t trace_num);

    int64_t Tell();

    bool Get(int64_t trace_num, void* head);

    /**
     * Return true if there are more heads. The routine usually accompany with NextHead
     */
    bool HasNext();

    bool Next(void* head);

    bool Close();

    bool Init();

private:
    int64_t GetRealNum(int64_t trace_num);

    HeadFilter _head_filter;
    RowFilter _row_filter;

    int64_t _cur_logic_num;
    int64_t _last_actual_num;

    std::string _head_filename;
    std::string _meta_filename;
    std::string _filename;
    std::string seisnas_home_path_;
    GEFile *_gf_head;
    GEFile *_gf_head_next;
    HeadType _head_type;
    Lifetime _lifetime_days;
    MetaNAS *_meta;

};

}

}

#endif /* SEISFILE_NAS_HEADREADER_H_ */
