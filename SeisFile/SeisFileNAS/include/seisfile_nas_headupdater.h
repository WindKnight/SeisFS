/*
 * seisfile_headupdater.h
 *
 *  Created on: Mar 26, 2018
 *      Author: wyd
 */

#include <stdint.h>
#include <GEFile.h>
#include <seisfile_nas_meta.h>
#include <seisfs_row_filter.h>
#include <seisfs_head_filter.h>
#include <seisfile_headupdater.h>

#ifndef SEISFILE_NAS_HEADUPDATER_H_
#define SEISFILE_NAS_HEADUPDATER_H_

namespace seisfs {

class HeadFilter;
class RowFilter;

namespace file {

class HeadUpdaterNAS : public HeadUpdater {
public:

    virtual ~HeadUpdaterNAS(){};

    HeadUpdaterNAS(const std::string& filename);

    /*
     * caller should not delete the pointer of filter after calling this function
     */
    bool SetHeadFilter(const HeadFilter& head_filter);

    /*
     * caller should not delete the pointer of filter after calling this function
     */
    bool SetRowFilter(const RowFilter& row_filter);

    /**
     * Move the read offset to head_idx(th) head.
     */
    bool Seek(int64_t head_num);

    /**
     * update a head
     */
    bool Put(const void* head);

    /**
     * update a head
     */
    bool Put(int64_t head_num, const void* head);

    /**
     * Transfers all modified in-core data of the file to the disk device where that file resides.
     */
    bool Sync();

    /**
     *  close file.
     */
    bool Close();

    bool Init();

private:
    HeadFilter _head_filter;
    RowFilter _row_filter;

    int64_t _actual_trace_num;
    int64_t _num_of_trace_filter;  //total number of trace after filter

    std::string _head_filename;
    std::string _meta_filename;
    std::string _filename;
   std::string seisnas_home_path_;
    GEFile *_gf_head;
    HeadType _head_type;
    MetaNAS *_meta;

};

}

}

#endif /* SEISFILE_NAS_HEADUPDATER_H_ */
