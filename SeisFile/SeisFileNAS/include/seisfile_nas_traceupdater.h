/*
 * seisfile_traceupdater.h
 *
 *  Created on: Mar 26, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_NAS_TRACEUPDATER_H_
#define SEISFILE_NAS_TRACEUPDATER_H_

#include <stdint.h>
#include <GEFile.h>
#include <seisfile_nas_meta.h>
#include <seisfs_row_filter.h>
#include <seisfile_traceupdater.h>

namespace seisfs {

class RowFilter;

namespace file {

class TraceType;
class MetaNAS;

class TraceUpdaterNAS : public TraceUpdater{
public:

    /**
     * Destructor.
     */
    virtual ~TraceUpdaterNAS(){};

    TraceUpdaterNAS(const std::string& filename);

    /*
     * caller should not delete the pointer of filter after calling this function
     */
    bool SetRowFilter(const RowFilter& filter);

    /**
     * Move the read offset to trace_idx(th) trace.
     */
    bool Seek(int64_t trace_num);

    /**
     * Update a trace by filter.
     */
    bool Put(const void* trace);

    /**
     * Update a trace by trace index.
     */
    bool Put(int64_t trace_num, const void* trace);

    /**
     * Transfers all modified in-core data of the file to the disk device where that file resides.
     */
    bool Sync();

    /**
     * Close file.
     */
    bool Close();


    bool Init();

private:
    RowFilter _row_filter;

    int64_t _actual_trace_num;
    int64_t _num_of_trace_filter;  //total number of trace after filter

    std::string _trace_filename;
    std::string _meta_filename;
    std::string _filename;
   std::string seisnas_home_path_;
    GEFile *_gf_trace;
    TraceType _trace_type;
    MetaNAS *_meta;


};

}

}

#endif /* SEISFILE_NAS_TRACEUPDATER_H_ */
