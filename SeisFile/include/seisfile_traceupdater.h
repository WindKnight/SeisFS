/*
 * seisfile_traceupdater.h
 *
 *  Created on: Mar 26, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_TRACEUPDATER_H_
#define SEISFILE_TRACEUPDATER_H_

#include <stdint.h>

namespace seisfs {

class RowFilter;

namespace file {

class TraceUpdater {
public:

    /**
     * Destructor.
     */
    virtual ~TraceUpdater(){};

    /*
     * caller should not delete the pointer of filter after calling this function
     */
    virtual bool SetRowFilter(const RowFilter& filter) = 0;

    /**
     * Move the read offset to trace_idx(th) trace.
     */
    virtual bool Seek(int64_t trace_idx) = 0;

    /**
     * Update a trace by filter.
     */
    virtual bool Put(const void* trace) = 0;

    /**
     * Update a trace by trace index.
     */
    virtual bool Put(int64_t trace_idx, const void* trace) = 0;

    /**
     * Transfers all modified in-core data of the file to the disk device where that file resides.
     */
    virtual bool Sync() = 0;

    /**
     * Close file.
     */
    virtual bool Close() = 0;

private:



};

}

}

#endif /* SEISFILE_TRACEUPDATER_H_ */
