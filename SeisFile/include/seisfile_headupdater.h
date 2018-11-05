/*
 * seisfile_headupdater.h
 *
 *  Created on: Mar 26, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_HEADUPDATER_H_
#define SEISFILE_HEADUPDATER_H_

namespace seisfs {

class HeadFilter;
class RowFilter;

namespace file {

class HeadUpdater {
public:

    virtual ~HeadUpdater(){};

    /*
     * caller should not delete the pointer of filter after calling this function
     */
    virtual bool SetHeadFilter(const HeadFilter& head_filter) = 0;

    /*
     * caller should not delete the pointer of filter after calling this function
     */
    virtual bool SetRowFilter(const RowFilter& row_filter) = 0;

    /**
     * Move the read offset to head_idx(th) head.
     */
    virtual bool Seek(int64_t head_idx) = 0;

    /**
     * update a head
     */
    virtual bool Put(const void* head) = 0;

    /**
     * update a head
     */
    virtual bool Put(int64_t head_idx, const void* head) = 0;

    /**
     * Transfers all modified in-core data of the file to the disk device where that file resides.
     */
    virtual bool Sync() = 0;

    /**
     *  close file.
     */
    virtual bool Close() = 0;

};

}

}

#endif /* SEISFILE_HEADUPDATER_H_ */
