/*
 * seisfile_headreader.h
 *
 *  Created on: Mar 26, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_HEADREADER_H_
#define SEISFILE_HEADREADER_H_

#include <stdint.h>

namespace seisfs {

class HeadFilter;
class RowFilter;

namespace file {

class HeadReader {
public:

    virtual ~HeadReader(){}

    /*
     * caller should not delete the pointer of filter after calling this function
     */
    virtual bool SetHeadFilter(const HeadFilter& head_filter) = 0;

    /*
     * caller should not delete the pointer of filter after calling this function
     */
    virtual bool SetRowFilter(const RowFilter& row_filter) = 0;

    virtual int GetHeadSize() = 0; //return length of one head, in bytes

    virtual int64_t GetTraceNum() = 0;

    /**
     * Move the read offset to head_idx(th) head.
     */
    virtual bool Seek(int64_t head_idx) = 0;

    virtual int64_t Tell() = 0;

    virtual bool Get(int64_t head_idx, void* head) = 0;

    /**
     * Return true if there are more heads. The routine usually accompany with NextHead
     */
    virtual bool HasNext() = 0;

    virtual bool Next(void* head) = 0;

    virtual bool Close() = 0;

private:

};

}

}

#endif /* SEISFILE_HEADREADER_H_ */
