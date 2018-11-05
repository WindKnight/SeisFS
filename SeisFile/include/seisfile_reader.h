/*
 * seisfile_reader.h
 *
 *  Created on: Mar 26, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_READER_H_
#define SEISFILE_READER_H_
#include <stdint.h>

namespace seisfs {

class RowFilter;
class HeadFilter;

namespace file {

class Reader {
public:
	virtual ~Reader(){};

    virtual bool SetHeadFilter(const HeadFilter& head_filter) = 0;

    virtual bool SetRowFilter(const RowFilter& row_filter) = 0;

    virtual uint64_t GetTraceNum() = 0; // return trace number;

    virtual int GetTraceSize() = 0; //return length of one trace, in bytes

    virtual int GetHeadSize() = 0;//return length of one head, in bytes

    virtual bool Get(int64_t trace_idx, void *head, void* trace) = 0;

    /**
     * Move the read offset to trace_idx(th) trace.
     */
    virtual bool Seek(int64_t trace_idx) = 0;

    virtual int64_t Tell() = 0;
    /**
     * Return true if there are more traces. The routine usually accompany with NextTrace
     */
    virtual bool HasNext() = 0;

    virtual bool Next(void* head, void* trace) = 0;

    virtual bool Close() = 0;

private:

};

}

}

#endif /* SEISFILE_READER_H_ */
