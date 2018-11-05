/*
 * seisfile_tracereader.h
 *
 *  Created on: Mar 26, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_TRACEREADER_H_
#define SEISFILE_TRACEREADER_H_

#include <stdint.h>

namespace seisfs {

class RowFilter;

namespace file {

class TraceReader {
public:

    /**
     * Destructor.
     */
	virtual ~TraceReader(){};

    /*
     * caller should not delete the pointer of filter after calling this function
     */
    virtual bool SetRowFilter(const RowFilter& row_filter) = 0;

    /**
     * Get the total trace rows of seis.
     */
	virtual int64_t GetTraceNum() = 0;

    /**
     * Get the trace length.
     */
	virtual int64_t GetTraceSize() = 0;//return length of one trace, in bytes

    /**
     * Move the read offset to trace_idx(th) trace.
     */
	virtual bool Seek(int64_t trace_idx) = 0;

	virtual int64_t Tell() = 0;

    /**
     * Get a trace by trace index.
     */
	virtual bool Get(int64_t trace_idx, void* trace) = 0;

    /**
     * Return true if there are more traces. The routine usually accompany with NextTrace.
     */
	virtual bool HasNext() = 0;

    /**
     * Get a trace by filter.
     */
	virtual bool Next(void* trace) = 0;

    /**
     * Close file.
     */
	virtual bool Close() = 0;

private:

};

}

}

#endif /* SEISFILE_TRACEREADER_H_ */
