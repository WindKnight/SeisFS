/*
 * seisfile_tracewriter.h
 *
 *  Created on: Mar 26, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_TRACEWRITER_H_
#define SEISFILE_TRACEWRITER_H_

#include <stdint.h>


namespace seisfs {

namespace file {

class TraceWriter {
public:

    /**
     * Destructor.
     */
	virtual ~TraceWriter(){};

    /**
     * Append a trace.
     */
	virtual bool Write(const void* trace) = 0;

    /**
     * Returns the position that data is written to.
     */
	virtual int64_t Pos() = 0;

    /**
     * Transfers all modified in-core data of the file to the disk device where that file resides.
     */
	virtual bool Sync() = 0;

    /**
     *  Close file.
     */
	virtual bool Close() = 0;

	virtual bool Truncate(int64_t traces) = 0;

private:

};

}

}

#endif /* SEISFILE_TRACEWRITER_H_ */
