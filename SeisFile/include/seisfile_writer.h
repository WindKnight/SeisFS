/*
 * seisfile_writer.h
 *
 *  Created on: Mar 26, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_WRITER_H_
#define SEISFILE_WRITER_H_

#include <stdint.h>

namespace seisfs {

namespace file {

class Writer {
public:

    virtual ~Writer(){};

    /**
     * Append a trace head
     */
    virtual bool Write(const void* head, const void* trace) = 0;

    /**
     * Returns the position that data is written to.
     */
    virtual int64_t Pos() = 0;

    /**
     * Transfers all modified in-core data of the file to the disk device where that file resides.
     */
    virtual bool Sync() = 0;

    /**
     * Close file.
     */
    virtual bool Close() = 0;

    virtual bool Truncate(int64_t trace_num) = 0;

    virtual bool Init() = 0;

private:

};

}

}

#endif /* SEISFILE_WRITER_H_ */
