/*
 * seisfile_headwriter.h
 *
 *  Created on: Mar 26, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_HEADWRITER_H_
#define SEISFILE_HEADWRITER_H_

#include <stdint.h>

namespace seisfs {

namespace file {

class HeadWriter {
public:
    virtual ~HeadWriter(){};

    /**
     * Append a head
     */
    virtual bool Write(const void* head) = 0;

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

    virtual bool Truncate(int64_t trace_num) = 0;

};

}

}

#endif /* SEISFILE_HEADWRITER_H_ */
