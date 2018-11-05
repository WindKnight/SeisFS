/*
 * gcache_tmp_writer_nas.h
 *
 *  Created on: Jan 7, 2016
 *      Author: wb
 */

#ifndef GCACHE_TMP_WRITER_CENTRAL_H_
#define GCACHE_TMP_WRITER_CENTRAL_H_

#include <boost/smart_ptr/shared_ptr.hpp>
#include <sys/types.h>

#include "gcache_global.h"
#include "gcache_tmp_writer.h"
#include "gcache_tmp_error.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {

/**
 * WriterNAS class is a child of Writer.
 * this class provides some functions of write in NAS.
 */
class WriterNAS: public Writer {
public:

    /**
     * constructor, construct a WriterNAS object.
     */
    WriterNAS(int fd, boost::shared_ptr<bool> isWriterAlive);

    /**
     * destructor, destruct a WriterNAS object.
     */
    virtual ~WriterNAS();

    /**
     * write at most max_len bytes of data to the file.
     * return the number of bytes that were actually written, or -1 if an error occurred.
     */
    virtual int64_t Write(const char *data, int64_t max_len);

    /**
     * for random-access devices, this function sets the current position to offset. whence as follows:
     * SEEK_SET: The offset is set to offset bytes
     * SEEK_CUR: The offset is set to its current location plus offset bytes.
     * SEEK_END: The offset is set to the size of the file plus offset bytes.
     */
    virtual bool Seek(int64_t offset, int whence);

    /**
     * get the position that data is written to.
     */
    virtual int64_t Pos();

    /**
     * truncate the file to a size of precisely length in bytes.
     */
    virtual bool Truncate(int64_t length);

    /**
     * transfers all modified in-core data of the file to the disk device where that file resides.
     */
    virtual void Sync();

private:

    /*member variable*/
    int _fd;                            //file descriptor
    boost::shared_ptr<bool> _isWriterAlive; //indicate if this writer is alive

};

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_TMP_WRITER_CENTRAL_H_ */
