/*
 * gcache_tmp_writer_local.h
 *
 *  Created on: Jan 4, 2016
 *      Author: wyd
 */

#ifndef GCACHE_TMP_WRITER_LOCAL_H_
#define GCACHE_TMP_WRITER_LOCAL_H_

#include <sys/types.h>
#include <boost/smart_ptr/shared_ptr.hpp>

#include "gcache_global.h"
#include "gcache_tmp_writer.h"
#include "gcache_tmp_error.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {

class WriterLocal: public Writer {
public:
    WriterLocal(int fd, boost::shared_ptr<bool> isWriterAlive);
    virtual ~WriterLocal();

//        bool Init();

    /**
     * Writes at most max_len bytes of data to the file.
     * Return s the number of bytes that were actually written, or -1 if an error occurred.
     */
    virtual int64_t Write(const char *data, int64_t max_len);

    /**
     * For random-access devices, this function sets the current position to offset. whence as follows:
     * SEEK_SET: The offset is set to offset bytes
     * SEEK_CUR: The offset is set to its current location plus offset bytes.
     * SEEK_END: The offset is set to the size of the file plus offset bytes.
     */
    virtual bool Seek(int64_t offset, int whence);

    /**
     * Returns the position that data is written to.
     */
    virtual int64_t Pos();

    /**
     * Truncates the file to a size of precisely lenght bytes.
     */
    virtual bool Truncate(int64_t length);

    /**
     * Transfers all modified in-core data of the file to the disk device where that file resides.
     */
    virtual void Sync();

private:

    int _fd;
    boost::shared_ptr<bool> _isWriterAlive; //indicate if this writer is alive

};

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_TMP_WRITER_LOCAL_H_ */
