/*
 * gcache_tmp_writer_hdfs.h
 *
 *  Created on: Jan 7, 2016
 *      Author: wyd
 */

#ifndef GCACHE_TMP_WRITER_DIS_H_
#define GCACHE_TMP_WRITER_DIS_H_

#include <boost/smart_ptr/shared_ptr.hpp>
#include <sys/types.h>
#include <string>

#include "hdfs.h"
#include "gcache_global.h"
#include "gcache_tmp_writer.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {

class WriterHDFS: public Writer {
public:

    /**
     * constructor, initialize member.
     */
    WriterHDFS(hdfsFS fs, hdfsFile fd, boost::shared_ptr<bool> isWriterAlive);

    /**
     * destructor, close hdfs file.
     */
    virtual ~WriterHDFS();

    /**
     * write at most max_len bytes of data to the file.
     * return the number of bytes that were actually written, or -1 if an error occurred.
     */
    virtual int64_t Write(const char *data, int64_t max_len);

    /**
     * the seek function is not supported when writing a file.
     */
    virtual bool Seek(int64_t offset, int whence);

    /**
     * get the current offset in the file, in bytes.
     */
    virtual int64_t Pos();

    /**
     * the truncate function is not supported when writing a file.
     */
    virtual bool Truncate(int64_t length);

    /**
     * transfers all modified in-core data of the file to the disk device where that file resides.
     */
    virtual void Sync();

private:
    hdfsFile _fd;                           //file descriptor
    hdfsFS _fs;                             //hdfs filesystem internal wrapper
    boost::shared_ptr<bool> _isWriterAlive;  //indicate if this writer is alive
};

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_TMP_WRITER_DIS_H_ */
