/*
 * gcache_tmp_reader_hdfs.h
 *
 *  Created on: Dec 29, 2015
 *      Author: wyd
 */

#ifndef GCACHE_TMP_READER_DIS_H_
#define GCACHE_TMP_READER_DIS_H_

#include <boost/smart_ptr/shared_ptr.hpp>
#include <sys/types.h>
#include <cstdio>
#include <string>

#include "hdfs.h"
#include "gcache_global.h"
#include "gcache_tmp_reader.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {

class ReaderHDFS: public Reader {
public:

    /**
     * Constructor, construct a ReaderHDFS object.
     */
    ReaderHDFS(hdfsFile fd, hdfsFS fs, boost::shared_ptr<bool> isWriterAlive,
            boost::shared_ptr<std::string> sharedFileName);

    /**
     * Destructor, destruct a ReaderHDFS object.
     */
    virtual ~ReaderHDFS();

    /**
     * Initialize member.
     */
    bool Init();

    /**
     * Reads at most max_len bytes from the device into data, and return the number of bytes read.
     * If an error occurs, this function return -1.
     * 0 is returned when no more data is available for reading.
     */
    virtual int64_t Read(char *data, int64_t max_len);

    /**
     * Reads  in  at  most one less than size characters from stream and stores them into the buffer pointed to by s.  Reading
     * stops after an EOF or a newline.  If a newline is read, it is stored into the buffer.  A ’\0’ is stored after the last character
     * in the buffer.
     * Return the number of bytes read.
     * 0 is returned when no more data is available for reading.
     * If an error occurs, this function return -1.
     */
    virtual int64_t ReadLine(char* s, int size);

    /**
     * overloaded function.
     */
    virtual int64_t ReadLine(std::string& s);

    /**
     * For random-access devices, this function sets the current position to offset. whence as follows:
     * SEEK_SET: The offset is set to offset bytes, SEEK_CUR and SEEK_END are not supported.
     */
    virtual bool Seek(int64_t offset, int whence = SEEK_SET);

private:
    hdfsFile _fd;                                        //file descriptor
    hdfsFS _fs;                     //hdfs filesystem internal wrapper
    boost::shared_ptr<std::string> _sharedFileName;  //the name of file
    boost::shared_ptr<bool> _isWriterAlive;  //indicate if this writer is alive
};

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_TMP_READER_CENTRAL_H_ */
