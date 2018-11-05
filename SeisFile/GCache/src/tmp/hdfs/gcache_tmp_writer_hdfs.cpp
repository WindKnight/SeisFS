/*
 * gcache_tmp_writer_dis.cpp
 *
 *  Created on: Jan 7, 2016
 *      Author: wyd
 */

#include <boost/smart_ptr/shared_ptr.hpp>
#include <cerrno>
#include <string>

#include "gcache_tmp_writer_hdfs.h"
#include "gcache_tmp_error.h"
#include "gcache_hdfs_utils.h"
#include "gcache_log.h"
#include "gcache_io.h"

#define MAX_WRITE_SIZE 1073741824 //1G

NAMESPACE_BEGIN_SEISFS
namespace tmp {

/**
 * constructor
 *
 * @author          weibing
 * @version                0.1.4
 * @param                fd is file descriptor
 *                                 direct_write indicates use direct I/O or not
 * @func                set member variable to initial values.
 * @return                no return.
 */
WriterHDFS::WriterHDFS(hdfsFS fs, hdfsFile fd, boost::shared_ptr<bool> isWriterAlive) :
        _fd(fd), _fs(fs) {

    _isWriterAlive = isWriterAlive;
    *_isWriterAlive = true;
}
/**
 * destructor
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                close hdfs file,
 * @return      no return.
 */
WriterHDFS::~WriterHDFS() {
    hdfsCloseFile(_fs, _fd);
    *_isWriterAlive = false;
}

/**
 * Write
 *
 * @author          weibing
 * @version                0.1.4
 * @param       data is the data to be written
 *              max_len is the length of data to be written
 * @func                writes data to the file.
 * @return      if write successfully return the length of data written to file, else return -1.
 */
int64_t WriterHDFS::Write(const char *data, int64_t max_len) {
    int64_t ret_len = 0;
    ret_len = SafeWrite(_fs, _fd, data, max_len);
    return ret_len;
}

/**
 * Seek
 *
 * @author          weibing
 * @version                0.1.4
 * @param       offset is the position of the file
 *              whence argument specifies how the offset should be interpreted
 * @func                the seek function is not supported when writing a file.
 * @return      if seek successfully return true, else return false.
 */
bool WriterHDFS::Seek(int64_t offset, int whence) {
    Err("Seek function is not supported when writing a file in HDFS\n");
    errno = GcacheTemCombineErrno(GCACHE_TMP_ERR_NOT_SUPPORT, errno);
    return false;
}

/**
 * Position
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func            get the current offset in the file, in bytes.
 * @return      return the current offset in the file.
 */
int64_t WriterHDFS::Pos() {
    off_t offset = hdfsTell(_fs, _fd);
    return (int64_t) offset;
}

/**
 * Truncate
 *
 * @author          weibing
 * @version                0.1.4
 * @param       length is the final size of the file
 * @func                Truncate function is not supported when writing a file.
 * @return      if truncate successfully return true, else return false.
 */
bool WriterHDFS::Truncate(int64_t length) {
    Err("Truncate function is not supported when writing a file in HDFS\n");
    errno = GCACHE_TMP_ERR_NOT_SUPPORT;
    return false;
}

/**
 * Sync
 *
 * @author          weibing
 * @version                0.1.4
 * @param
 * @func                transfers all modified in-core data of the file to the disk device where that file resides.
 * @return      no return.
 */
void WriterHDFS::Sync() {
    hdfsSync(_fs, _fd);
}

NAMESPACE_END_SEISFS
}
