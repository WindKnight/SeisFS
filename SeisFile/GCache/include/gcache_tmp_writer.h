/*
 * gcache_tmp_writer.h
 *
 *  Created on: Jan 4, 2016
 *      Author: wyd
 */

#ifndef GCACHE_TMP_WRITER_H_
#define GCACHE_TMP_WRITER_H_

#include <sys/types.h>

#include "gcache_global.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {

/*
 Writer是负责写文件的类型，通过Write，Seek接口可以向文件中写入数据，通过Truncate可以截断文件中现有的数据，最后通过Close可以关闭文件
 Writer是一个抽象的基类，需要为本地盘，集中存储，分布式存储分别定义子类，实现相应的方法。
 其中在分布式存储下不支持随机写，因此不支持Seek操作，如果调用Seek操作会报错。
 */
class Writer {
public:

    Writer() {
    }
    virtual ~Writer() {
    }

    /**
     * Writes at most max_len bytes of data to the file.
     * Return s the number of bytes that were actually written, or -1 if an error occurred.
     */
    virtual int64_t Write(const char *data, int64_t max_len) = 0;

    /**
     * For random-access devices, this function sets the current position to offset. whence as follows:
     * SEEK_SET: The offset is set to offset bytes
     * SEEK_CUR: The offset is set to its current location plus offset bytes.
     * SEEK_END: The offset is set to the size of the file plus offset bytes.
     */
    virtual bool Seek(int64_t offset, int whence) = 0;

    /**
     * Returns the position that data is written to.
     */
    virtual int64_t Pos() = 0;

    /**
     * Truncates the file to a size of precisely lenght bytes.
     */
    virtual bool Truncate(int64_t length) = 0;

    /**
     * Transfers all modified in-core data of the file to the disk device where that file resides.
     */
    virtual void Sync() = 0;

    //virtual bool Close() = 0;

};

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_TMP_WRITER_H_ */
