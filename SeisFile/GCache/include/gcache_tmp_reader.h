/*
 * gcache_tmp_reader.h
 *
 *  Created on: Dec 29, 2015
 *      Author: wyd
 */

#ifndef GCACHE_TMP_READER_H_
#define GCACHE_TMP_READER_H_

#include <sys/types.h>
#include <string>

#include "gcache_global.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {

/*
 Reader是负责读文件类型，通过Reader提供的Read，Seek操作可以从文件中读取数据，读取之后通过调用Close方法可以关闭文件。
 Reader类型是一个抽象的基类，需要为本地盘，集中存储，分布式存储分别定义子类，实现相应的方法。
 这样做的好处是可以定义同意的接口，并且在不同的存储类型上有相应的特殊处理。
 */
class Reader {
public:

    Reader() {
    }
    virtual ~Reader() {
    }

    /*
     * Reads at most max_len bytes from the device into data, and return the number of bytes read.
     * If an error occurs, this function return -1.
     *
     * 0 is returned when no more data is available for reading.
     */
    virtual int64_t Read(char *data, int64_t max_len) = 0;

    /*
     * Reads  in  at  most one less than size characters from stream and stores them into the buffer pointed to by s.  Reading
     * stops after an EOF or a newline.  If a newline is read, it is stored into the buffer.  A ’\0’ is stored after the last character
     in the buffer.
     Return the number of bytes read.
     0 is returned when no more data is available for reading.
     If an error occurs, this function return -1.
     */
    virtual int64_t ReadLine(char* s, int size) = 0;

    /**
     * overloaded function.
     */
    virtual int64_t ReadLine(std::string& s) = 0;
    /**
     * For random-access devices, this function sets the current position to offset. whence as follows:
     * SEEK_SET: The offset is set to offset bytes
     * SEEK_CUR: The offset is set to its current location plus offset bytes.
     * SEEK_END: The offset is set to the size of the file plus offset bytes.
     */
    virtual bool Seek(int64_t offset, int whence) = 0;

    //virtual bool Close() = 0;

};

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_TMP_READER_H_ */
