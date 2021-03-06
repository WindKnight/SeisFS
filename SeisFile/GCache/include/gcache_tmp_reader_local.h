/*
 * gcache_tmp_reader_local.h
 *
 *  Created on: Dec 29, 2015
 *      Author: wyd
 */

#ifndef GCACHE_TMP_READER_LOCAL_H_
#define GCACHE_TMP_READER_LOCAL_H_

#include <sys/types.h>
#include <string>

#include "gcache_global.h"
#include "gcache_tmp_reader.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {
/**
 * ReaderLocal class is a child of Reader
 * This class implements some function to read data from temporary files in local storage
 */

class ReaderLocal: public Reader {
public:

    ReaderLocal(int fd, bool direct_read, char *map_buf);
    virtual ~ReaderLocal();

    bool Init();

    /**
     * Reads at most max_len bytes from the device into data, and return the number of bytes read.
     * If an error occurs, this function return -1.
     *
     * 0 is returned when no more data is available for reading.
     */
    virtual int64_t Read(char *data, int64_t max_len);

    /**
     * Reads  in  at  most one less than size characters from stream and stores them into the buffer pointed to by s.  Reading
     * stops after an EOF or a newline.  If a newline is read, it is stored into the buffer.  A ’\0’ is stored after the last character
     in the buffer.
     Return the number of bytes read.
     0 is returned when no more data is available for reading.
     If an error occurs, this function return -1.
     */
    virtual int64_t ReadLine(char* s, int size);

    /**
     * overloaded function.
     */
    virtual int64_t ReadLine(std::string& s);
    /**
     * For random-access devices, this function sets the current position to offset. whence as follows:
     * SEEK_SET: The offset is set to offset bytes
     * SEEK_CUR: The offset is set to its current location plus offset bytes.
     * SEEK_END: The offset is set to the size of the file plus offset bytes.
     */
    virtual bool Seek(int64_t offset, int whence);

private:

    int _fd;                                                                       //file descriptor

    /*
     * for mmap
     */
    char *_map_buf, *_read_ptr;           //map_buf is the start position of data mapped into memory
                                          //read_ptr point to the current read position

    int64_t _file_size;

    /*
     * for direct read
     */
    bool _direct_read;                                                   //true if we use direct I/O
    char *_direct_read_buf, *_copy_src_ptr; //direct_read_buf point to a buffer to store data read from disk drive
                                            //copy_src_ptr point to the head of data left in the buffer
    long int _block_size; //block size in the direct I/O, may change according to different linux kernel version
    long int _direct_buf_len;                       //size of the buffer to hold data read from disk
    int _left_data_size_in_buf; //size of data left in the buffer that haven't been copied to the user space

};

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_TMP_READER_LOCAL_H_ */
