/*
 * seisfile_hdfs_headwriter.h
 *
 *  Created on: Mar 26, 2018
 *      Author: cssl
 */

#ifndef SEISFILE_HDFS_HEADWRITER_H_
#define SEISFILE_HDFS_HEADWRITER_H_

#include <stdint.h>
#include "seisfile_hdfs_meta.h"
#include "seisfile_headwriter.h"

namespace seisfs {

namespace file {

class HeadWriterHDFS : public HeadWriter {
public:
    virtual ~HeadWriterHDFS();

    //HeadWriterHDFS(const HeadType &head_type,const std::string& filename, Lifetime lifetime_days);
    HeadWriterHDFS(const std::string& filename, MetaHDFS *meta);

    bool Init();

    /**
     * Append a head
     */
    bool Write(const void* head);

    /**
     * Returns the position that data is written to.
     */
    int64_t Pos();

    /**
     * Transfers all modified in-core data of the file to the disk device where that file resides.
     */
    bool Sync();

    /**
     *  Close file.
     */
    bool Close();

    bool Truncate(int64_t trace_num);

private:
    /*bool Init();

    HeadType* _head;
    GEFile _gf;
    std::string _headfilename;
    off_t _ptr;*/
    uint32_t _head_length;
    std::string _head_filename;
    std::string _meta_filename;
    std::string _filename;
    std::string seisnas_home_path_;
//    GEFile *_gf_head;
    int fd_head_;
    HeadType _head_type;
    Lifetime _lifetime_days;
    MetaHDFS *_meta;

};

}

}

#endif /* SEISFILE_HDFS_HEADWRITER_H_ */
