/*
 * gcache_seis_headwriter_row.h
 *
 *  Created on: Jul 8, 2016
 *      Author: zch
 */

#ifndef GCACHE_SEIS_HEADWRITER_ROW_H_
#define GCACHE_SEIS_HEADWRITER_ROW_H_

#include "util/seisfs_shared_pointer.h"
#include "gcache_global.h"
#include "gcache_seis_meta.h"
#include "seisfile_headwriter.h"
#include "gcache_seis_storage.h"
#include "gcache_seis_truncater.h"
#include <hdfs.h>
#include <string>
#include "gcache_seis_error.h"
#include "gcache_io.h"
#include "gcache_string.h"
#include "gcache_hdfs_utils.h"
#include "gcache_log.h"
#include "gcache_seis_headupdater_row.h"
#include "gcache_seis_utils.h"
#include <string.h>

NAMESPACE_BEGIN_SEISFS
namespace file {
class SeisHeadWriterRow: public HeadWriter {
public:

    /**
     * Destructor.
     */
    virtual ~SeisHeadWriterRow();

    /**
     * Append a head
     */
    virtual bool Write(const void* head);

    /**
     * Returns the position that data is written to.
     */
    virtual int64_t Pos();

    /**
     * Transfers all modified in-core data of the file to the disk device where that file resides.
     */
    virtual bool Sync();

    /**
     *  Close file.
     */
    virtual bool Close();

    virtual bool Truncate(int64_t heads);

    friend class HDFS;

    friend class SeisWriter;

private:

    /**
     *  Constructor.
     */
    SeisHeadWriterRow(hdfsFS fs, const std::string& hdfs_data_name, SharedPtr<MetaSeisHDFS> meta,
            StorageSeis* storage_seis, std::string data_name);

    /**
     *  Preliminary work for write data.
     */
    bool Init();

    std::string _hdfs_data_head_name; //the full name of current file to be written
    std::string _hdfs_data_name;                    //the full name of seis
    hdfsFS _fs; //the configured filesystem handle.
    hdfsFile _head_fd;           //the file handle of current file to be written
    SharedPtr<MetaSeisHDFS> _meta;                      //the meta of seis
    std::string _head_write_dir;       //the full name of current head directory
    int64_t _head_current_row;     //the write row of current file to be written
    bool _is_close_flag; //the flag indicate whether this object is closed or not

    StorageSeis* _storage_seis;
    std::string _data_name;
};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_HEADWRITER_ROW_H_ */
