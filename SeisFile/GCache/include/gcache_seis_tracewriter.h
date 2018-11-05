/*
 * gcache_seis_tracewriter.h
 *
 *  Created on: Jul 8, 2016
 *      Author: zch
 */

#ifndef GCACHE_SEIS_TRACEWRITER_H_
#define GCACHE_SEIS_TRACEWRITER_H_

#include "util/seisfs_shared_pointer.h"
#include "gcache_global.h"
#include "gcache_seis_meta.h"
#include "gcache_seis_truncater.h"
#include "gcache_seis_storage.h"
#include "seisfile_tracewriter.h"
#include <hdfs.h>
#include "gcache_log.h"
#include "gcache_seis_utils.h"
#include "gcache_seis_traceupdater.h"
#include <string.h>
#include "gcache_seis_error.h"
#include "gcache_io.h"
#include "gcache_string.h"
#include "gcache_hdfs_utils.h"
NAMESPACE_BEGIN_SEISFS
namespace file {
class SeisTraceWriter: public TraceWriter {
public:

    /**
     * Destructor.
     */
    ~SeisTraceWriter();

    /**
     * Append a trace.
     */
    bool Write(const void* trace);

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

    bool Truncate(int64_t traces);

    friend class HDFS;

    friend class SeisWriter;

private:

    /**
     *  Constructor.
     */
    SeisTraceWriter(hdfsFS fs, const std::string& hdfs_data_name, SharedPtr<MetaSeisHDFS> meta,
            StorageSeis* storage_seis, std::string data_name);

    /**
     *  Preliminary work for write data.
     */
    bool Init();

    std::string _hdfs_data_name;                     //the full name of seis
    std::string _hdfs_data_trace_name; //the full name of current file to be written
    hdfsFS _fs; //the configured filesystem handle.
    hdfsFile _trace_fd;          //the file handle of current file to be written
    SharedPtr<MetaSeisHDFS> _meta;                       //the meta of seis
    std::string _trace_write_dir;     //the full name of current trace directory
    int64_t _trace_current_row;    //the write row of current file to be written
    bool _is_close_flag; //the flag indicate whether this object is closed or not

    StorageSeis* _storage_seis;
    std::string _data_name;

};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_TRACEWRITER_H_ */
