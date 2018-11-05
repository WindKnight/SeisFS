/*
 * gcache_seis_writer.h
 *
 *  Created on: Jul 7, 2016
 *      Author: zch
 */

#ifndef GCACHE_SEIS_WRITER_H_
#define GCACHE_SEIS_WRITER_H_

#include "gcache_global.h"
#include "util/seisfs_shared_pointer.h"
#include "gcache_seis_meta.h"
#include "gcache_seis_tracewriter.h"
#include "seisfile_headwriter.h"
#include "seisfile_writer.h"
#include <hdfs.h>

NAMESPACE_BEGIN_SEISFS
namespace file {
class SeisWriter: public Writer {
public:

    SeisWriter(hdfsFS fs, const std::string& hdfsDataName, SharedPtr<MetaSeisHDFS> meta,
            StorageSeis* storage_seis, std::string data_name);

    ~SeisWriter();

    /**
     * Append a trace head
     */
    bool Write(const void* head, const void* trace);

    /**
     * Returns the position that data is written to.
     */
    int64_t Pos();

    /**
     * Transfers all modified in-core data of the file to the disk device where that file resides.
     */
    bool Sync();

    /**
     * Close file.
     */
    bool Close();

    bool Truncate(int64_t heads);

    friend class HDFS;

private:

    bool Init();

    SeisTraceWriter* _traceWriter;
    HeadWriter* _headWriter;

    hdfsFS _fs;
    std::string _hdfs_data_name;
    SharedPtr<MetaSeisHDFS> _meta;

    StorageSeis* _storage_seis;
    std::string _data_name;

};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_WRITER_H_ */
