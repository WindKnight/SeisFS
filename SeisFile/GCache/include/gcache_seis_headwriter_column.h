/*
 * gcache_seis_headwriter_column.h
 *
 *  Created on: Jul 8, 2016
 *      Author: zch
 */

#ifndef GCACHE_SEIS_HEADWRITER_COLUMN_H_
#define GCACHE_SEIS_HEADWRITER_COLUMN_H_

#include <string.h>
#include <hdfs.h>
#include <pthread.h>
#include "gcache_seis_error.h"
#include "gcache_io.h"
#include "gcache_log.h"
#include "gcache_seis_utils.h"
#include "gcache_seis_headupdater_column.h"
#include "util/seisfs_shared_pointer.h"
#include "gcache_global.h"
#include "gcache_seis_meta.h"
#include "gcache_seis_storage.h"
#include "gcache_seis_truncater.h"
#include "seisfile_headwriter.h"

NAMESPACE_BEGIN_SEISFS
namespace file {
class SeisHeadWriterColumn: public HeadWriter {
public:

    virtual ~SeisHeadWriterColumn();

    /**
     * Append a trace head
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

    virtual bool Close();

    virtual bool Truncate(int64_t heads);

    friend class HDFS;

    friend class SeisWriter;

private:

    SeisHeadWriterColumn(hdfsFS fs, const std::string& hdfs_data_name, SharedPtr<MetaSeisHDFS> meta,
            StorageSeis* storage_seis, std::string data_name);

    bool Init();

    static void* Flush(void* tmp); //Process to write Cache File into DataFile

    std::string _hdfs_data_name;

    std::string _head_write_dir;       //the full name of current head directory
    bool _is_close_flag;
    hdfsFile _head_cache_fd;
    std::string _head_cache_dir;
    std::string _hdfs_head_cache_name;
    int64_t _cnt; // cache count;
    pthread_t _pid;
    // 10/25
    char* _column_cache1;
//    char* _cur_cache;
    bool _first_flush;

    StorageSeis* _storage_seis;
    std::string _data_name;

    //flush using
    SharedPtr<MetaSeisHDFS> _meta;
    int64_t _head_current_row;     //the write row of current file to be written
    hdfsFile _head_fd;
    hdfsFS _fs;
    std::string _hdfs_data_head_name;
    bool _thread_false;
    char* _column_cache2;
    bool _cache_flag;
    pthread_mutex_t _thread_mutex;
};

NAMESPACE_END_SEISFS
}
#endif /* GCACHE_SEIS_HEADWRITER_COLUMN_H_ */
