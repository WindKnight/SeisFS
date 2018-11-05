/*
 * seiscache_writer.h
 *
 *  Created on: Mar 23, 2018
 *      Author: wyd
 */

#ifndef SEISCACHE_WRITER_H_
#define SEISCACHE_WRITER_H_

#include "seiscache_writerbase.h"
#include "seisfile_writer.h"

namespace seisfs {

namespace cache {

class Writer : public WriterBase{
public:
	Writer(const std::string &data_name, SharedPtr<file::Writer> f_writer, StoreType store_type, SharedPtr<MetaManager> meta_manager);
    virtual ~Writer();

    /**
     * Append a trace
     */
    bool Write(const void* head,const void* trace);

    virtual bool Truncate(uint64_t trace_id);

    virtual int64_t Tell();

    virtual bool Sync();

    virtual bool Close();

private:

    SharedPtr<file::Writer> f_writer_;

};

}

}



#endif /* SEISCACHE_WRITER_H_ */
