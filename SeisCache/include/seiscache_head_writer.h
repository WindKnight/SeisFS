/*
 * seiscache_head_writer.h
 *
 *  Created on: Mar 23, 2018
 *      Author: wyd
 */

#ifndef SEISCACHE_HEAD_WRITER_H_
#define SEISCACHE_HEAD_WRITER_H_

#include "seiscache_writerbase.h"
#include "seisfile_headwriter.h"

namespace seisfs {

namespace cache {

class HeadWriter : public WriterBase{
public:
	HeadWriter(const std::string &data_name, SharedPtr<file::HeadWriter> f_head_writer, StoreType store_type, SharedPtr<MetaManager> meta_manager);
    virtual ~HeadWriter();

    /**
     * Append a head
     */
    bool Write(const void* head);

    virtual bool Truncate(uint64_t trace_id);

    virtual int64_t Tell();

    virtual bool Sync();

    virtual bool Close();

private:

    SharedPtr<file::HeadWriter> f_head_writer_;


};

}

}


#endif /* SEISCACHE_HEAD_WRITER_H_ */
