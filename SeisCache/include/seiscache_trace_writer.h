/*
 * seiscache_trace_writer.h
 *
 *  Created on: Mar 23, 2018
 *      Author: wyd
 */

#ifndef SEISCACHE_TRACE_WRITER_H_
#define SEISCACHE_TRACE_WRITER_H_

#include "seiscache_writerbase.h"
#include "seisfile_tracewriter.h"


namespace seisfs {

namespace cache {

class TraceWriter : public WriterBase{
public:
	TraceWriter(const std::string &data_name, SharedPtr<file::TraceWriter> f_trace_writer, StoreType store_type, SharedPtr<MetaManager> meta_manager);
    virtual ~TraceWriter();

    /**
     * Append a trace
     */
    bool Write(const void* trace);

    virtual bool Truncate(uint64_t trace_id);

    virtual int64_t Tell();

    virtual bool Sync();

    virtual bool Close();

private:

    SharedPtr<file::TraceWriter> f_trace_writer_;

};

}

}

#endif /* SEISCACHE_TRACE_WRITER_H_ */
