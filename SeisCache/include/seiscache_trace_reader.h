/*
 * seiscache_trace_reader.h
 *
 *  Created on: Mar 23, 2018
 *      Author: wyd
 */

#ifndef SEISCACHE_TRACE_READER_H_
#define SEISCACHE_TRACE_READER_H_

#include "seiscache_readerbase.h"
#include "seisfile_tracereader.h"

namespace seisfs {

namespace cache {


class TraceGather : public GatherBase {
public:
	TraceGather();
	~TraceGather();

	bool Next(void *trace);
	bool GetAll(void *trace);
};

class TraceReader : public ReaderBase{

public:
	TraceReader(SharedPtr<file::TraceReader> f_trace_reader, StoreType real_store_type, SharedPtr<MetaManager> meta_manager);
    virtual ~TraceReader();

    virtual bool SetRowFilter(SharedPtr<RowFilter> row_filter);

    virtual int64_t GetTraceNum();

    virtual int64_t Tell();

    virtual bool Seek(uint64_t trace_idx);

    virtual bool HasNext();

	virtual bool Close();

    int GetTraceSize();//return length of one trace, in bytes

    bool Get(uint64_t trace_idx, void* trace);

    bool Next(void* trace);

    TraceGather *NextGather();

    bool SeekToGather(int gather_id);

private:
    SharedPtr<file::TraceReader> f_trace_reader_;

};

}

}

#endif /* SEISCACHE_TRACE_READER_H_ */
