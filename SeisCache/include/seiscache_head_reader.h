/*
 * seiscache_head_reader.h
 *
 *  Created on: Mar 23, 2018
 *      Author: wyd
 */

#ifndef SEISCACHE_HEAD_READER_H_
#define SEISCACHE_HEAD_READER_H_

#include "seiscache_readerbase.h"
#include "seisfile_headreader.h"


namespace seisfs {

namespace cache {

class HeadGather : public GatherBase {
public:

	bool Next(void *head);
	bool GetAll(void *head);
};

class HeadReader : public ReaderBase{
public:
	HeadReader(SharedPtr<file::HeadReader> f_head_reader, StoreType real_store_type, SharedPtr<MetaManager> meta_manager);
    virtual ~HeadReader();

    virtual bool SetHeadFilter(SharedPtr<HeadFilter> head_filter);

    virtual bool SetRowFilter(SharedPtr<RowFilter> row_filter);

    virtual int64_t GetTraceNum();

    virtual int64_t Tell();

    virtual bool Seek(uint64_t trace_idx);

    virtual bool HasNext();

	virtual bool Close();

    int GetHeadSize(); //return length of one head after filter, in bytes

    bool Get(uint64_t trace_idx, void *head);

    bool Next(void* head);

    HeadGather *NextGather();

    bool SeekToGather(int gather_idx);

private:
    SharedPtr<file::HeadReader> f_head_reader_;

};

}

}

#endif /* SEISCACHE_HEAD_READER_H_ */
