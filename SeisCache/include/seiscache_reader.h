/*
 * seiscache_reader.h
 *
 *  Created on: Mar 22, 2018
 *      Author: wyd
 */

#ifndef SEISCACHE_READER_H_
#define SEISCACHE_READER_H_

#include <stdint.h>
#include "seiscache_readerbase.h"
#include "seisfile_reader.h"

namespace seisfs {

namespace cache {

class Gather : public GatherBase {
public:
	Gather();
	~Gather();

	bool Next(void *head, void *trace);
	bool GetAll(void *head, void *trace);
	char *GetGatherKey(); //

};


class Reader : public ReaderBase{
public:
	Reader(SharedPtr<file::Reader> f_reader, StoreType real_store_type, SharedPtr<MetaManager> meta_manager);
    virtual ~Reader();

    virtual bool SetHeadFilter(SharedPtr<HeadFilter> head_filter);

    virtual bool SetRowFilter(SharedPtr<RowFilter> row_filter);

    virtual int64_t GetTraceNum();

    virtual int64_t Tell();

    virtual bool Seek(uint64_t trace_idx);

    virtual bool HasNext();

	virtual bool Close();


    int GetHeadSize();//return length of one head after filter, in bytes

    int GetTraceSize();//return length of one trace, in bytes

    /*
     * Get does not influence the cursor of Next
     */
    bool Get(uint64_t trace_idx, void *head, void* trace);

    bool Next(void* head, void* trace);

    /*
     * get next trace gather defined by order.
     */
    Gather *NextGather();

    bool SeekToGather(int gather_id);

private:

    SharedPtr<file::Reader> f_reader_;

};

}

}


#endif /* SEISCACHE_READER_H_ */
