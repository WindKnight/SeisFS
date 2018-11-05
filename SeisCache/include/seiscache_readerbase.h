/*
 * seiscache_readerbase.h
 *
 *  Created on: Mar 23, 2018
 *      Author: wyd
 */

#ifndef SEISCACHE_READERBASE_H_
#define SEISCACHE_READERBASE_H_

#include "util/seisfs_shared_pointer.h"
#include "seisfs_config.h"
#include "seisfs_head_filter.h"
#include "seisfs_row_filter.h"

#include "seiscache_row_filter_by_key.h"
#include "seiscache_order.h"
#include "seiscache_meta.h"


namespace seisfs {

namespace cache {


class GatherBase {
public:

	GatherBase ();

	virtual ~GatherBase();

	int64_t GetTraceNum();

	bool HasNext();
};


class ReaderBase {
public:
	ReaderBase (StoreType real_store_type, SharedPtr<MetaManager> meta_manager);
	virtual ~ReaderBase();

	virtual bool SetHeadFilter(SharedPtr<HeadFilter> head_filter);

    virtual bool SetRowFilter(SharedPtr<RowFilter> row_filter);

    virtual int64_t GetTraceNum() = 0;

	/*
	 * return the current read trace_idx
	 */
    virtual int64_t Tell() = 0;
	/**
	 * Move the read offset to trace_idx(th) trace.
	 */
	virtual bool Seek(uint64_t trace_idx) = 0;
	/**
	 * Return true if there are more traces. The routine usually accompany with NextTrace
	 */
	virtual bool HasNext() = 0;

	virtual bool Close();


    bool SetRowFilter(SharedPtr<RowFilterByKey> row_filter_by_key); //TODO

    bool SetOrder(SharedPtr<Order> order);

protected:

	StoreType real_store_type_;
	SharedPtr<HeadFilter> head_filter_;
	SharedPtr<RowFilter> row_filter_;
	SharedPtr<RowFilterByKey> row_filter_by_key_;
	SharedPtr<Order> order_;
	SharedPtr<MetaManager> meta_manager_;

	bool is_closed_;
};

}

}


#endif /* SEISCACHE_READERBASE_H_ */
