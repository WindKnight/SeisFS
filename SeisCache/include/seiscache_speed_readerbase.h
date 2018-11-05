/*
 * seiscache_speed_readerbase.h
 *
 *  Created on: Jun 27, 2018
 *      Author: wyd
 */

#ifndef SEISCACHE_SPEED_READERBASE_H_
#define SEISCACHE_SPEED_READERBASE_H_

#include "seisfile_speed_reader.h"
#include "seisfs_head_filter.h"
#include "seisfs_row_filter.h"

#include "seiscache_row_filter_by_key.h"
#include "seiscache_order.h"
#include "seiscache_meta.h"


#define DISFETCH_THRD_NUM		2
#define DISFETCH_TASK_GRAIN		100  //the number of traces in one task that scheduled to a worker
#define DISFETCH_REBALANCE		1

namespace seisfs {
namespace cache {

class SpeedReaderBase {

public:
	SpeedReaderBase(const std::string data_name, StoreType real_store_type, SharedPtr<MetaManager> meta_manager);
	virtual ~SpeedReaderBase();

	bool Init();

	int64_t GetTraceNum(); //return trace num after filter.
    int GetHeadSize();//return length of one head after filter, in bytes
    int GetTraceSize();//return length of one trace, in bytes

	bool SetHeadFilter(SharedPtr<HeadFilter> head_filter);
	bool SetRowFilter(SharedPtr<RowFilter> row_filter);

	bool SetRowFilter(SharedPtr<RowFilterByKey> row_filter_by_key);
	bool SetOrder(SharedPtr<Order> order);

	virtual bool Close();

protected:
	std::string data_name_;
	StoreType real_store_type_;
	SharedPtr<MetaManager> meta_manager_;
	file::SpeedReader *f_spd_reader_;
	int head_size_,trace_size_;
	int64_t trace_num_;

	SharedPtr<HeadFilter> head_filter_;
	SharedPtr<RowFilter> row_filter_;
	std::vector<int64_t> gather_;

	bool is_closed_;
};


}
}

#endif /* SEISCACHE_SPEED_READERBASE_H_ */
