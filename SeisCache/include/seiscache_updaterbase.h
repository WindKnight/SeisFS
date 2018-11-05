/*
 * seiscache_updaterbase.h
 *
 *  Created on: Mar 23, 2018
 *      Author: wyd
 */

#ifndef SEISCACHE_UPDATERBASE_H_
#define SEISCACHE_UPDATERBASE_H_

#include <stdint.h>
#include "seisfs_config.h"
#include "util/seisfs_shared_pointer.h"
#include "seiscache_meta.h"
#include "seisfs_head_filter.h"
#include "seisfs_row_filter.h"

#include "seiscache_row_filter_by_key.h"
#include "seiscache_order.h"
#include "seisfile.h"

#define COMPACT_THRESHOLD	0.3

namespace seisfs {

namespace cache {

class UpdaterBase {
public:

	UpdaterBase(const std::string &data_name, SharedPtr<file::SeisFile> seis_file, StoreType real_store_type, SharedPtr<MetaManager> meta_manager);
    virtual ~UpdaterBase();

	virtual bool SetHeadFilter(SharedPtr<HeadFilter> head_filter);

    virtual bool SetRowFilter(SharedPtr<RowFilter> row_filter);

    /**
     * Move the read offset to head_idx(th) head.
     */
    virtual bool Seek(int64_t head_idx) = 0;

    /**
     * Transfers all modified in-core data of the file to the disk device where that file resides.
     */
    virtual bool Sync() = 0;

    /**
     *  close file.
     */
    bool BaseClose(int mod_type);

    virtual bool Close() = 0;

    bool SetRowFilter(SharedPtr<RowFilterByKey> row_filter_by_key);

    bool SetOrder(SharedPtr<Order> order);

protected:
	StoreType real_store_type_;
	SharedPtr<file::SeisFile> seis_file_;

	SharedPtr<HeadFilter> head_filter_;
	SharedPtr<RowFilter> row_filter_;
	SharedPtr<RowFilterByKey> row_filter_by_key_;
	SharedPtr<Order> order_;
	SharedPtr<MetaManager> meta_manager_;

	std::string data_name_;
	bool is_closed_;
	bool data_updated_flag_;

};

}

}


#endif /* SEISCACHE_UPDATERBASE_H_ */
