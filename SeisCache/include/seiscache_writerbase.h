/*
 * seiscache_writerbase.h
 *
 *  Created on: Mar 23, 2018
 *      Author: wyd
 */

#ifndef SEISCACHE_WRITERBASE_H_
#define SEISCACHE_WRITERBASE_H_

#include <stdint.h>
#include "seisfs_config.h"
#include "util/seisfs_shared_pointer.h"
#include "seiscache_meta.h"
#include <gpp.h>



namespace seisfs {

namespace cache {

class WriterBase {
public:

	WriterBase(const std::string &data_name, StoreType store_type, SharedPtr<MetaManager> meta_manager);

    virtual ~WriterBase();


    virtual bool Truncate(uint64_t trace_id) = 0;
    /**
     * Returns the position that data is written to.
     */
    virtual int64_t Tell() = 0;

    /**
     * Transfers all modified in-core data of the file to the disk device where that file resides.
     */
    virtual bool Sync() = 0;

    /**
     * Close file.
     */
    virtual bool Close() = 0;

    bool BaseClose(int mod_type);

protected:
    std::string data_name_;
    StoreType store_type_;
    SharedPtr<MetaManager> meta_manager_;

    bool is_closed_;
    bool data_written_flag_;

private:
	static GPP::Mutex task_set_mutex_;
	static std::set<std::string> *copy_task_set_;
};

}

}

#endif /* SEISCACHE_WRITERBASE_H_ */
