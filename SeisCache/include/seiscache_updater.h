/*
 * seiscache_updater.h
 *
 *  Created on: Mar 23, 2018
 *      Author: wyd
 */

#ifndef SEISCACHE_UPDATER_H_
#define SEISCACHE_UPDATER_H_

#include "seiscache_updaterbase.h"
#include "seisfile_headupdater.h"
#include "seisfile_traceupdater.h"

namespace seisfs {

namespace cache {

class Updater : public UpdaterBase{

public:
	Updater(const std::string &data_name, SharedPtr<file::HeadUpdater> f_head_updater, SharedPtr<file::TraceUpdater> f_trace_updater,
			SharedPtr<file::SeisFile> seis_file, StoreType real_store_type, SharedPtr<MetaManager> meta_manager);
    virtual ~Updater();

	virtual bool SetHeadFilter(SharedPtr<HeadFilter> head_filter);

    virtual bool SetRowFilter(SharedPtr<RowFilter> row_filter);

    virtual bool Seek(int64_t trace_idx);

    virtual bool Sync();

    virtual bool Close();

    bool Put(const void *head, const void* trace);

    bool Put(int64_t trace_idx, const void *head, const void* trace);

private:
    SharedPtr<file::TraceUpdater> f_trace_updater_;
    SharedPtr<file::HeadUpdater> f_head_updater_;

};

}

}

#endif /* SEISCACHE_UPDATER_H_ */
