/*
 * seiscache_trace_updater.cpp
 *
 *  Created on: May 31, 2018
 *      Author: wyd
 */


#include "seiscache_trace_updater.h"
#include "seiscache_error.h"

namespace seisfs {

namespace cache {

TraceUpdater::TraceUpdater(const std::string &data_name, SharedPtr<file::TraceUpdater> f_trace_updater, SharedPtr<file::SeisFile> seis_file,
		StoreType real_store_type, SharedPtr<MetaManager> meta_manager) :
		UpdaterBase(data_name, seis_file, real_store_type, meta_manager), f_trace_updater_(f_trace_updater) {

}
TraceUpdater::~TraceUpdater() {
	if(!is_closed_) {
		Close();
	}
}

bool TraceUpdater::SetRowFilter(SharedPtr<RowFilter> row_filter) {
	ASSERT_CLOSED(false);
	row_filter_ = row_filter;
	if(NULL != row_filter) {
		return f_trace_updater_->SetRowFilter(*row_filter_);
	} else {
		return true;
	}
}

bool TraceUpdater::Seek(int64_t trace_idx) {
	ASSERT_CLOSED(false);
	if(!f_trace_updater_->Seek(trace_idx)) {
		return false;
	}
	return true;
}

bool TraceUpdater::Sync() {
	ASSERT_CLOSED(false);
	if(!f_trace_updater_->Sync()) {
		return false;
	}
	return true;
}

bool TraceUpdater::Close() {
	if(is_closed_) {
		return true;
	} else {
		if(!f_trace_updater_->Close()) {
			sc_errno = SCCombineErrno(SC_ERR_CLOSE_FILE, errno);
			return false;
		}

/*		if(data_updated_flag_) {
#if 0
			if(NULL != head_filter_) {

				const std::vector<int> key_arr = head_filter_->GetKeys();

				for(int i = 0; i < key_arr.size(); ++i) {
					if(!SeisbaseIndexIO::Remove(attr_, key_vec[i]))
						seis_errno = SeisbaseCombineErrno(SEIS_ERR_DELINDEX, errno);
				}
			} else {
				if(!SeisbaseIndexIO::RemoveAll(attr_))
					seis_errno = SeisbaseCombineErrno(SEIS_ERR_DELINDEX, errno);
			}
#endif
			//TODO remove related index
		}*/

		int mod_type = MOD_NONE;
		if(data_updated_flag_) {
			mod_type = MOD_TRACE;
		}

		if(!BaseClose(mod_type)) {
			return false;
		}

		if(!meta_manager_->ClearUpdating()) {
			return false;
		}

		is_closed_ = true;
		return true;
	}
}

bool TraceUpdater::Put(const void* trace) {
	ASSERT_CLOSED(false);
	if(!f_trace_updater_->Put(trace)) {
		return false;
	}

	if(!data_updated_flag_) {
		data_updated_flag_ = true;

		if(real_store_type_ == STABLE) {
			meta_manager_->SetLifetimeStat(CACHE, INVALID);
		} else {
			meta_manager_->SetLifetimeStat(STABLE, INVALID);
		}
	}

	return true;
}

bool TraceUpdater::Put(int64_t trace_idx, const void* trace) {
	ASSERT_CLOSED(false);

	if(!f_trace_updater_->Put(trace_idx, trace)) {
		return false;
	}

	if(!data_updated_flag_) {
		data_updated_flag_ = true;

		if(real_store_type_ == STABLE) {
			meta_manager_->SetLifetimeStat(CACHE, INVALID);
		} else {
			meta_manager_->SetLifetimeStat(STABLE, INVALID);
		}
	}

	return true;
}

}

}

