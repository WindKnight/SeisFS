/*
 * seiscache_head_updater.cpp
 *
 *  Created on: May 31, 2018
 *      Author: wyd
 */


#include "seiscache_head_updater.h"
#include "seiscache_error.h"

namespace seisfs {

namespace cache {

HeadUpdater::HeadUpdater(const std::string &data_name, SharedPtr<file::HeadUpdater> f_head_updater, SharedPtr<file::SeisFile> seis_file,
		StoreType real_store_type, SharedPtr<MetaManager> meta_manager) :
		UpdaterBase(data_name, seis_file, real_store_type, meta_manager), f_head_updater_(f_head_updater) {

}
HeadUpdater::~HeadUpdater() {
	if(!is_closed_) {
		Close();
	}
}

bool HeadUpdater::SetHeadFilter(SharedPtr<HeadFilter> head_filter) {
	ASSERT_CLOSED(false);
	head_filter_ = head_filter;
	if(NULL != head_filter_) {
		return f_head_updater_->SetHeadFilter(*head_filter_);
	} else {
		return true;
	}
}

bool HeadUpdater::SetRowFilter(SharedPtr<RowFilter> row_filter) {
	ASSERT_CLOSED(false);
	row_filter_ = row_filter;
	if(NULL != row_filter) {
		return f_head_updater_->SetRowFilter(*row_filter_);
	} else {
		return true;
	}
}

bool HeadUpdater::Seek(int64_t trace_idx) {
	ASSERT_CLOSED(false);
	if(!f_head_updater_->Seek(trace_idx)) {
		return false;
	}
	return true;
}

bool HeadUpdater::Sync() {
	ASSERT_CLOSED(false);
	if(!f_head_updater_->Sync()) {
		return false;
	}
	return true;
}

bool HeadUpdater::Close() {
	if(is_closed_) {
		return true;
	} else {

		if(!f_head_updater_->Close()) {
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
			mod_type = MOD_HEAD;
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

bool HeadUpdater::Put(const void *head) {
	ASSERT_CLOSED(false);
	if(!f_head_updater_->Put(head)) {
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

bool HeadUpdater::Put(int64_t trace_idx, const void *head) {
	ASSERT_CLOSED(false);

	if(!f_head_updater_->Put(trace_idx, head)) {
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

