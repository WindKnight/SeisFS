/*
 * seiscache_updaterbase.cpp
 *
 *  Created on: May 31, 2018
 *      Author: wyd
 */

#include "seiscache_updaterbase.h"
#include "seiscache_error.h"
#ifdef USE_GCACHE
#include "gcache_seis_data.h"
#else
#include "seisfile_hdfs.h"
#endif
#include "glock_queue.h"

namespace seisfs {

namespace cache {

UpdaterBase::UpdaterBase(const std::string &data_name, SharedPtr<file::SeisFile> seis_file, StoreType real_store_type, SharedPtr<MetaManager> meta_manager) :
		data_name_(data_name), seis_file_(seis_file), real_store_type_(real_store_type), meta_manager_(meta_manager){

	head_filter_ = NULL;
	row_filter_ = NULL;
	row_filter_by_key_ = NULL;
	order_ = NULL;

	data_updated_flag_ = false;
	is_closed_ = false;
}
UpdaterBase::~UpdaterBase() {

}

bool UpdaterBase::SetHeadFilter(SharedPtr<HeadFilter> head_filter) {
	return false;
}

bool UpdaterBase::SetRowFilter(SharedPtr<RowFilter> row_filter) {
	return false;
}

bool UpdaterBase::SetRowFilter(SharedPtr<RowFilterByKey> row_filter_by_key) {
	row_filter_by_key_ = row_filter_by_key;
	return true;
}

bool UpdaterBase::SetOrder(SharedPtr<Order> order) {
	order_ = order;
	return true;
}


bool UpdaterBase::BaseClose(int mod_type) {

	if(is_closed_)
		return true;

#ifdef USE_GCACHE
	std::string sf_class_name = seis_file_->GetClassName();
	if(sf_class_name == HDFS_CLASS_NAME) {
		file::HDFS *sf_hdfs = (file::HDFS*)seis_file_.get();
		int64_t trace_num = sf_hdfs->GetTraceNum();
		int64_t updated_trace_num = sf_hdfs->GetUpdatedTraceRows();
		int64_t updated_head_num = sf_hdfs->GetUpdatedHeadRows();

		double threshold = COMPACT_THRESHOLD * trace_num;

		if((double)updated_trace_num > threshold || (double)updated_head_num > threshold) {
			if(!sf_hdfs->Compact()) {
				sc_errno = SCCombineErrno(SC_ERR_COMPACT, errno);
				return false;
			}
		}
	}
#endif

	if(!meta_manager_->SetLifetimeStat(real_store_type_, NORMAL)) {
		return false;
	}

	if(mod_type == MOD_NONE) {
		return true;
	} else {
		if(!meta_manager_->RecordLastModified()) {
			return false;
		}

		if(real_store_type_ == STABLE)
			return true;

#if 0
		if(copy_task_set_ == NULL) {
			task_set_mutex_.lock();
			if(copy_task_set_ == NULL) {
				copy_task_set_ = new std::set<std::string>();
			}
			task_set_mutex_.unlock();
		}
#endif

		std::string strQueue;

		char *queue_name = getenv("WBQ_NAME");
		if(queue_name != NULL) {
			strQueue = "/";
			strQueue += queue_name;
		} else {
			sc_errno = SC_ERR_REG_QUEUE;
			return false;
		}

		std::string strKey;
		GCacheQueue q(strKey);
		if(q.setProperty(strQueue)!=0)
		{
			sc_errno = SC_ERR_REG_QUEUE;
			return false;
		}

		std::string real_task_name;
		bool in_queue;

		if(mod_type & MOD_HEAD) {

			//TODO(remove all related index)
#if 0
			if(!SeisbaseIndexIO::RemoveAll(attr_)) {
				seis_errno = SeisbaseCombineErrno(SEIS_ERR_DELINDEX, errno);
			}
#endif
			real_task_name = data_name_ + "H";

			if(!meta_manager_->GetWriteBackFlag(MOD_HEAD, in_queue)) {
				return false;
			}
			if(in_queue) {
				return true;
			} else {
				q.push(real_task_name.c_str(), real_task_name.size());
				if(!meta_manager_->SetWriteBackFlag(MOD_HEAD)) {
					return false;
				}
			}
		}
		if(mod_type & MOD_TRACE) {
			real_task_name = data_name_ + "T";

			if(!meta_manager_->GetWriteBackFlag(MOD_TRACE, in_queue)) {
				return false;
			}
			if(in_queue) {
				return true;
			} else {
				q.push(real_task_name.c_str(), real_task_name.size());
				if(!meta_manager_->SetWriteBackFlag(MOD_TRACE)) {
					return false;
				}
			}
		}
	}

	return true;
}

#if 0
bool UpdaterBase::Close() {
	std::string sf_class_name = seis_file_->GetClassName();
#ifdef USE_GCACHE
	if(sf_class_name == HDFS_CLASS_NAME) {

		file::HDFS *sf_hdfs = (file::HDFS*)seis_file_.get();
		int64_t trace_num = sf_hdfs->GetTraceNum();
		int64_t updated_trace_num = sf_hdfs->GetUpdatedTraceRows();
		int64_t updated_head_num = sf_hdfs->GetUpdatedHeadRows();

		double threshold = COMPACT_THRESHOLD * trace_num;

		if((double)updated_trace_num > threshold || (double)updated_head_num > threshold) {
			if(!sf_hdfs->Compact()) {
				sc_errno = SCCombineErrno(SC_ERR_COMPACT, errno);
				return false;
			}
		}
	}
#endif

	if(!meta_manager_->ClearUpdating()) {
		return false;
	}

	if(!meta_manager_->RecordLastModified()) {
		return false;
	}
	return true;

	//close updater
	//record meta data
	//delete invalid index
}
#endif

}

}


