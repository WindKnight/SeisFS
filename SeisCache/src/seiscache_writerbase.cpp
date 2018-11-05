/*
 * seiscache_writerbase.cpp
 *
 *  Created on: May 28, 2018
 *      Author: wyd
 */

#include "seiscache_writerbase.h"
#include "seiscache_error.h"
#include "glock_queue.h"
#include <unistd.h>


namespace seisfs {

namespace cache {

WriterBase::WriterBase(const std::string &data_name, StoreType store_type, SharedPtr<MetaManager> meta_manager) :
		data_name_(data_name), store_type_(store_type), meta_manager_(meta_manager){

	is_closed_ = false;
	data_written_flag_ = false;
}

WriterBase::~WriterBase() {

}


bool WriterBase::BaseClose(int mod_type) {
	if(is_closed_)
		return true;

	if(!meta_manager_->SetLifetimeStat(store_type_, NORMAL)) {
		return false;
	}

	if(mod_type == MOD_NONE) {
		return true;
	} else {
		if(!meta_manager_->RecordLastModified()) {
			return false;
		}

		if(store_type_ == STABLE)
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

//		std::string strNode = "/";
//		strNode += data_name_;

		std::string real_task_name;
		bool in_queue = false;
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
bool WriterBase::Close() {
	if(is_closed_)
		return true;

	if(data_written_flag_) {
		//TODO  remove invalid index
#if 0
		if(!SeisbaseIndexIO::RemoveAll(attr_)) {
			seis_errno = SeisbaseCombineErrno(SEIS_ERR_DELINDEX, errno);
		}
#endif
	}
	if(!meta_manager_->SetLifetimeStat(store_type_, NORMAL)) {
		return false;
	}
	if(!meta_manager_->RecordLastModified()) {
		return false;
	}

	//数据回写注册到队列
	if(store_type_ == STABLE)
		return true;

#if 1
	if(copy_task_set_ == NULL) {
		task_set_mutex_.lock();
		if(copy_task_set_ == NULL) {
			copy_task_set_ = new std::set<std::string>();
		}
		task_set_mutex_.unlock();
	}
#endif

#if 1
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
	std::string strNode = "/";
#if 0
	std::string strUser;
	if(!getlogin())
		strUser = cuserid(0);
	else
	    strUser = getlogin();
	strNode += strUser + "/" + data_name_;
#endif
	strNode += data_name_;

//	q.push(strNode.c_str(),strNode.size());
#endif

#if 1
	if(copy_task_set_->end() == copy_task_set_->find(strNode)) {
		task_set_mutex_.lock();
		if(copy_task_set_->end() == copy_task_set_->find(strNode)) {
			copy_task_set_->insert(strNode);
			q.push(strNode.c_str(),strNode.size());
		}
		task_set_mutex_.unlock();
	}
#endif

	return true;

}
#endif

GPP::Mutex WriterBase::task_set_mutex_;
std::set<std::string> *WriterBase::copy_task_set_ = NULL;

}

}

