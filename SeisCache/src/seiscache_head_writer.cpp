/*
 * seiscache_head_writer.cpp
 *
 *  Created on: May 28, 2018
 *      Author: wyd
 */

#include "seiscache_head_writer.h"
#include "seiscache_error.h"


namespace seisfs {

namespace cache {

HeadWriter::HeadWriter(const std::string &data_name, SharedPtr<file::HeadWriter> f_head_writer, StoreType store_type, SharedPtr<MetaManager> meta_manager) :
		WriterBase(data_name, store_type, meta_manager), f_head_writer_(f_head_writer) {

}
HeadWriter::~HeadWriter() {
	if(!is_closed_) {
		Close();
	}
}

/**
 * Append a head
 */
bool HeadWriter::Write(const void* head) {
	ASSERT_CLOSED(false);

	if(!data_written_flag_) {
		data_written_flag_ = true;

		if(store_type_ == STABLE) {
			meta_manager_->SetLifetimeStat(STABLE, NORMAL); //The data has to be set to normal, because other processes may read it after the write
			meta_manager_->SetLifetimeStat(CACHE, INVALID);
		} else {
			meta_manager_->SetLifetimeStat(CACHE, NORMAL);
			meta_manager_->SetLifetimeStat(STABLE, INVALID);
		}
	}

	return f_head_writer_->Write(head);
}

bool HeadWriter::Truncate(uint64_t trace_id) {
	ASSERT_CLOSED(false);

	return f_head_writer_->Truncate(trace_id);
}

int64_t HeadWriter::Tell() {
	ASSERT_CLOSED(-1);

	return f_head_writer_->Pos();
}

bool HeadWriter::Sync() {
	ASSERT_CLOSED(false);

	return f_head_writer_->Sync();
}

bool HeadWriter::Close() {
	if(is_closed_) {
		return true;
	} else {

		if(!f_head_writer_->Close()) {
			sc_errno = SCCombineErrno(SC_ERR_CLOSE_FILE, errno);
			return false;
		}

		int mod_type = MOD_NONE;
		if(data_written_flag_) {
			mod_type = MOD_HEAD;
		}
		if(!BaseClose(mod_type))
			return false;

		if(!meta_manager_->ClearWriting()) {
			return false;
		}
		meta_manager_->UnlockWriteHead();
		is_closed_ = true;
		return true;
	}
}


}

}
