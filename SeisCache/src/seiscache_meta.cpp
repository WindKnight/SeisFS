/*
 * seiscache_meta.cpp
 *
 *  Created on: May 28, 2018
 *      Author: wyd
 */

#include "seiscache_meta.h"
#include "seiscache_error.h"
#include "util/seisfs_scoped_pointer.h"
#include "seisfile.h"
#include "util/seisfs_util_log.h"


namespace seisfs {

namespace cache {


MetaManager::MetaManager(const std::string &data_name) :
		data_name_(data_name){

}
MetaManager::~MetaManager() {

}

bool MetaManager::InitAllMeta() {
	Remove();
	ASSERT(ClearWriting(), false);
	ASSERT(ClearAllUpdating(), false);
	ASSERT(ClearWriteBackFlag(MOD_HEAD), false);
	ASSERT(ClearWriteBackFlag(MOD_TRACE), false);
	ASSERT(RecordLastAccessed(), false);
	ASSERT(RecordLastModified(), false);

	return true;
}


std::string MetaManager::GetVersionMetaName() {
	return data_name_ + "/version";
}
std::string MetaManager::GetWritingMetaName() {
	return data_name_ + "/writing";
}
std::string MetaManager::GetUpdatingMetaName() {
	return data_name_ + "/updating";
}
std::string MetaManager::GetLastAccMetaName() {
	return data_name_ + "/last_acc";
}
std::string MetaManager::GetLastModMetaName() {
	return data_name_ + "/last_mod";
}
std::string MetaManager::GetHeadTypeMetaName() {
	return data_name_ + "/head_type";
}
std::string MetaManager::GetHeadTypeNumMetaName() {
	return data_name_ + "/head_type_num";
}
std::string MetaManager::GetTraceTypeMetaName() {
	return data_name_ + "/trace_type";
}

std::string MetaManager::GetWriteBackFlagName() {
	return data_name_ + "/write_back_flag";
}

std::string MetaManager::GetWriteMutexHeadName() {
	return data_name_ + "/write_mutex_head";
}
std::string MetaManager::GetWriteMutexTraceName() {
	return data_name_ + "/write_mutex_trace";
}
std::string MetaManager::GetUpdateMetaMutexName() {
	return data_name_ + "/update_meta_mutex";
}


bool IsValidStat(LifetimeStat ltstat) {
	if(ltstat == NORMAL || ltstat == MOVE_TO_STABLE) {
		return true;
	} else if(ltstat == EXPIRED || ltstat == INVALID) {
		return false;
	} else {
		return false;
	}
}


bool MetaManager::IsValid(StoreType store_type) {
	ScopedPointer<file::SeisFile> file(file::SeisFile::New(data_name_, store_type));
	ASSERT_NEW(file.data(), false);
	if(!file->Init()) {
		sc_errno = SCCombineErrno(SC_ERR_FILE_INIT, errno);
		return false;
	}
	if(!file->Exists()) {
		return false;
	}
	LifetimeStat ltstat = file->GetLifetimeStat();
	if(IsValidStat(ltstat)) {
		return true;
	} else {
		return false;
	}

}

bool MetaManager::SetLifetimeStat(StoreType store_type, LifetimeStat stat) {
	ScopedPointer<file::SeisFile> file(file::SeisFile::New(data_name_, store_type));
	ASSERT_NEW(file.data(), false);

	if(!file->Init()) {
		sc_errno = SCCombineErrno(SC_ERR_FILE_INIT, errno);
		return false;
	}

	if(!file->Exists()) {
		return true;
	}
	file->SetLifetimeStat(stat);
	return true;
}

bool MetaManager::PutMeta(SeisMeta *seis_meta, char *buf, int size) {
	if(!seis_meta->write(buf, size)) {   //TODO write from where? always beginning?
		sc_errno = SCCombineErrno(SC_ERR_PUT_META, errno);
		return false;
	}
	return true;
}
bool MetaManager::GetMeta(SeisMeta *seis_meta, char *buf, int size) {
	int iret1, iret2;
	iret1 = seis_meta->read(buf, size, iret2);
	if(0 > iret1) {
		sc_errno = SCCombineErrno(SC_ERR_GET_META, errno);
		return false;
	}
	return true;
}


bool MetaManager::SetWriteBackFlag(int mod_type) {
	std::string wbf_meta_name = GetWriteBackFlagName();
	if(mod_type & MOD_HEAD) {
		wbf_meta_name += "H";
	} else if(mod_type & MOD_TRACE) {
		wbf_meta_name += "T";
	} else {
		return false;
	}
	ScopedPointer<SeisMeta> seis_meta(new SeisMeta(wbf_meta_name));
	ASSERT_NEW(seis_meta.data(), false);

	char write_back_flag = 1;
	if(!PutMeta(seis_meta.data(), (char*)&write_back_flag, sizeof(write_back_flag))) {
		return false;
	}
	return true;
}
bool MetaManager::ClearWriteBackFlag(int mod_type) {
	std::string wbf_meta_name = GetWriteBackFlagName();
	if(mod_type & MOD_HEAD) {
		wbf_meta_name += "H";
	} else if(mod_type & MOD_TRACE) {
		wbf_meta_name += "T";
	} else {
		return false;
	}
	ScopedPointer<SeisMeta> seis_meta(new SeisMeta(wbf_meta_name));
	ASSERT_NEW(seis_meta.data(), false);

	char write_back_flag = 0;
	if(!PutMeta(seis_meta.data(), (char*)&write_back_flag, sizeof(write_back_flag))) {
		return false;
	}
	return true;
}
bool MetaManager::GetWriteBackFlag(int mod_type, bool &in_queue) {
	std::string wbf_meta_name = GetWriteBackFlagName();
	if(mod_type & MOD_HEAD) {
		wbf_meta_name += "H";
	} else if(mod_type & MOD_TRACE) {
		wbf_meta_name += "T";
	} else {
		return false;
	}
	ScopedPointer<SeisMeta> seis_meta(new SeisMeta(wbf_meta_name));
	ASSERT_NEW(seis_meta.data(), false);

	char write_back_flag;

	if(!GetMeta(seis_meta.data(), (char*)&write_back_flag, sizeof(write_back_flag))) {
		return false;
	}
	printf("write_back_flag = %d\n", write_back_flag);
	in_queue = write_back_flag ? true : false;
	return true;
}


bool MetaManager::SetWriting() {
	std::string w_meta_name = GetWritingMetaName();
	ScopedPointer<SeisMeta> seis_meta(new SeisMeta(w_meta_name));
	ASSERT_NEW(seis_meta.data(), false);

	int wrting_num = 1;
	if(!PutMeta(seis_meta.data(), (char*)&wrting_num, sizeof(wrting_num))) {
		return false;
	}
	return true;
}
bool MetaManager::ClearWriting() {
	std::string w_meta_name = GetWritingMetaName();
	ScopedPointer<SeisMeta> seis_meta(new SeisMeta(w_meta_name));
	ASSERT_NEW(seis_meta.data(), false);

	int wrting_num = 0;
	if(!PutMeta(seis_meta.data(), (char*)&wrting_num, sizeof(wrting_num))) {
		return false;
	}
	return true;
}
bool MetaManager::GetWritingNum(int &wrting_num) {
	std::string w_meta_name = GetWritingMetaName();
	ScopedPointer<SeisMeta> seis_meta(new SeisMeta(w_meta_name));
	ASSERT_NEW(seis_meta.data(), false);

	if(!GetMeta(seis_meta.data(), (char*)&wrting_num, sizeof(wrting_num))) {
		return false;
	}
	return true;
}

bool MetaManager::SetUpdating() {
	std::string update_meta_name = GetUpdatingMetaName();
	ScopedPointer<SeisMeta> seis_meta(new SeisMeta(update_meta_name));
	ASSERT_NEW(seis_meta.data(), false);

	int updating_num;

	if(!GetMeta(seis_meta.data(), (char*)&updating_num, sizeof(updating_num))) {
		return false;
	}

	updating_num ++;
	if(!PutMeta(seis_meta.data(), (char*)&updating_num, sizeof(updating_num))) {
		return false;
	}
	return true;
}
bool MetaManager::ClearUpdating() {
	std::string update_meta_name = GetUpdatingMetaName();
	ScopedPointer<SeisMeta> seis_meta(new SeisMeta(update_meta_name));
	ASSERT_NEW(seis_meta.data(), false);

	int updating_num;
	if(!GetMeta(seis_meta.data(), (char*)&updating_num, sizeof(updating_num))) {
		return false;
	}

	updating_num --;

	if(!PutMeta(seis_meta.data(), (char*)&updating_num, sizeof(updating_num))) {
		return false;
	}
	return true;
}

bool MetaManager::ClearAllUpdating() {
	std::string update_meta_name = GetUpdatingMetaName();
	ScopedPointer<SeisMeta> seis_meta(new SeisMeta(update_meta_name));
	ASSERT_NEW(seis_meta.data(), false);

	int updating_num = 0;
	if(!PutMeta(seis_meta.data(), (char*)&updating_num, sizeof(updating_num))) {
		return false;
	}
	return true;
}

bool MetaManager::GetUpdatingNum(int &updating_num) {
	std::string update_meta_name = GetUpdatingMetaName();
	ScopedPointer<SeisMeta> seis_meta(new SeisMeta(update_meta_name));
	ASSERT_NEW(seis_meta.data(), false);

	if(!GetMeta(seis_meta.data(), (char*)&updating_num, sizeof(updating_num))) {
		return false;
	}

	return true;
}


bool MetaManager::RecordTime(time_t time, int time_type) {
	std::string meta_name;

	if(time_type == LAST_ACC) {
		meta_name = GetLastAccMetaName();
	} else if(time_type == LAST_MOD) {
		meta_name = GetLastModMetaName();
	}

	ScopedPointer<SeisMeta> seis_meta(new SeisMeta(meta_name));
	ASSERT_NEW(seis_meta.data(), false);

	if(!PutMeta(seis_meta.data(), (char*)&time, sizeof(time))) {
		return false;
	}
	return true;
}

time_t MetaManager::GetTime(int time_type) {

	std::string meta_name;

	if(time_type == LAST_ACC) {
		meta_name = GetLastAccMetaName();
	} else if(time_type == LAST_MOD) {
		meta_name = GetLastModMetaName();
	}

	ScopedPointer<SeisMeta> seis_meta(new SeisMeta(meta_name));
	ASSERT_NEW(seis_meta.data(), -1);

	time_t time;
	if(!GetMeta(seis_meta.data(), (char*)&time, sizeof(time))) {
		return -1;
	}
	return time;
}


bool MetaManager::RecordLastAccessed() {
	time_t now = time(NULL);
	return RecordTime(now, LAST_ACC);
}
time_t MetaManager::GetLastAccessed() {
	return GetTime(LAST_ACC);
}
bool MetaManager::RecordLastModified() {
	time_t now = time(NULL);
	return RecordTime(now, LAST_MOD);
}
time_t MetaManager::GetLastModified() {
	return GetTime(LAST_MOD);
}

bool MetaManager::GetHeadType(HeadType *head_type) {
	std::string head_type_meta_name = GetHeadTypeMetaName();
	std::string head_type_num_meta_name = GetHeadTypeNumMetaName();

	ScopedPointer<SeisMeta> seis_meta_type_num(new SeisMeta(head_type_num_meta_name));
	ASSERT_NEW(seis_meta_type_num.data(), false);

	ScopedPointer<SeisMeta> seis_meta_type(new SeisMeta(head_type_meta_name));
	ASSERT_NEW(seis_meta_type.data(), false);
	int type_num;

	if(!GetMeta(seis_meta_type_num.data(), (char*)&type_num, sizeof(type_num))) {
		return false;
	}

	ScopedPointer<int16_t> type_buf(new int16_t[type_num]);
	if(!GetMeta(seis_meta_type.data(), (char*)(type_buf.data()), sizeof(int16_t) * type_num)) {
		return false;
	}

	for(int i = 0; i < type_num; ++i) {
		head_type->type_arr.push_back(type_buf.data()[i]);
	}

	return true;

}
bool MetaManager::GetTraceType(TraceType *trace_type) {
	std::string trace_type_meta_name = GetTraceTypeMetaName();
	ScopedPointer<SeisMeta> seis_meta(new SeisMeta(trace_type_meta_name));
	ASSERT_NEW(seis_meta.data(), false);

	if(!GetMeta(seis_meta.data(), (char*)&(trace_type->trace_size), sizeof(trace_type->trace_size))) {
		return false;
	}
	return true;
}
bool MetaManager::SaveHeadType(const HeadType &head_type) {
	std::string head_type_meta_name = GetHeadTypeMetaName();
	std::string head_type_num_meta_name = GetHeadTypeNumMetaName();

	ScopedPointer<SeisMeta> seis_meta_type_num(new SeisMeta(head_type_num_meta_name));
	ASSERT_NEW(seis_meta_type_num.data(), false);

	ScopedPointer<SeisMeta> seis_meta_type(new SeisMeta(head_type_meta_name));
	ASSERT_NEW(seis_meta_type.data(), false);

	int type_num = head_type.type_arr.size();

	if(!PutMeta(seis_meta_type_num.data(), (char*)&type_num, sizeof(type_num))) {
		return false;
	}

	ScopedPointer<int16_t> type_buf(new int16_t[type_num]);

	for(int i = 0; i < type_num; ++i) {
		type_buf.data()[i] = head_type.type_arr[i];
	}

	if(!PutMeta(seis_meta_type.data(), (char*)(type_buf.data()), sizeof(int16_t) * type_num)) {
		return false;
	}

	return true;
}
bool MetaManager::SaveTraceType(const TraceType &trace_type) {
	std::string trace_type_meta_name = GetTraceTypeMetaName();
	ScopedPointer<SeisMeta> seis_meta(new SeisMeta(trace_type_meta_name));
	ASSERT_NEW(seis_meta.data(), false);

	if(!PutMeta(seis_meta.data(), (char*)&(trace_type.trace_size), sizeof(trace_type.trace_size))) {
		return false;
	}
	return true;
}


bool MetaManager::TryLockWriteHead() {
	ScopedPointer<GCacheMutex> write_mutex_head(new GCacheMutex());
	ASSERT_NEW(write_mutex_head.data(), false);

	std::string write_mutex_head_name = this->GetWriteMutexHeadName();
	write_mutex_head->setProperty(write_mutex_head_name);
	if(0 == write_mutex_head->trylock()) {
		return true;
	} else {
		 return false;
	}
}
void MetaManager::UnlockWriteHead() {
	ScopedPointer<GCacheMutex> write_mutex_head(new GCacheMutex());
	std::string write_mutex_head_name = this->GetWriteMutexHeadName();
	write_mutex_head->setProperty(write_mutex_head_name);
	write_mutex_head->unlock();
}

bool MetaManager::TryLockWriteTrace() {
	ScopedPointer<GCacheMutex> write_mutex_trace(new GCacheMutex());
	ASSERT_NEW(write_mutex_trace.data(), false);

	std::string write_mutex_trace_name = this->GetWriteMutexTraceName();
	write_mutex_trace->setProperty(write_mutex_trace_name);
	if(0 == write_mutex_trace->trylock()) {
		return true;
	} else {
		 return false;
	}
}
void MetaManager::UnlockWriteTrace() {
	ScopedPointer<GCacheMutex> write_mutex_trace(new GCacheMutex());

	std::string write_mutex_trace_name = this->GetWriteMutexTraceName();
	write_mutex_trace->setProperty(write_mutex_trace_name);
	write_mutex_trace->unlock();
}

bool MetaManager::TryLockWrite() {
	if(!TryLockWriteHead()) {
		return false;
	}

	if(!TryLockWriteTrace()) {
		UnlockWriteHead();
		return false;
	}

	return true;
}


void MetaManager::UnlockWrite() {
	UnlockWriteHead();
	UnlockWriteTrace();
}


bool MetaManager::Remove() {

	ScopedPointer<SeisMeta> seis_meta(new SeisMeta(data_name_));
	ASSERT_NEW(seis_meta.data(), false);

	if(!seis_meta->removeAll(data_name_)) {//TODO  if not  exist return true
		sc_errno = SCCombineErrno(SC_ERR_RM_META, errno);
		return false;
	}
	return true;
}
bool MetaManager::CopyMeta(const std::string &new_name) {
	ScopedPointer<MetaManager> meta_dest(new MetaManager(new_name));
	ASSERT_NEW(meta_dest.data(), false);

	int writing_num, updating_num;
	if(!this->GetWritingNum(writing_num)) {
		return false;
	}
	if(!this->GetUpdatingNum(updating_num)) {
		return false;
	}
	if(writing_num > 0 || updating_num > 0) {
		sc_errno = SC_ERR_BUSY;
		return false;
	}

	if(!meta_dest->ClearWriting()){
		return false;
	}

	if(!meta_dest->ClearAllUpdating()) {
		return false;
	}

	if(!meta_dest->ClearWriteBackFlag(MOD_HEAD)){
		return false;
	}

	if(!meta_dest->ClearWriteBackFlag(MOD_TRACE)){
		return false;
	}

	if(!meta_dest->RecordTime(this->GetLastAccessed(), LAST_ACC)) { //TODO set to now or same with old data?
		return false;
	}

	if(!meta_dest->RecordTime(this->GetLastModified(), LAST_MOD)) { //TODO set to now or same with old data?
		return false;
	}

	HeadType head_type;
	TraceType trace_type;
	if(!this->GetHeadType(&head_type)){
		return false;
	}
	if(!this->GetTraceType(&trace_type)) {
		return false;
	}
	if(!meta_dest->SaveHeadType(head_type)) {
		return false;
	}
	if(!meta_dest->SaveTraceType(trace_type)) {
		return false;
	}

	return true;

}


}

}


