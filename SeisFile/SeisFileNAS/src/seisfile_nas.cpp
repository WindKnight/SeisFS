/*
 * seisfile_nas.cpp
 *
 *  Created on: Apr 24, 2018
 *      Author: wyd
 */

#include "seisfile_nas.h"
#include "seisfile_nas_kvinfo.h"
#include "seisfile_nas_headreader.h"
#include "seisfile_nas_tracereader.h"
#include "seisfile_nas_writer.h"
#include "seisfile_nas_tracewriter.h"
#include "seisfile_nas_headwriter.h"
#include "seisfile_nas_reader.h"
#include "seisfile_nas_traceupdater.h"
#include "seisfile_nas_headupdater.h"
#include "seisfile_nas_headmerger.h"
#include "seisfile_nas_tracemerger.h"
#include "util/seisfs_util_log.h"
#include <iostream>

#include <string.h>

namespace seisfs {

namespace file {

SeisFileNAS::SeisFileNAS(const std::string &data_name) :
		SeisFile(data_name){
	//PRINTL

	seisnas_home_path_ = getenv("GEFILE_DISK_PATH");

	_data_name = data_name;
	_sortkey_filename = seisnas_home_path_ + "/" +  _data_name + "_sortkeys";
	_meta_filename = seisnas_home_path_ + "/" + _data_name + "_meta";

	//_meta = new seisfs::file::MetaNAS(seisnas_home_path_ + "/" + _data_name + "_meta");
	//_meta->MetaRead(); //wyd exist will always return true.

	SeisFileNAS::Init();
}

SeisFileNAS::~SeisFileNAS() {

}

bool SeisFileNAS::Init() {
	_meta = new seisfs::file::MetaNAS(seisnas_home_path_ + "/" + _data_name + "_meta");
	_meta->Init();
	//_meta->MetaRead();

	_gf_sortkeys = new GEFile(_sortkey_filename);
	_gf_sortkeys->Open(IO_READWRITE, false);
	return true;
}
/**
 * Record the sort order of this data instance
 */
bool SeisFileNAS::SetSortKeys(const SortKeys& sort_keys) {
	_sort_keys.clear();
	char p1[sizeof(int64_t)];
	int64_t keynum = sort_keys.size();
	_gf_sortkeys->SeekToBegin();
	memcpy(p1, &keynum, sizeof(int64_t));
	_gf_sortkeys->Write(p1, sizeof(int64_t));

	char p[sizeof(SeisKey)];
	for(int64_t i = 0; i < keynum; i++){
		SeisKey tempkey = sort_keys[i];
		_sort_keys.push_back(tempkey);
		memcpy(p, &tempkey, sizeof(SeisKey));
		_gf_sortkeys->Write(p, sizeof(SeisKey));
	}
	return true;
}

/**
 * Get the sort keys.
 */
SortKeys SeisFileNAS::GetSortKeys() {
	_sort_keys.clear();
	char p1[sizeof(int64_t)], p[sizeof(SeisKey)];
	int64_t keynum = 0;
	_gf_sortkeys->SeekToBegin();
	_gf_sortkeys->Read(p1, sizeof(int64_t));
	memcpy(&keynum, p1, sizeof(int64_t));
	for(int64_t i = 0; i < keynum; i++){
		SeisKey tempkey = 0;
		_gf_sortkeys->Read(p, sizeof(SeisKey));
		memcpy(&tempkey, p, sizeof(SeisKey));
		_sort_keys.push_back(tempkey);
	}
	return _sort_keys;
}

/**
 * Like k-v table. SFReel filename format: {data_name}.{key}.SFReel
 */
KVInfo* SeisFileNAS::OpenKVInfo() {
	KVInfoNAS *kvinfo_nas = new KVInfoNAS(_data_name);
	return kvinfo_nas;
}

/*
 * Read only
 */
HeadReader* SeisFileNAS::OpenHeadReader() {
	if(!SeisFileNAS::Exists()){
		//error
		std::cout<<"No Such Data!"<<endl;
		return 0;
	}
	_meta->CreateMetaFile();
	HeadReaderNAS *ret_reader = new HeadReaderNAS(_data_name, _meta);
	return ret_reader;
}
TraceReader* SeisFileNAS::OpenTraceReader() {
	if(!SeisFileNAS::Exists()){
		//error
		std::cout<<"No Such Data!"<<endl;
		return 0;
	}
	_meta->CreateMetaFile();
	TraceReaderNAS *ret_reader = new TraceReaderNAS(_data_name, _meta);
	return ret_reader;
}
Reader* SeisFileNAS::OpenReader() {
	if(!SeisFileNAS::Exists()){
		//error
		std::cout<<"No Such Data!"<<endl;
		return 0;
	}
	_meta->CreateMetaFile();
	_reader = new ReaderNAS(_data_name, _meta);
	return _reader;
}

/*
 * Append only
 */
HeadWriter* SeisFileNAS::OpenHeadWriter(const HeadType &head_type, Lifetime lifetime_days) {
	_meta->CreateMetaFile();
	_meta->MetaSetheadtype(head_type);
	_meta->MetaSetLifetime(lifetime_days);
	HeadWriter *headwriter = new HeadWriterNAS(_data_name, _meta);
	return headwriter;
}
TraceWriter* SeisFileNAS::OpenTraceWriter(const TraceType& trace_type, Lifetime lifetime_days) {
	_meta->CreateMetaFile();
	_meta->MetaSettracetype(trace_type);
	_meta->MetaSetLifetime(lifetime_days);
	TraceWriter *tracewriter = new TraceWriterNAS(_data_name, _meta);
	return tracewriter;
}
Writer* SeisFileNAS::OpenWriter(const HeadType &head_type, const TraceType& trace_type, Lifetime lifetime_days) {
	_meta->CreateMetaFile();
	_meta->MetaSetheadtype(head_type);
	_meta->MetaSettracetype(trace_type);
	_meta->MetaSetLifetime(lifetime_days);
	Writer *writer = new WriterNAS(_data_name, _meta);
	return writer;
}

HeadWriter* SeisFileNAS::OpenHeadWriter() {
	_meta->CreateMetaFile();
	HeadWriter *headwriter = new HeadWriterNAS(_data_name, _meta);
	return headwriter;
}
TraceWriter* SeisFileNAS::OpenTraceWriter() {
	_meta->CreateMetaFile();
	TraceWriter *tracewriter = new TraceWriterNAS(_data_name, _meta);
	return tracewriter;
}
Writer* SeisFileNAS::OpenWriter() {
	_meta->CreateMetaFile();
	Writer *writer = new WriterNAS(_data_name, _meta);
	//printf("meta trace size is:%u\n",_meta->MetaGettracetype().trace_size);
	return writer;
}

/*
 * update only
 */
HeadUpdater* SeisFileNAS::OpenHeadUpdater() {
	HeadUpdaterNAS *headupdater = new HeadUpdaterNAS(_data_name);
	return headupdater;
}
TraceUpdater* SeisFileNAS::OpenTraceUpdater() {
	TraceUpdaterNAS *traceupdater = new TraceUpdaterNAS(_data_name);
	return traceupdater;
}

bool SeisFileNAS::Remove() {
	if(!SeisFileNAS::Exists()){
		return true;
	}
	GEFile::Remove(seisnas_home_path_ + "/" + _data_name + "_head");
	GEFile::Remove(seisnas_home_path_ + "/" + _data_name + "_trace");
	GEFile::Remove(seisnas_home_path_ + "/" + _data_name + "_meta");
	GEFile::Remove(seisnas_home_path_ + "/" + _data_name + "sortkeys");
	//wyd does not remove all files.
	//check after remove. keep removing until not exist.
	return true;
}

bool SeisFileNAS::Rename(const std::string& new_name) {
	if(!SeisFileNAS::Exists()){
		return false;
	}
	GEFile *gf_head, *gf_trace, *gf_meta, *gf_sortkeys;
	gf_head = new GEFile(seisnas_home_path_ + "/" + _data_name + "_head");
	gf_head->Rename(seisnas_home_path_ + "/" + new_name + "_head");
	gf_trace = new GEFile(seisnas_home_path_ + "/" + _data_name + "_trace");
	gf_trace->Rename(seisnas_home_path_ + "/" + new_name + "_trace");
	gf_meta = new GEFile(seisnas_home_path_ + "/" + _data_name + "_meta");
	gf_meta->Rename(seisnas_home_path_ + "/" + new_name + "_meta");
	gf_sortkeys = new GEFile(seisnas_home_path_ + "/" + _data_name + "_sortkeys");
	gf_sortkeys->Rename(seisnas_home_path_ + "/" + new_name + "_sortkeys");
	return true;
}

bool SeisFileNAS::Copy(const std::string& newName) {

}

bool SeisFileNAS::SetLifetime(Lifetime lifetime_days) {
	_meta->MetaSetLifetime(lifetime_days);
	return true;
}
/*
 * merge src_data_name to current data.
 * if merge fail, both of the original data should be the same with those before merge.
 */
bool SeisFileNAS::Merge(const std::string& src_data_name) {
	/*int64_t src_headsize, src_tracesize, des_headsize, des_tracesize;
	seisfs::file::SeisFileNAS *seis_des = new seisfs::file::SeisFileNAS(_data_name);
	seisfs::file::SeisFileNAS *seis_src = new seisfs::file::SeisFileNAS(src_data_name);
	des_headsize = seis_des->GetHeadSize();
	des_tracesize = seis_des->GetTraceSize();
	src_headsize = seis_src->GetHeadSize();
	src_tracesize = seis_src->GetTraceSize();

	if((des_headsize != src_headsize) || (des_tracesize != src_tracesize)){
		return false;
	}
	else{
		for(int64_t i = 0; i < seis_src->GetTraceNum(); i++){
			char phead[src_headsize], ptrace[src_tracesize];
		}
	}*/
	std::string des_name, src_name;
	des_name = seisnas_home_path_ + "/" + _data_name;
	src_name = seisnas_home_path_ + "/" + src_data_name;
	/*if(!GEFile::Exists(src_head_name) || !GEFile::Exists(src_head_name)){
		//no source data
		return false;
	}
	if(!GEFile::Exists(des_head_name) || !GEFile::Exists(des_trace_name)){
		//no destination data
		return false;
	}*/

	HeadMergerNAS *hm = new HeadMergerNAS(des_name, src_name);
	hm->Merge();
	TraceMergerNAS *tm =new  TraceMergerNAS(des_name, src_name);
	tm->Merge();
	/*if(!GEFile::Exists(src_head_name) && !GEFile::Exists(src_trace_name)){
		//merge finish, delete source meta file
		GEFile::Remove(seisnas_home_path_ + src_data_name + "_meta");
	}*/
	GEFile::Remove(seisnas_home_path_ + "/" + src_data_name + "_meta");
	GEFile::Remove(seisnas_home_path_ + "/" + src_data_name + "_head");
	GEFile::Remove(seisnas_home_path_ + "/" + src_data_name + "_trace");
	return true;
}

bool SeisFileNAS::MergeHead(const std::string& src_data_name){
	std::string des_name, src_name;
	des_name = seisnas_home_path_ + "/" + _data_name;
	src_name = seisnas_home_path_ + "/" + src_data_name;
	HeadMergerNAS *hm = new HeadMergerNAS(des_name, src_name);
	hm->Merge();
	return true;
}

bool SeisFileNAS::MergeTrace(const std::string& src_data_name){
	std::string des_name, src_name;
	des_name = seisnas_home_path_ + "/" + _data_name;
	src_name = seisnas_home_path_ + "/" + src_data_name;
	TraceMergerNAS *tm =new  TraceMergerNAS(des_name, src_name);
	tm->Merge();
	return true;
}

/*
 * truncate
 */
bool SeisFileNAS::TruncateTrace(int64_t traces) {
	SeisFileNAS::ReadMeta();
	_meta->MetaSettracenum(traces);
	int64_t totallength = _meta->MetaGettracelength() * traces;
	GEFile *gf_trace_truncate = new GEFile(seisnas_home_path_ + "/" + _data_name + "_trace");
	return gf_trace_truncate->Truncate(totallength);
}
bool SeisFileNAS::TruncateHead(int64_t heads) {
	SeisFileNAS::ReadMeta();
	_meta->MetaSetheadnum(heads);
	int64_t totallength = _meta->MetaGetheadlength() * heads;
	GEFile *gf_head_truncate = new GEFile(seisnas_home_path_ + "/" + _data_name + "_head");
	return gf_head_truncate->Truncate(totallength);
}

/**
 * property
 */
bool SeisFileNAS::Exists() {
	//return false;
	if((!GEFile::Exists(seisnas_home_path_ + "/" + _data_name + "_head")) && (!GEFile::Exists(seisnas_home_path_ + "/" + _data_name + "_trace"))){
		return false;
	}
	return true;
}

int64_t SeisFileNAS::Size() {
	SeisFileNAS::ReadMeta();
	int64_t headlength, tracelength, headnum, tracenum, size;
	headlength = _meta->MetaGetheadlength();
	tracelength = _meta->MetaGettracelength();
	headnum = _meta->MetaGetheadnum();
	tracenum = _meta->MetaGettracenum();
	size = headlength*headnum + tracelength*tracenum;
	return size;

} //total size in bytes, include head

int SeisFileNAS::GetKeyNum() {
	SeisFileNAS::ReadMeta();
	return _meta->MetaGetkeynum();
}

int SeisFileNAS::GetTraceSize() {
	PRINTL
	SeisFileNAS::ReadMeta();
	return _meta->MetaGettracelength();
}//return length of one trace, in bytes

int SeisFileNAS::GetHeadSize() {
	PRINTL
	SeisFileNAS::ReadMeta();
	return _meta->MetaGetheadlength();
}//return length of one head, in bytes

//get trace number , returns the small number of heads or traces,
uint32_t SeisFileNAS::GetTraceNum() {
	SeisFileNAS::ReadMeta();
	uint32_t tracenum = 0;
	tracenum = _meta->MetaGetheadnum()>_meta->MetaGettracenum()?_meta->MetaGettracenum():_meta->MetaGetheadnum();
	return tracenum;
}

Lifetime SeisFileNAS::GetLifetime() {
	SeisFileNAS::ReadMeta();
	return _meta->MetaGetLifetime();
}

time_t SeisFileNAS::LastModified() {
	SeisFileNAS::ReadMeta();
	return _meta->MetaGetLastModify();
}

time_t SeisFileNAS::LastAccessed() {
	SeisFileNAS::ReadMeta();
	return _meta->MetaGetLastModify();
}

std::string SeisFileNAS::GetName() {
	return _data_name;
}

LifetimeStat SeisFileNAS::GetLifetimeStat() {
	SeisFileNAS::ReadMeta();
	return _meta->MetaGetLifetimestat();
}

void SeisFileNAS::SetLifetimeStat(LifetimeStat lt_stat) {
	_meta->MetaSetLifetimestat(lt_stat);
}

void SeisFileNAS::ReadMeta(){
	if(SeisFileNAS::Exists()){
		//error
		_meta->CreateMetaFile();
	}
	else{
		//error
		printf("No Such Data\n");
	}
}

}

}

