/*
 * seisfile_nas_reader.cpp
 *
 *  Created on: Aug 5, 2018
 *      Author: cssl
 */

#include "seisfile_nas_reader.h"
#include <seisfile_nas_meta.h>
#include <stdio.h>

//for test
#include <malloc.h>
#include <string.h>

namespace seisfs {
namespace file {

/*ReaderNAS::ReaderNAS(const std::string& filename) {

	seisnas_home_path_ = getenv("GEFILE_DISK_PATH");

	_head_filename = seisnas_home_path_ + "/" + filename + "_head";
	_trace_filename = seisnas_home_path_ + "/" + filename + "_trace";
	_meta_filename = seisnas_home_path_ + "/" + filename +"_meta";
	_filename = filename;
	ReaderNAS::Init();
}*/

ReaderNAS::ReaderNAS(const std::string& filename, MetaNAS *meta) {
	_meta = meta;

	seisnas_home_path_ = getenv("GEFILE_DISK_PATH");

	_head_filename = seisnas_home_path_ + "/" + filename + "_head";
	_trace_filename = seisnas_home_path_ + "/" + filename + "_trace";
	_meta_filename = seisnas_home_path_ + "/" + filename +"_meta";
	_filename = filename;
	ReaderNAS::Init();
}

bool ReaderNAS::Init() {
	_cur_logic_num = 0;
	//_actual_trace_num = 0;
	_last_actual_num = 0;

	_gf_head = new GEFile(_head_filename);
	_gf_trace = new GEFile(_trace_filename);
	_gf_head->SetProjectName(_head_filename);
	_gf_trace->SetProjectName(_trace_filename);
	_gf_head_next = new GEFile(_head_filename);
	_gf_trace_next = new GEFile(_trace_filename);
	_gf_head_next->SetProjectName(_head_filename);
	_gf_trace_next->SetProjectName(_trace_filename);
	/*_meta = new MetaNAS(_meta_filename);
	_meta->MetaRead();*/
	_head_type = _meta->MetaGetheadtype();
	_trace_type = _meta->MetaGettracetype();
	//Seek(_cur_logic_num);
	/*_gf_head_next->SeekToBegin();
	_gf_trace_next->SeekToBegin();*/

	seisfs::HeadFilter init_head_filter;
	for(int i = 0; i < _meta->MetaGetkeynum(); i++){
		init_head_filter.AddKey(i);
	}
	seisfs::RowScope init_row_scope;
	init_row_scope.SetStartTrace(0);
	init_row_scope.SetStopTrace(-1);
	seisfs::RowFilter init_row_filter(init_row_scope);
	ReaderNAS::SetHeadFilter(init_head_filter);
	ReaderNAS::SetRowFilter(init_row_filter);

	if(_gf_head->Open(IO_READONLY, true) && _gf_trace->Open(IO_READONLY, true) &&
			_gf_head_next->Open(IO_READONLY, true) && _gf_trace_next->Open(IO_READONLY, true)){
		return true;
	}
	else{
		//error:fail to open file
		return false;
	}
}

ReaderNAS::~ReaderNAS() {
	delete _gf_head;
	delete _gf_trace;
	delete _gf_head_next;
	delete _gf_trace_next;
}

/*
 * caller should not delete the pointer of filter after calling this function
 */
bool ReaderNAS::SetHeadFilter(const HeadFilter& head_filter) {
	_head_filter = head_filter;
	return true;
}

/*
 * caller should not delete the pointer of filter after calling this function
 */
bool ReaderNAS::SetRowFilter(const RowFilter& row_filter) {
	_row_filter = row_filter;
	return true;
}


int ReaderNAS::GetTraceSize() {
	_trace_type = _meta->MetaGettracetype();
	return _trace_type.trace_size;
}

int ReaderNAS::GetHeadSize() {
	_head_type = _meta->MetaGetheadtype();
	uint32_t headlength = 0;
	for(auto i = _head_filter.GetKeyList().begin(); i != _head_filter.GetKeyList().end(); i++){
		headlength += _head_type.head_size[(*i)];
	}
	return headlength;

} //return length of one head, in bytes

uint64_t ReaderNAS::GetTraceNum() {
	int64_t tracenum = 0;
	int64_t temp = 0;
	for(int i = 0; i < _row_filter.GetAllScope().size(); i++){
		if(_row_filter.GetAllScope()[i].GetStopTrace() != -1){
			temp = _row_filter.GetAllScope()[i].GetStopTrace() - _row_filter.GetAllScope()[i].GetStartTrace() + 1;
		}
		else{
			temp = (_meta->MetaGetheadnum()>_meta->MetaGettracenum()?_meta->MetaGetheadnum():_meta->MetaGettracenum())
					- _row_filter.GetAllScope()[i].GetStartTrace();
		}
		tracenum += temp;
	}
	return tracenum;
}

/**
 * Move the read offset to head_idx(th) head.
 */
bool ReaderNAS::Seek(int64_t trace_num) {
	/*if(trace_num > ReaderNAS::GetTraceNum() - 1){
		//error:trace index out of range
		return false;
	}
	_cur_logic_num = trace_num;

	int64_t head_pos = 0, trace_pos = 0;
	int64_t count = 0;  //find actual tracenum by checking rowfilter
	int64_t cur_scope_num = 0;
	int64_t temp;  //record tracenum of one scope
	while((count <= trace_num) && (cur_scope_num < _row_filter.GetAllScope().size())){
		if(_row_filter.GetAllScope()[cur_scope_num].GetStopTrace() != -1){
			temp = _row_filter.GetAllScope()[cur_scope_num].GetStopTrace() - _row_filter.GetAllScope()[cur_scope_num].GetStartTrace() + 1;
		}
		else{
			temp = trace_num - _row_filter.GetAllScope()[cur_scope_num].GetStartTrace() + 1;
		}
		count += temp;
		cur_scope_num++;
	}
	cur_scope_num--;
	temp = (_row_filter.GetAllScope()[cur_scope_num].GetStopTrace() == -1) ? trace_num : _row_filter.GetAllScope()[cur_scope_num].GetStopTrace();
	_actual_trace_num = temp - (count - trace_num) + 1;
	head_pos = _actual_trace_num * _meta->MetaGetheadlength();
	trace_pos = _actual_trace_num * _meta->MetaGettracelength();
	std::cout<<"_actual_trace_num is:"<<_actual_trace_num<<endl;
	//std::cout<<"_last_trace_num is:"<<_last_trace_num<<endl;
	//if(_actual_trace_num != _last_trace_num + 1){
		_gf_head->Seek(head_pos, SEEK_SET);
		_gf_trace->Seek(trace_pos, SEEK_SET);
	//}
	//_last_trace_num = _actual_trace_num;*/

	_cur_logic_num = trace_num;
	int64_t head_pos = 0, trace_pos = 0;
	int64_t realnum = ReaderNAS::GetRealNum(trace_num);
	if(realnum == _last_actual_num + 1){  //physically next to previous trace, needn't seek
		//printf("needn't seek\n");
		_last_actual_num = realnum;
		return true;
	}
	//printf("realnum is:%u, _last_actual_num is:%u\n", realnum, _last_actual_num);
	_last_actual_num = realnum;
	//printf("real num is:%u\n", realnum);
	head_pos = realnum * _meta->MetaGetheadlength();
	trace_pos = realnum * _meta->MetaGettracelength();
	//printf("head_pos is:%u\n", head_pos);
	//_gf_head_next->SeekToBegin();
	//printf("head pos is:%u\n", _gf_head->GetPosition());
	_gf_head_next->Seek(head_pos, SEEK_SET);
	_gf_trace_next->Seek(trace_pos, SEEK_SET);
	//printf("head pos is:%u\n", _gf_head_next->GetPosition());

	return true;
}

int64_t ReaderNAS::Tell() {
	/*int64_t pos = _gf_head->GetPosition();
	int64_t physical_tracenum = pos / _meta->MetaGetheadlength();
	int64_t trace_num = 0;
	int64_t temp = 0;
	int mark = 0;
	for(int i = 0; i < _row_filter.GetAllScope().size(); i++){
		if(mark == 1){
			break;
		}
		temp = (_row_filter.GetAllScope()[i].GetStopTrace() == -1)?_meta->MetaGettracenum():_row_filter.GetAllScope()[i].GetStopTrace();
		for(int64_t j = _row_filter.GetAllScope()[i].GetStartTrace(); j <= temp; j++){
			if(physical_tracenum != j){
				trace_num++;
			}
			else{
				mark = 1;
				break;
			}
		}
	}
	trace_num++;
	return trace_num;*/
	return _cur_logic_num;
}

bool ReaderNAS::Get(int64_t trace_num, void *head, void* trace){
	if(trace_num >= _meta->MetaGettracenum()){
		//error:trace index out of range
		//printf("tracenum is:%u and total is:%u\n", trace_num, _meta->MetaGettracenum());
		std::cout<<"trace index out of range"<<endl;
		return false;
	}
	char *p = (char *)head;
	int64_t offset_in_head = 0;
	int64_t one_key_size = 0;
	/*for(auto i = _head_filter.GetKeyList().begin(); i != _head_filter.GetKeyList().end(); ++i){
		offset_in_head = 0;
		HeadType headtype = _meta->MetaGetheadtype();
		int64_t j = 0;
		while(j <= (*i) - 1){
			offset_in_head += headtype.head_size[j];   //find the position of the key
			j++;
		}
		one_key_size = headtype.head_size[j];
		ReaderNAS::Seek(trace_num);
		_gf_head->Seek(offset_in_head, SEEK_CUR);  //seek to the key position
		_gf_head->Read(p, one_key_size);
		p[one_key_size] = '\0';
		p += one_key_size;
	}*/

	//modify head reading
	int64_t head_pos = 0, trace_pos = 0;
	head_pos = trace_num * _meta->MetaGetheadlength();
	trace_pos = trace_num * _meta->MetaGettracelength();
	_gf_head->Seek(head_pos, SEEK_SET);
	_gf_trace->Seek(trace_pos, SEEK_SET);
	char *total_head = new char[_meta->MetaGetheadlength()];
	_gf_head->Read(total_head, _meta->MetaGetheadlength());
	//read the total head in one time and filter it after read
	for(auto i = _head_filter.GetKeyList().begin(); i != _head_filter.GetKeyList().end(); ++i){
		offset_in_head = 0;
		HeadType headtype = _meta->MetaGetheadtype();
		int64_t j = 0;
		while(j <= (*i) - 1){
			offset_in_head += headtype.head_size[j];   //find the position of the key
			j++;
		}
		one_key_size = headtype.head_size[j];
		//ReaderNAS::Seek(trace_num);
		//_gf_head->Seek(offset_in_head, SEEK_CUR);  //seek to the key position
		//_gf_head->Read(p, one_key_size);
		memcpy(p, total_head + offset_in_head,  one_key_size);
		p[one_key_size] = '\0';
		p += one_key_size;
	}

	//read trace
	char *p1 = (char *)trace;
	_gf_trace->Read(p1, _meta->MetaGettracelength());
	p1[_meta->MetaGettracelength()]='\0';
	//_last_trace_num = _actual_trace_num;

	return true;

}

/**
 * Return true if there are more heads. The routine usually accompany with NextHead
 */
bool ReaderNAS::HasNext() {
	//printf("current trace num is:%d\n",ReaderNAS::Tell());
	if(ReaderNAS::Tell() < ReaderNAS::GetTraceNum()){ //modify equal
		return true;
	}
	return false;
}

bool ReaderNAS::Next(void* head, void* trace){ //Next(void* head) {
	if(ReaderNAS::HasNext()){
		//return ReaderNAS::Get(ReaderNAS::Tell()/* + 1*/, head, trace);  //plus 1
		ReaderNAS::Seek(_cur_logic_num);
		char *p2 = (char *)head;
		int64_t offset_in_head = 0;
		int64_t one_key_size = 0;
		char *total_head = new char[_meta->MetaGetheadlength()];
		//printf("size is:%u\n",_gf_head_next->GetPosition());
		_gf_head_next->Read(total_head, _meta->MetaGetheadlength());
		//read the total head in one time and filter it after read
		for(auto i = _head_filter.GetKeyList().begin(); i != _head_filter.GetKeyList().end(); ++i){
			offset_in_head = 0;
			HeadType headtype = _meta->MetaGetheadtype();
			int64_t j = 0;
			while(j <= (*i) - 1){
				offset_in_head += headtype.head_size[j];   //find the position of the key
				j++;
			}
			one_key_size = headtype.head_size[j];
			//ReaderNAS::Seek(trace_num);
			//_gf_head->Seek(offset_in_head, SEEK_CUR);  //seek to the key position
			//_gf_head->Read(p, one_key_size);
			memcpy(p2, total_head + offset_in_head,  one_key_size);
			p2[one_key_size] = '\0';
			p2 += one_key_size;
		}

		//read trace
		char *p3 = (char *)trace;
		_gf_trace_next->Read(p3, _meta->MetaGettracelength());
		p3[_meta->MetaGettracelength()]='\0';
		_cur_logic_num++;
		return true;

	}
	//printf("No next trace\n");
	return false;
}

bool ReaderNAS::Close() {
	_gf_head->Close();
	_gf_trace->Close();
	_gf_head_next->Close();
	_gf_trace_next->Close();
	return true;
}

int64_t ReaderNAS::GetRealNum(int64_t trace_num){
	int64_t realnum;
	if(trace_num >= _meta->MetaGettracenum()){
		//error:trace index out of range
		//printf("tracenum is:%u and total is:%u\n", trace_num, _meta->MetaGettracenum());
		std::cout<<"trace index out of range"<<endl;
		return -1;
	}

	int64_t count = 0;  //find actual tracenum by checking rowfilter
	int64_t cur_scope_num = 0;
	int64_t temp;  //record tracenum of one scope
	while((count <= trace_num) && (cur_scope_num < _row_filter.GetAllScope().size())){
		if(_row_filter.GetAllScope()[cur_scope_num].GetStopTrace() != -1){
			temp = _row_filter.GetAllScope()[cur_scope_num].GetStopTrace() - _row_filter.GetAllScope()[cur_scope_num].GetStartTrace() + 1;
		}
		else{
			temp = trace_num - _row_filter.GetAllScope()[cur_scope_num].GetStartTrace() + 1;
		}
		count += temp;
		cur_scope_num++;
	}
	cur_scope_num--;
	temp = (_row_filter.GetAllScope()[cur_scope_num].GetStopTrace() == -1) ? trace_num : _row_filter.GetAllScope()[cur_scope_num].GetStopTrace();
	realnum = temp - (count - trace_num) + 1;

	return realnum;
}


//int64_t ReaderNAS::_last_trace_num = 0;

}

}


