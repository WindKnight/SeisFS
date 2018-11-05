/*
 * seisfile_hdfs_headreader.cpp
 *
 *  Created on: Jun 26, 2018
 *      Author: wyd
 */

#include "seisfile_hdfs_headreader.h"
#include <seisfile_hdfs_meta.h>

#include <string.h>

namespace seisfs {
namespace file {

HeadReaderHDFS::~HeadReaderHDFS() {
//	delete _gf_head;
//	delete _gf_head_next;
}

HeadReaderHDFS::HeadReaderHDFS(const std::string& filename, MetaHDFS *meta){
	_meta = meta;

	seisnas_home_path_ = getenv(GEFILE_ENV_NAME);

	_head_filename = seisnas_home_path_ + "/" + filename + "_head";
	_meta_filename = seisnas_home_path_ + "/" + filename +"_meta";
	_filename = filename;
	HeadReaderHDFS::Init();
}

bool HeadReaderHDFS::Init(){
	_cur_logic_num = 0;
	_last_actual_num = 0;

//	_gf_head = new GEFile(_head_filename);
//	_gf_head->SetProjectName(_head_filename);
//	_gf_head_next = new GEFile(_head_filename);
//	_gf_head_next->SetProjectName(_head_filename);
	_head_type = _meta->MetaGetheadtype();

	seisfs::HeadFilter init_head_filter;
	for(int i = 0; i < _meta->MetaGetkeynum(); i++){
		init_head_filter.AddKey(i);
	}
	seisfs::RowScope init_row_scope;
	init_row_scope.SetStartTrace(0);
	init_row_scope.SetStopTrace(-1);
	seisfs::RowFilter init_row_filter(init_row_scope);
	HeadReaderHDFS::SetHeadFilter(init_head_filter);
	HeadReaderHDFS::SetRowFilter(init_row_filter);

//	if(_gf_head->Open(IO_READONLY, true) &&_gf_head_next->Open(IO_READONLY, true)){
//		return true;
//	}
//	else{
//		//error:fail to open file
//		return false;
//	}
	fd_head_ = open(_head_filename.c_str(), O_RDONLY);
	if(0 > fd_head_) {
		return false;
	}
	fd_head_next_ = open(_head_filename.c_str(), O_RDONLY);
	if(0 > fd_head_next_) {
		return false;
	}
	return true;
}

/*
 * caller should not delete the pointer of filter after calling this function
 */
bool HeadReaderHDFS::SetHeadFilter(const HeadFilter& head_filter) {
	_head_filter = head_filter;
	return true;
}

/*
 * caller should not delete the pointer of filter after calling this function
 */
bool HeadReaderHDFS::SetRowFilter(const RowFilter& row_filter) {
	_row_filter = row_filter;
	return true;
}

int HeadReaderHDFS::GetHeadSize() {
	_head_type = _meta->MetaGetheadtype();
	uint32_t headlength = 0;
	for(auto i = _head_filter.GetKeyList().begin(); i != _head_filter.GetKeyList().end(); i++){
		headlength += _head_type.head_size[(*i)];
	}
	return headlength;
} //return length of one head, in bytes

int64_t HeadReaderHDFS::GetTraceNum() {
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
bool HeadReaderHDFS::Seek(int64_t trace_num) {
	_cur_logic_num = trace_num;
	int64_t head_pos = 0;
	int64_t realnum = HeadReaderHDFS::GetRealNum(trace_num);
	if(realnum == _last_actual_num + 1){  //physically next to previous trace, needn't seek
		//printf("needn't seek\n");
		_last_actual_num = realnum;
		return true;
	}
	_last_actual_num = realnum;
	head_pos = realnum * _meta->MetaGetheadlength();
//	_gf_head_next->Seek(head_pos, SEEK_SET);
	lseek(fd_head_, head_pos, SEEK_SET);

	return true;
}

int64_t HeadReaderHDFS::Tell() {
	return _cur_logic_num;
}

bool HeadReaderHDFS::Get(int64_t trace_num, void* head) {
	if(trace_num >= _meta->MetaGettracenum()){
		//error:trace index out of range
		std::cout<<"trace index out of range"<<endl;
		return false;
	}
	char *p = (char *)head;
	int64_t offset_in_head = 0;
	int64_t one_key_size = 0;
	//modify head reading
	int64_t head_pos = 0, trace_pos = 0;
	head_pos = trace_num * _meta->MetaGetheadlength();
//	_gf_head->Seek(head_pos, SEEK_SET);
	lseek(fd_head_, head_pos, SEEK_SET);
	char *total_head = new char[_meta->MetaGetheadlength()];
//	_gf_head->Read(total_head, _meta->MetaGetheadlength());
	read(fd_head_, total_head, _meta->MetaGetheadlength());
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
		memcpy(p, total_head + offset_in_head,  one_key_size);
		p[one_key_size] = '\0';
		p += one_key_size;
	}


	return true;
}

/**
 * Return true if there are more heads. The routine usually accompany with NextHead
 */
bool HeadReaderHDFS::HasNext() {
	if(HeadReaderHDFS::Tell() < HeadReaderHDFS::GetTraceNum()){ //modify equal
		return true;
	}
	return false;
}


bool HeadReaderHDFS::Next(void* head) {
	if(HeadReaderHDFS::HasNext()){
		HeadReaderHDFS::Seek(_cur_logic_num);
		char *p2 = (char *)head;
		int64_t offset_in_head = 0;
		int64_t one_key_size = 0;
		char *total_head = new char[_meta->MetaGetheadlength()];
//		_gf_head_next->Read(total_head, _meta->MetaGetheadlength());
		read(fd_head_next_, total_head, _meta->MetaGetheadlength());
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
			memcpy(p2, total_head + offset_in_head,  one_key_size);
			p2[one_key_size] = '\0';
			p2 += one_key_size;
		}

		_cur_logic_num++;
		return true;

	}
	return false;
}

int64_t HeadReaderHDFS::GetRealNum(int64_t trace_num){
	int64_t realnum;
	if(trace_num >= _meta->MetaGettracenum()){
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

bool HeadReaderHDFS::Close() {
//	_gf_head->Close();
//	_gf_head_next->Close();
	close(fd_head_);
	close(fd_head_next_);
	return true;
}

}

}


