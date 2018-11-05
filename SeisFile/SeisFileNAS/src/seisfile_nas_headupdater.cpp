/*
 * seisfile_nas_headupdater.cpp
 *
 *  Created on: Aug 14, 2018
 *      Author: cssl
 */

#include "seisfile_nas_headupdater.h"
#include <string.h>

namespace seisfs {
namespace file {

HeadUpdaterNAS::HeadUpdaterNAS(const std::string& filename){

	seisnas_home_path_ = getenv("GEFILE_DISK_PATH");

	_head_filename = seisnas_home_path_ + "/" + filename + "_head";
	_meta_filename = seisnas_home_path_ + "/" + filename +"_meta";
	_filename = filename;
	HeadUpdaterNAS::Init();
}

bool HeadUpdaterNAS::Init(){
	_gf_head = new GEFile(_head_filename);
	_gf_head->SetProjectName(_filename + "head");
	_meta = new MetaNAS(_meta_filename);
	_meta->MetaRead();
	return _gf_head->Open(IO_READWRITE, true);
}

bool HeadUpdaterNAS::SetHeadFilter(const HeadFilter& head_filter){
	_head_filter = head_filter;
	return true;
}

bool HeadUpdaterNAS::SetRowFilter(const RowFilter& row_filter){
	_row_filter = row_filter;
	_num_of_trace_filter = 0;
	int64_t temp;
	for(int i = 0; i < _row_filter.GetAllScope().size(); i++){
		if(_row_filter.GetAllScope()[i].GetStopTrace() != -1){
			temp = _row_filter.GetAllScope()[i].GetStopTrace() - _row_filter.GetAllScope()[i].GetStartTrace() + 1;
		}
		else{
			temp = _meta->MetaGetheadnum() - _row_filter.GetAllScope()[i].GetStartTrace();
		}
		_num_of_trace_filter += temp;
	}
	//printf("have:%d trace in face\n",_num_of_trace_filter);
	return true;
}

bool HeadUpdaterNAS::Seek(int64_t head_num){
	if(head_num > _num_of_trace_filter - 1){
		//index out of range
		printf("trace index out of range!\n");
		return false;
	}
	int64_t head_pos = 0;
	int64_t count = 0;  //find actual tracenum by checking rowfilter
	int64_t cur_scope_num = 0;
	int64_t temp;  //record tracenum of one scope
	while((count <= head_num) && (cur_scope_num < _row_filter.GetAllScope().size())){
		if(_row_filter.GetAllScope()[cur_scope_num].GetStopTrace() != -1){
			temp = _row_filter.GetAllScope()[cur_scope_num].GetStopTrace() - _row_filter.GetAllScope()[cur_scope_num].GetStartTrace() + 1;
		}
		else{
			temp = head_num - _row_filter.GetAllScope()[cur_scope_num].GetStartTrace() + 1;
		}
		count += temp;
		cur_scope_num++;
	}
	cur_scope_num--;
	temp = (_row_filter.GetAllScope()[cur_scope_num].GetStopTrace() == -1) ? head_num : _row_filter.GetAllScope()[cur_scope_num].GetStopTrace();
	_actual_trace_num = temp - (count - head_num) + 1;
	head_pos = _actual_trace_num * _meta->MetaGetheadlength();
	_gf_head->Seek(head_pos, SEEK_SET);

	return true;
}

bool HeadUpdaterNAS::Put(const void* head){
	char *p = (char *)head;
	int64_t offset_in_head = 0;
	int64_t one_key_size = 0;
	int64_t cur_head_start_pos = _gf_head->GetPosition();
	for(auto i = _head_filter.GetKeyList().begin(); i != _head_filter.GetKeyList().end(); ++i){
		offset_in_head = 0;
		HeadType headtype = _meta->MetaGetheadtype();
		int64_t j = 0;
		while(j <= (*i) - 1){
			offset_in_head += headtype.head_size[j];   //find the position of the key
			j++;
		}
		one_key_size = headtype.head_size[j];
		//HeadUpdaterNAS::Seek(trace_num);
		_gf_head->Seek(cur_head_start_pos, SEEK_SET);
		_gf_head->Seek(offset_in_head, SEEK_CUR);  //seek to the key position
		//_gf_head->Read(p, one_key_size);
		//p[one_key_size] = '\0';
		_gf_head->Write(p, one_key_size);
		p += one_key_size;
	}

	return true;
}

bool HeadUpdaterNAS::Put(int64_t head_num, const void* head){
	if(HeadUpdaterNAS::Seek(head_num)){
		return HeadUpdaterNAS::Put(head);
	}
	else{
		return false;
	}
}

bool HeadUpdaterNAS::Sync(){
	_gf_head->Flush();
	_meta->MetaUpdate();
	return true;
}

bool HeadUpdaterNAS::Close(){
	_gf_head->Close();
	return true;
}


}

}
