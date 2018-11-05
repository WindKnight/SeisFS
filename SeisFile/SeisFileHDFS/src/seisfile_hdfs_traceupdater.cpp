/*
 * seisfile_hdfs_traceupdater.cpp
 *
 *  Created on: Aug 13, 2018
 *      Author: cssl
 */

#include "seisfile_hdfs_traceupdater.h"
#include <stdio.h>
#include <string.h>

namespace seisfs{
namespace file{

TraceUpdaterHDFS::TraceUpdaterHDFS(const std::string& filename){

	seisnas_home_path_ = getenv(GEFILE_ENV_NAME);

	_trace_filename = seisnas_home_path_ + "/" + filename + "_trace";
	_meta_filename = seisnas_home_path_ + "/" + filename +"_meta";
	_filename = filename;
	TraceUpdaterHDFS::Init();
}

bool TraceUpdaterHDFS::Init(){
//	_gf_trace = new GEFile(_trace_filename);
//	_gf_trace->SetProjectName(_filename + "trace");
	fd_trace_ = open(_trace_filename.c_str(), O_WRONLY | O_CREAT, 0644);
	if(0 > fd_trace_)
		return false;
	_meta = new MetaHDFS(_meta_filename);
	_meta->MetaRead();
	return true;
//	return _gf_trace->Open(IO_READWRITE, true);
}

bool TraceUpdaterHDFS::SetRowFilter(const RowFilter& filter){
	_row_filter = filter;
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
	return true;
}

bool TraceUpdaterHDFS::Seek(int64_t trace_num){
	if(trace_num > _num_of_trace_filter - 1){
		//error:index out of range
		printf("trace index out of range!\n");
		return false;
	}
	int64_t trace_pos = 0;
	int64_t count = 0;  //find actual tracenum by checking rowfilter
	int64_t cur_scope_num = 0;
	int64_t temp;  //record tracenum of one scope
	int64_t final_stop_trace_num = _meta->MetaGetheadnum()>_meta->MetaGetheadnum()?_meta->MetaGetheadnum():_meta->MetaGetheadnum() - 1;
	while((count <= trace_num) && (cur_scope_num < _row_filter.GetAllScope().size())){
		if(_row_filter.GetAllScope()[cur_scope_num].GetStopTrace() != -1){
			temp = _row_filter.GetAllScope()[cur_scope_num].GetStopTrace() - _row_filter.GetAllScope()[cur_scope_num].GetStartTrace() + 1;
		}
		else{
			//final_stop_trace_num = _meta->MetaGetheadnum()>_meta->MetaGetheadnum()?_meta->MetaGetheadnum():_meta->MetaGetheadnum() - 1;
			//temp = trace_num - _row_filter.GetAllScope()[cur_scope_num].GetStartTrace() + 1;
			temp = final_stop_trace_num - _row_filter.GetAllScope()[cur_scope_num].GetStartTrace() + 1;
		}
		count += temp;
		cur_scope_num++;
	}
	cur_scope_num--;
	//temp = (_row_filter.GetAllScope()[cur_scope_num].GetStopTrace() == -1) ? trace_num : _row_filter.GetAllScope()[cur_scope_num].GetStopTrace();
	temp = (_row_filter.GetAllScope()[cur_scope_num].GetStopTrace() == -1) ? final_stop_trace_num : _row_filter.GetAllScope()[cur_scope_num].GetStopTrace();
	_actual_trace_num = temp - (count - trace_num) + 1;
	printf("======acutal trace num is:%d======\n",_actual_trace_num);
	trace_pos = _actual_trace_num * _meta->MetaGettracelength();
//	_gf_trace->Seek(trace_pos, SEEK_SET);
	lseek(fd_trace_, trace_pos, SEEK_SET);

	return true;
}

bool TraceUpdaterHDFS::Put(const void* trace){
	/*if(strlen((const char *)trace) != _meta->MetaGettracelength()){
		//wrong trace length
		return false;
	}*/
//	_gf_trace->Write(trace, _meta->MetaGettracelength());
	write(fd_trace_, trace, _meta->MetaGettracelength());
	_meta->MetaUpdate();
	return true;
}

bool TraceUpdaterHDFS::Put(int64_t trace_num, const void* trace){
	if(TraceUpdaterHDFS::Seek(trace_num)){
		TraceUpdaterHDFS::Put(trace);
	}
	else{
		return false;
	}
}

bool TraceUpdaterHDFS::Sync(){
//	_gf_trace->Flush();
	fsync(fd_trace_);
	return true;
}

bool TraceUpdaterHDFS::Close(){
//	_gf_trace->Close();
//	delete _gf_trace;
	close(fd_trace_);

	delete _meta;
	return true;
}

}
}
