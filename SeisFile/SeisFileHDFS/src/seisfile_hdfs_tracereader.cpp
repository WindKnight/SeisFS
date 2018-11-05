/*
 * seisfile_hdfs_tracereader.cpp
 *
 *  Created on: Jun 26, 2018
 *      Author: wyd
 */

#include "seisfile_hdfs_tracereader.h"
#include <seisfile_hdfs_meta.h>

namespace seisfs {

namespace file {

TraceReaderHDFS::~TraceReaderHDFS(){
//	delete _gf_trace;
//	delete _gf_trace_next;
}

TraceReaderHDFS::TraceReaderHDFS(const std::string& filename, MetaHDFS *meta){
	_meta = meta;

	seisnas_home_path_ = getenv(GEFILE_ENV_NAME);

	_trace_filename = seisnas_home_path_ + "/" + filename + "_trace";
	_meta_filename = seisnas_home_path_ + "/" + filename +"_meta";
	_filename = filename;
	TraceReaderHDFS::Init();
}

bool TraceReaderHDFS::Init(){
	_cur_logic_num = 0;
	_last_actual_num = 0;

//	_gf_trace = new GEFile(_trace_filename);
//	_gf_trace->SetProjectName(_trace_filename);
//	_gf_trace_next = new GEFile(_trace_filename);
//	_gf_trace_next->SetProjectName(_trace_filename);
	_trace_type = _meta->MetaGettracetype();

	seisfs::RowScope init_row_scope;
	init_row_scope.SetStartTrace(0);
	init_row_scope.SetStopTrace(-1);
	seisfs::RowFilter init_row_filter(init_row_scope);
	TraceReaderHDFS::SetRowFilter(init_row_filter);

	fd_trace_ = open(_trace_filename.c_str(), O_RDONLY);
	if(0 > fd_trace_) {
		return false;
	}

	fd_trace_next_ = open(_trace_filename.c_str(), O_RDONLY);
	if(0 > fd_trace_next_) {
		return false;
	}

	return true;

//	if(_gf_trace->Open(IO_READONLY, true) && _gf_trace_next->Open(IO_READONLY, true)){
//		return true;
//	}
//	else{
//		//error:fail to open file
//		return false;
//	}
}

/*
 * caller should not delete the pointer of filter after calling this function
 */
bool TraceReaderHDFS::SetRowFilter(const RowFilter& row_filter) {
	_row_filter = row_filter;
	return true;
}

/**
 * Get the total trace rows of seis.
 */
int64_t TraceReaderHDFS::GetTraceNum() {
	int64_t tracenum = 0;
	int64_t temp = 0;
	for(int i = 0; i < _row_filter.GetAllScope().size(); i++){
		if(_row_filter.GetAllScope()[i].GetStopTrace() != -1){
			temp = _row_filter.GetAllScope()[i].GetStopTrace() - _row_filter.GetAllScope()[i].GetStartTrace() + 1;
		}
		else{
			temp = _meta->MetaGettracenum()- _row_filter.GetAllScope()[i].GetStartTrace();
		}
		tracenum += temp;
	}
	return tracenum;
}

/**
 * Get the trace length.
 */
int64_t TraceReaderHDFS::GetTraceSize() {
	_trace_type = _meta->MetaGettracetype();
	return _trace_type.trace_size;
}//return length of one trace, in bytes

/**
 * Move the read offset to trace_idx(th) trace.
 */
bool TraceReaderHDFS::Seek(int64_t trace_num) {
	_cur_logic_num = trace_num;
	int64_t trace_pos = 0;
	int64_t realnum = TraceReaderHDFS::GetRealNum(trace_num);
	if(realnum == _last_actual_num + 1){  //physically next to previous trace, needn't seek
		//printf("needn't seek\n");
		_last_actual_num = realnum;
		return true;
	}
	_last_actual_num = realnum;
	trace_pos = realnum * _meta->MetaGettracelength();
//	_gf_trace_next->Seek(trace_pos, SEEK_SET);
	lseek(fd_trace_next_, trace_pos, SEEK_SET);

	return true;
}

int64_t TraceReaderHDFS::Tell() {
	return _cur_logic_num;
}

/**
 * Get a trace by trace index.
 */
bool TraceReaderHDFS::Get(int64_t trace_num, void* trace) {
	if(trace_num >= _meta->MetaGettracenum()){
		std::cout<<"trace index out of range"<<endl;
		return false;
	}

	//read trace
	char *p1 = (char *)trace;
//	_gf_trace->Read(p1, _meta->MetaGettracelength());
	read(fd_trace_, p1, _meta->MetaGettracelength());
	p1[_meta->MetaGettracelength()]='\0';

	return true;
}

/**
 * Return true if there are more traces. The routine usually accompany with NextTrace.
 */
bool TraceReaderHDFS::HasNext() {
	if(TraceReaderHDFS::Tell() < TraceReaderHDFS::GetTraceNum()){ //modify equal
		return true;
	}
	return false;
}

/**
 * Get a trace by filter.
 */
bool TraceReaderHDFS::Next(void* trace) {
	if(TraceReaderHDFS::HasNext()){
		TraceReaderHDFS::Seek(_cur_logic_num);

		//read trace
		char *p3 = (char *)trace;
//		_gf_trace_next->Read(p3, _meta->MetaGettracelength());
		read(fd_trace_next_, p3, _meta->MetaGettracelength());
		p3[_meta->MetaGettracelength()]='\0';
		_cur_logic_num++;
		return true;

	}
	return false;
}


int64_t TraceReaderHDFS::GetRealNum(int64_t trace_num){
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

/**
 * Close file.
 */
bool TraceReaderHDFS::Close() {
//	_gf_trace->Close();
//	_gf_trace_next->Close();
	close(fd_trace_);
	close(fd_trace_next_);
	return true;
}


}
}


