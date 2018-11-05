/*
 * seisfile_hdfs_writer.cpp
 *
 *  Created on: Aug 5, 2018
 *      Author: cssl
 */

#include "seisfile_hdfs_tracewriter.h"
#include <GEFile.h>
#include "seisfile_hdfs_meta.h"
#include <string>

namespace seisfs {

namespace file {

	TraceWriterHDFS::~TraceWriterHDFS(){
//		delete _gf_trace;
	}

	/*TraceWriterHDFS::TraceWriterHDFS(const TraceType &trace_type,const std::string& filename, Lifetime lifetime_days) {

		seisnas_home_path_ = getenv(GEFILE_ENV_NAME);

		_trace_filename = seisnas_home_path_ + "/" + filename + "_trace";
		_meta_filename = seisnas_home_path_ + "/" + filename +"_meta";
		_filename = filename;
		_trace_type = trace_type;
		_lifetime_days = lifetime_days;
		TraceWriterHDFS::Init();
	}*/

	TraceWriterHDFS::TraceWriterHDFS(const std::string& filename, MetaHDFS *meta) {
		_meta = meta;
		/*if(_meta->MetafileExits()){
			_meta->MetaRead();
		}*/
		_trace_type = _meta->MetaGettracetype();

		seisnas_home_path_ = getenv(GEFILE_ENV_NAME);

		_trace_filename = seisnas_home_path_ + "/" + filename + "_trace";
		//_meta_filename = seisnas_home_path_ + "/" + filename +"_meta";
		_filename = filename;
		//_trace_type = trace_type;
		//_lifetime_days = lifetime_days;
		Init();
	}

	bool TraceWriterHDFS::Init(){
//		_gf_trace = new GEFile(_trace_filename);
//		_gf_trace->SetProjectName(_trace_filename);
//		//_meta = new MetaHDFS(_meta_filename, _trace_type, _lifetime_days);
//		//_meta->MetaRead();
//		if(_gf_trace->Open(IO_WRITEONLY, true)){
//			_gf_trace->SeekToEnd();
//		}


		fd_trace_ = open(_trace_filename.c_str(), O_WRONLY | O_CREAT, 0644);
		if(0 > fd_trace_) {
			return false;
		}
		return true;
	}

	bool TraceWriterHDFS::Write(const void* trace) {
		/*if(!_gf_trace->IsOpen()) {
			//file is not open
			return false;
		}
		_meta->MetaheadnumPlus();
		//_gf_trace->SeekToEnd();
		_gf_trace->Write(trace, _trace_type.trace_size);
		_meta->MetatracenumPlus();*/
//		if(_gf_trace->Write(trace, _trace_type.trace_size)>0){
		if(_trace_type.trace_size == write(fd_trace_, trace, _trace_type.trace_size)) {
			_meta->MetatracenumPlus();
		}
		else{
			//error:fail to write
			return false;
		}

		return true;
	}

	int64_t TraceWriterHDFS::Pos() {
		return _meta->MetaGetheadnum();
	}

	bool TraceWriterHDFS::Sync() {
//		_gf_trace->Flush();
		fsync(fd_trace_);
		return true;
	}

	bool TraceWriterHDFS::Close() {
		_meta->MetaUpdate();
//		_gf_trace->Close();
		close(fd_trace_);
		//delete _gf_trace;
		//delete _meta;
		return true;
	}

	bool TraceWriterHDFS::Truncate(int64_t trace_num) {
		/*_meta->MetaSetheadnum(trace_num);
		return true;*/
		if(trace_num <= _meta->MetaGettracenum() && trace_num >= 0){
			_meta->MetaSettracenum(trace_num);
			return true;
		}
		else{
			//error:trace index out of range
			return false;
		}
	}

}

}
