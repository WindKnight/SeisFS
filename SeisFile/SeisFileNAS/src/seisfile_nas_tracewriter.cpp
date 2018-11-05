/*
 * seisfile_nas_writer.cpp
 *
 *  Created on: Aug 5, 2018
 *      Author: cssl
 */

#include "seisfile_nas_tracewriter.h"
#include <GEFile.h>
#include "seisfile_nas_meta.h"
#include <string>

namespace seisfs {

namespace file {

	TraceWriterNAS::~TraceWriterNAS(){
		delete _gf_trace;
	}

	/*TraceWriterNAS::TraceWriterNAS(const TraceType &trace_type,const std::string& filename, Lifetime lifetime_days) {

		seisnas_home_path_ = getenv("GEFILE_DISK_PATH");

		_trace_filename = seisnas_home_path_ + "/" + filename + "_trace";
		_meta_filename = seisnas_home_path_ + "/" + filename +"_meta";
		_filename = filename;
		_trace_type = trace_type;
		_lifetime_days = lifetime_days;
		TraceWriterNAS::Init();
	}*/

	TraceWriterNAS::TraceWriterNAS(const std::string& filename, MetaNAS *meta) {
		_meta = meta;
		/*if(_meta->MetafileExits()){
			_meta->MetaRead();
		}*/
		_trace_type = _meta->MetaGettracetype();

		seisnas_home_path_ = getenv("GEFILE_DISK_PATH");

		_trace_filename = seisnas_home_path_ + "/" + filename + "_trace";
		//_meta_filename = seisnas_home_path_ + "/" + filename +"_meta";
		_filename = filename;
		//_trace_type = trace_type;
		//_lifetime_days = lifetime_days;
		Init();
	}

	bool TraceWriterNAS::Init(){
		_gf_trace = new GEFile(_trace_filename);
		_gf_trace->SetProjectName(_trace_filename);
		//_meta = new MetaNAS(_meta_filename, _trace_type, _lifetime_days);
		//_meta->MetaRead();
		if(_gf_trace->Open(IO_WRITEONLY, true)){
			_gf_trace->SeekToEnd();
		}
		return true;
	}

	bool TraceWriterNAS::Write(const void* trace) {
		/*if(!_gf_trace->IsOpen()) {
			//file is not open
			return false;
		}
		_meta->MetaheadnumPlus();
		//_gf_trace->SeekToEnd();
		_gf_trace->Write(trace, _trace_type.trace_size);
		_meta->MetatracenumPlus();*/
		if(_gf_trace->Write(trace, _trace_type.trace_size)>0){
			_meta->MetatracenumPlus();
		}
		else{
			//error:fail to write
			return false;
		}

		return true;
	}

	int64_t TraceWriterNAS::Pos() {
		return _meta->MetaGetheadnum();
	}

	bool TraceWriterNAS::Sync() {
		_gf_trace->Flush();
		return true;
	}

	bool TraceWriterNAS::Close() {
		_meta->MetaUpdate();
		_gf_trace->Close();
		//delete _gf_trace;
		//delete _meta;
		return true;
	}

	bool TraceWriterNAS::Truncate(int64_t trace_num) {
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
