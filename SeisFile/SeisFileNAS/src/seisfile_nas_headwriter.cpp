/*
 * seisfile_nas_writer.cpp
 *
 *  Created on: July 22, 2018
 *      Author: cssl
 */

#include "seisfile_nas_headwriter.h"
#include <GEFile.h>
#include "seisfile_nas_meta.h"
#include <string>

namespace seisfs {

namespace file {

	HeadWriterNAS::~HeadWriterNAS(){
		delete _gf_head;
	}

	/*HeadWriterNAS::HeadWriterNAS(const HeadType &head_type,const std::string& filename, Lifetime lifetime_days) {

		seisnas_home_path_ = getenv("GEFILE_DISK_PATH");

		_head_filename = seisnas_home_path_ + "/" + filename + "_head";
		_meta_filename = seisnas_home_path_ + "/" + filename +"_meta";
		_filename = filename;
		_head_type = head_type;
		_lifetime_days = lifetime_days;

		_head_length = 0;
		for(int i = 0; i < _head_type.head_size.size(); i++){
			_head_length +=_head_type.head_size[i];
		}

		HeadWriterNAS::Init();
	}*/

	HeadWriterNAS::HeadWriterNAS(const std::string& filename, MetaNAS *meta){
		_meta = meta;
		/*if(_meta->MetafileExits()){
			_meta->MetaRead();
		}*/

		_head_type = _meta->MetaGetheadtype();
		_head_length = _meta->MetaGetheadlength();

		seisnas_home_path_ = getenv("GEFILE_DISK_PATH");
		_head_filename = seisnas_home_path_ + "/" + filename + "_head";

		Init();
	}

	bool HeadWriterNAS::Init(){
		/*_gf_head = new GEFile(_head_filename);
		_gf_head->SetProjectName(_filename + "head");
		_meta = new MetaNAS(_meta_filename, _head_type, _lifetime_days);
		_meta->MetaRead();
		return _gf_head->Open(IO_WRITEONLY, true);*/
		_gf_head = new GEFile(_head_filename);
        _gf_head->SetProjectName(_head_filename);
		if(_gf_head->Open(IO_WRITEONLY, true)){
			_gf_head->SeekToEnd();
        }
		return true;
	}

	bool HeadWriterNAS::Write(const void* head) {
		if(_gf_head->Write(head, _head_length)>0){
			_meta->MetaheadnumPlus();
		}
		else{
			//error:fail to write
			return false;
		}
		return true;
	}

	int64_t HeadWriterNAS::Pos() {
		return _meta->MetaGetheadnum();
	}

	bool HeadWriterNAS::Sync() {
		_gf_head->Flush();
		return true;
	}

	bool HeadWriterNAS::Close() {
		_meta->MetaUpdate();
		_gf_head->Close();
		return true;
	}

	bool HeadWriterNAS::Truncate(int64_t trace_num) {
		if(trace_num <= _meta->MetaGettracenum() && trace_num >= 0){
			_meta->MetaSetheadnum(trace_num);
			return true;
		}
		else{
			//error:trace index out of range
			return false;
		}
		return true;
	}

}

}
