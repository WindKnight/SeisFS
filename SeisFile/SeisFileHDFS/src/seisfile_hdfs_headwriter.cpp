/*
 * seisfile_hdfs_writer.cpp
 *
 *  Created on: July 22, 2018
 *      Author: cssl
 */

#include "seisfile_hdfs_headwriter.h"
#include <GEFile.h>
#include "seisfile_hdfs_meta.h"
#include <string>

namespace seisfs {

namespace file {

	HeadWriterHDFS::~HeadWriterHDFS(){
//		delete _gf_head;
	}

	/*HeadWriterHDFS::HeadWriterHDFS(const HeadType &head_type,const std::string& filename, Lifetime lifetime_days) {

		seisnas_home_path_ = getenv(GEFILE_ENV_NAME);

		_head_filename = seisnas_home_path_ + "/" + filename + "_head";
		_meta_filename = seisnas_home_path_ + "/" + filename +"_meta";
		_filename = filename;
		_head_type = head_type;
		_lifetime_days = lifetime_days;

		_head_length = 0;
		for(int i = 0; i < _head_type.head_size.size(); i++){
			_head_length +=_head_type.head_size[i];
		}

		HeadWriterHDFS::Init();
	}*/

	HeadWriterHDFS::HeadWriterHDFS(const std::string& filename, MetaHDFS *meta){
		_meta = meta;
		/*if(_meta->MetafileExits()){
			_meta->MetaRead();
		}*/

		_head_type = _meta->MetaGetheadtype();
		_head_length = _meta->MetaGetheadlength();

		seisnas_home_path_ = getenv(GEFILE_ENV_NAME);
		_head_filename = seisnas_home_path_ + "/" + filename + "_head";

		Init();
	}

	bool HeadWriterHDFS::Init(){
		/*_gf_head = new GEFile(_head_filename);
		_gf_head->SetProjectName(_filename + "head");
		_meta = new MetaHDFS(_meta_filename, _head_type, _lifetime_days);
		_meta->MetaRead();
		return _gf_head->Open(IO_WRITEONLY, true);*/
//		_gf_head = new GEFile(_head_filename);
//        _gf_head->SetProjectName(_head_filename);
//		if(_gf_head->Open(IO_WRITEONLY, true)){
//			_gf_head->SeekToEnd();
//        }

		fd_head_ = open(_head_filename.c_str(), O_WRONLY | O_CREAT , 0644);
		lseek(fd_head_, 0, SEEK_END);
		return true;
	}

	bool HeadWriterHDFS::Write(const void* head) {
//		if(_gf_head->Write(head, _head_length)>0){
		if(_head_length == write(fd_head_, head, _head_length)) {
			_meta->MetaheadnumPlus();
		}
		else{
			//error:fail to write
			return false;
		}
		return true;
	}

	int64_t HeadWriterHDFS::Pos() {
		return _meta->MetaGetheadnum();
	}

	bool HeadWriterHDFS::Sync() {
//		_gf_head->Flush();
		fsync(fd_head_);
		return true;
	}

	bool HeadWriterHDFS::Close() {
		_meta->MetaUpdate();
//		_gf_head->Close();
		close(fd_head_);
		return true;
	}

	bool HeadWriterHDFS::Truncate(int64_t trace_num) {
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
