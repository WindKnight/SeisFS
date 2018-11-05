/*
 * seisfile_hdfs_meta.cpp
 *
 *  Created on: July 31, 2018
 *      Author: cssl
 */

 #include "seisfile_hdfs_meta.h"
 #include <iostream>
 #include <string.h>
 #include <stdio.h>

 namespace seisfs {
	namespace file{

	MetaHDFS::MetaHDFS(const std::string &data_name){
		_filename = data_name;
		_meta = new MetastructHDFS();
		//_head_type = new HeadType();
		/*//_trace_type = new TraceType();
		new HeadType(_head_type);
		new TraceType(_trace_type);*/
		//MetaHDFS::Init();
	}

	void MetaHDFS::Init(){
//		_gf_meta = new GEFile(_filename);
//		_gf_meta->SetProjectName(_filename);
	}

	void MetaHDFS::CreateMetaFile(){
//		_gf_meta->Open(IO_READWRITE, false);

		time_t temp_time;
		time(&temp_time);

		int ret = access(_filename.c_str(), F_OK | R_OK | W_OK);
		if(0 == ret) {
//		if(_gf_meta->GetLength() == 0){//file does not exit before
			_meta->version = 0;
			_meta->head_num = 0;
			_meta->trace_num = 0;
			_meta->lifetimestat = NORMAL;
			//time(&(_meta->creat_time));
			//_meta->lastmodify_time = 0;
			_meta->creat_time = temp_time;
		}
		else{
			MetaRead();
		}
		_meta->lastaccess_time = temp_time;
		_meta->lastmodify_time = temp_time;
	}

	bool MetaHDFS::MetafileExits(){
//		if(_gf_meta->GetLength() > 0){
		if(0 == access(_filename.c_str(), F_OK | R_OK | W_OK)) {
			return true;
		}
		return false;
	}

	bool MetaHDFS::MetaRead(){
		int fd_meta = open(_filename.c_str(), O_RDONLY);
		read(fd_meta, _meta, sizeof(MetastructHDFS));
//		_gf_meta->Read(_meta, sizeof(MetastructHDFS));
		//printf("keynum is:%u\n", _meta->keynum);
		uint16_t temp = 0;
		for(int i = 0; i < _meta->keynum; i++){
//			_gf_meta->Read(&temp, sizeof(uint16_t));
			read(fd_meta, &temp, sizeof(uint16_t));
			_head_type.head_size.push_back(temp);
		}
		/*for(int i = 0; i < _head_type.head_size.size(); i++){
			printf("%d ",_head_type.head_size[i]);
		}
		printf("\n");*/
//		_gf_meta->Read(&(_trace_type.trace_size), sizeof(uint32_t));
		read(fd_meta, &(_trace_type.trace_size), sizeof(uint32_t));
		return true;
	}

	bool MetaHDFS::MetaUpdate(){
		int fd_meta = open(_filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
		time_t temp_time;
		time(&temp_time);
		_meta->lastmodify_time = temp_time;

//		_gf_meta->SeekToBegin();
//		_gf_meta->Write(_meta,sizeof(MetastructHDFS));
		write(fd_meta, _meta, sizeof(MetastructHDFS));
		for(int i = 0; i < _meta->keynum; i++){
//			_gf_meta->Write(&(_head_type.head_size[i]), sizeof(uint16_t));
			write(fd_meta, &(_head_type.head_size[i]), sizeof(uint16_t));
		}
//		_gf_meta->Write(&(_trace_type.trace_size), sizeof(uint32_t));
		write(fd_meta, &(_trace_type.trace_size), sizeof(uint32_t));
		return true;
	}

	/*bool MetaHDFS::MetaDelete(){
		return _gf_meta->Remove();
	}*/

	void MetaHDFS::MetaheadnumPlus(){
		_meta->head_num++;
		//MetaHDFS::MetaUpdate();
	}

	void MetaHDFS::MetatracenumPlus(){
		_meta->trace_num++;
		//MetaHDFS::MetaUpdate();
	}

	HeadType MetaHDFS::MetaGetheadtype(){
		return _head_type;
	}

	TraceType MetaHDFS::MetaGettracetype(){
		return _trace_type;
	}

	void MetaHDFS::MetaSetheadtype(HeadType headtype){
		_head_type = headtype;
		_meta->keynum = _head_type.head_size.size();
		int64_t templength = 0;
		for(int i = 0; i < _meta->keynum; i++){
			templength += _head_type.head_size[i];
		}
		_meta->head_length = templength;
	}

	void MetaHDFS::MetaSettracetype(TraceType tracetype){
		_trace_type = tracetype;
		_meta->trace_length = _trace_type.trace_size;
	}

	void MetaHDFS::MetaSetheadnum(const uint32_t& headnum){
		_meta->head_num = headnum;
		MetaHDFS::MetaUpdate();
	}

	void MetaHDFS::MetaSettracenum(const uint32_t& tracenum){
		_meta->trace_num = tracenum;
		MetaHDFS::MetaUpdate();
	}

	uint32_t MetaHDFS::MetaGetheadnum(){
		return _meta->head_num;
	}

	uint32_t MetaHDFS::MetaGettracenum(){
		return _meta->trace_num;
	}

	uint32_t MetaHDFS::MetaGetheadlength(){
		return _meta->head_length;
	}

	uint32_t MetaHDFS::MetaGettracelength(){
		return _trace_type.trace_size;
	}

	uint32_t MetaHDFS::MetaGetkeynum(){
		return _meta->keynum;
	}

	time_t MetaHDFS::MetaGetLastModify(){
		return _meta->lastmodify_time;
	}

	bool MetaHDFS::MetaSetLifetime(Lifetime lifetime_days){
		_meta->lifetime_days = lifetime_days;
		MetaHDFS::MetaUpdate();
		return true;
	}

	Lifetime MetaHDFS::MetaGetLifetime(){
		return _meta->lifetime_days;
	}

	LifetimeStat MetaHDFS::MetaGetLifetimestat(){
		return _meta->lifetimestat;
	}

	LifetimeStat MetaHDFS::MetaSetLifetimestat(LifetimeStat lt_stat){
		_meta->lifetimestat = lt_stat;
		MetaHDFS::MetaUpdate();
		return _meta->lifetimestat;
	}

	}
 }
