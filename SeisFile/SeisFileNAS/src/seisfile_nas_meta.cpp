/*
 * seisfile_nas_meta.cpp
 *
 *  Created on: July 31, 2018
 *      Author: cssl
 */

 #include "seisfile_nas_meta.h"
 #include <iostream>
 #include <string.h>
 #include <stdio.h>

 namespace seisfs {
	namespace file{

	MetaNAS::MetaNAS(const std::string &data_name){
		_filename = data_name;
		_meta = new Metastruct();
		//_head_type = new HeadType();
		/*//_trace_type = new TraceType();
		new HeadType(_head_type);
		new TraceType(_trace_type);*/
		//MetaNAS::Init();
	}

	void MetaNAS::Init(){
		_gf_meta = new GEFile(_filename);
		_gf_meta->SetProjectName(_filename);
	}

	void MetaNAS::CreateMetaFile(){
		_gf_meta->Open(IO_READWRITE, false);

		time_t temp_time;
		time(&temp_time);

		if(_gf_meta->GetLength() == 0){//file does not exit before
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

	bool MetaNAS::MetafileExits(){
		if(_gf_meta->GetLength() > 0){
			return true;
		}
		return false;
	}

	bool MetaNAS::MetaRead(){
		_gf_meta->Read(_meta, sizeof(Metastruct));
		//printf("keynum is:%u\n", _meta->keynum);
		uint16_t temp = 0;
		for(int i = 0; i < _meta->keynum; i++){
			_gf_meta->Read(&temp, sizeof(uint16_t));
			_head_type.head_size.push_back(temp);
		}
		/*for(int i = 0; i < _head_type.head_size.size(); i++){
			printf("%d ",_head_type.head_size[i]);
		}
		printf("\n");*/
		_gf_meta->Read(&(_trace_type.trace_size), sizeof(uint32_t));
		return true;
	}

	bool MetaNAS::MetaUpdate(){
		time_t temp_time;
		time(&temp_time);
		_meta->lastmodify_time = temp_time;

		_gf_meta->SeekToBegin();
		_gf_meta->Write(_meta,sizeof(Metastruct));
		for(int i = 0; i < _meta->keynum; i++){
			_gf_meta->Write(&(_head_type.head_size[i]), sizeof(uint16_t));
		}
		_gf_meta->Write(&(_trace_type.trace_size), sizeof(uint32_t));
		return true;
	}

	/*bool MetaNAS::MetaDelete(){
		return _gf_meta->Remove();
	}*/

	void MetaNAS::MetaheadnumPlus(){
		_meta->head_num++;
		//MetaNAS::MetaUpdate();
	}

	void MetaNAS::MetatracenumPlus(){
		_meta->trace_num++;
		//MetaNAS::MetaUpdate();
	}

	HeadType MetaNAS::MetaGetheadtype(){
		return _head_type;
	}

	TraceType MetaNAS::MetaGettracetype(){
		return _trace_type;
	}

	void MetaNAS::MetaSetheadtype(HeadType headtype){
		_head_type = headtype;
		_meta->keynum = _head_type.head_size.size();
		int64_t templength = 0;
		for(int i = 0; i < _meta->keynum; i++){
			templength += _head_type.head_size[i];
		}
		_meta->head_length = templength;
	}

	void MetaNAS::MetaSettracetype(TraceType tracetype){
		_trace_type = tracetype;
		_meta->trace_length = _trace_type.trace_size;
	}

	void MetaNAS::MetaSetheadnum(const uint32_t& headnum){
		_meta->head_num = headnum;
		MetaNAS::MetaUpdate();
	}

	void MetaNAS::MetaSettracenum(const uint32_t& tracenum){
		_meta->trace_num = tracenum;
		MetaNAS::MetaUpdate();
	}

	uint32_t MetaNAS::MetaGetheadnum(){
		return _meta->head_num;
	}

	uint32_t MetaNAS::MetaGettracenum(){
		return _meta->trace_num;
	}

	uint32_t MetaNAS::MetaGetheadlength(){
		return _meta->head_length;
	}

	uint32_t MetaNAS::MetaGettracelength(){
		return _trace_type.trace_size;
	}

	uint32_t MetaNAS::MetaGetkeynum(){
		return _meta->keynum;
	}

	time_t MetaNAS::MetaGetLastModify(){
		return _meta->lastmodify_time;
	}

	bool MetaNAS::MetaSetLifetime(Lifetime lifetime_days){
		_meta->lifetime_days = lifetime_days;
		MetaNAS::MetaUpdate();
		return true;
	}

	Lifetime MetaNAS::MetaGetLifetime(){
		return _meta->lifetime_days;
	}

	LifetimeStat MetaNAS::MetaGetLifetimestat(){
		return _meta->lifetimestat;
	}

	LifetimeStat MetaNAS::MetaSetLifetimestat(LifetimeStat lt_stat){
		_meta->lifetimestat = lt_stat;
		MetaNAS::MetaUpdate();
		return _meta->lifetimestat;
	}

	}
 }
