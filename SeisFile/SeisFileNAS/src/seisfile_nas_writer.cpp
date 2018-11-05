/*
 * seisfile_nas_writer.cpp
 *
 *  Created on: July 16, 2018
 *      Author: cssl
 */
 
 #include "seisfile_nas_writer.h"
 #include <string>
 #include <GEFile.h>
 #include <seisfile_nas_meta.h>
 #include <iostream>
 #include <cstring>
#include "util/seisfs_util_log.h"



 namespace seisfs {
	namespace file{

		/*WriterNAS::WriterNAS(const std::string& filename, const HeadType &head_type, const TraceType& trace_type, Lifetime lifetime_days){


			seisnas_home_path_ = getenv("GEFILE_DISK_PATH");

			_head_filename = seisnas_home_path_ + "/" + filename + "_head";
			_trace_filename = seisnas_home_path_ + "/" + filename + "_trace";
			_meta_filename = seisnas_home_path_ + "/" + filename +"_meta";
			_filename = filename;
			_head_type = head_type;
			_trace_type = trace_type;
			_lifetime_days = lifetime_days;

			_head_length = 0;
			for(int i = 0; i < _head_type.head_size.size(); i++){
				_head_length +=_head_type.head_size[i];
			}

			WriterNAS::Init();
		}*/

		WriterNAS::WriterNAS(const std::string& filename, MetaNAS *meta) {

			_meta = meta;
			/*if(_meta->MetafileExits()){
				_meta->MetaRead();
			}*/

			total_trace_num = _meta->MetaGettracenum();

			_head_type = _meta->MetaGetheadtype();
			_trace_type = _meta->MetaGettracetype();
			_head_length = _meta->MetaGetheadlength();
			/*for(int i = 0; i < _head_type.head_size.size(); i++){
				_head_length +=_head_type.head_size[i];
			}*/
			printf("head length is:%d\n",_head_length);

			seisnas_home_path_ = getenv("GEFILE_DISK_PATH");

			_head_filename = seisnas_home_path_ + "/" + filename + "_head";
			_trace_filename = seisnas_home_path_ + "/" + filename + "_trace";
			//_meta_filename = seisnas_home_path_ + "/" + filename +"_meta";

			//_meta = new MetaNAS(_meta_filename);
			//_meta->MetaRead();

			Init();

		}

		bool WriterNAS::Init() {
			/*_gf_head = new GEFile(_head_filename);
			_gf_trace = new GEFile(_trace_filename);
            _gf_head->SetProjectName(_filename + "head");
            _gf_trace->SetProjectName(_filename + "trace");
            _meta = new MetaNAS(_meta_filename, _head_type, _trace_type, _lifetime_days);
            if(_meta == NULL){
            	//fail to create or open _meta file
            }
            _meta->MetaRead();
            if(_gf_head->Open(IO_WRITEONLY, true) && _gf_trace->Open(IO_WRITEONLY, true)){
            	return true;
            }
            else{
            	//error:fail to open file
            	return false;
            }*/
			_gf_head = new GEFile(_head_filename);
			_gf_trace = new GEFile(_trace_filename);
            _gf_head->SetProjectName(_head_filename);
            _gf_trace->SetProjectName(_trace_filename);
			if(_gf_head->Open(IO_WRITEONLY, true) && _gf_trace->Open(IO_WRITEONLY, true)){
				_gf_head->SeekToEnd();
				_gf_trace->SeekToEnd();
				//printf("headfile pos is:%u\n",_gf_head->GetPosition());
            }
			return true;
		}

		WriterNAS::~WriterNAS(){
			delete _gf_head;
			delete _gf_trace;
			//delete _meta;
		}
		
		bool WriterNAS::Write(const void* head, const void* trace){

			//_gf_head->SeekToEnd(); //wyd

			//std::cout<<_gf_head->GetFileName()<<endl;
			//printf("head length is:%d\n",_head_length);
			if(_gf_head->Write(head, _head_length)>0){

				_meta->MetaheadnumPlus();
			}
			else{
				//error:fail to write

				return false;
			}

			//_gf_trace->SeekToEnd();  //wyd

			if(_gf_trace->Write(trace, _trace_type.trace_size)>0){

				_meta->MetatracenumPlus();
			}
			else{
				//error:fail to write

				return false;
			}

			//update meta after writing 10000 trace
			if(_meta->MetaGettracenum() - total_trace_num >= 10000){
				_meta->MetaUpdate();
				total_trace_num = _meta->MetaGettracenum();
			}

			return true;
		}
		
		int64_t WriterNAS::Pos(){
			return _meta->MetaGetheadnum()>_meta->MetaGetheadnum()?_meta->MetaGetheadnum():_meta->MetaGetheadnum();
		}
		
		bool WriterNAS::Sync(){
			_gf_head->Flush();
			_gf_trace->Flush();
			return true;
		}
		
		bool WriterNAS::Close(){
			_meta->MetaUpdate();
			_gf_head->Close();
			_gf_trace->Close();
			return true;
		}
		
		bool WriterNAS::Truncate(int64_t trace_num){
			if(trace_num <= _meta->MetaGettracenum() && trace_num >= 0){
				_meta->MetaSetheadnum(trace_num);
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
