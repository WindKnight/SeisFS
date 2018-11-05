/*
 * seisfile_hdfs_tracemerger.cpp
 *
 *  Created on: Sep 25, 2018
 *      Author: cssl
 */

#include "seisfile_hdfs_tracemerger.h"

namespace seisfs{

namespace file{

TraceMergerHDFS::TraceMergerHDFS(const std::string& des_dataname, const std::string& src_dataname){
	_des_filename = des_dataname;
	_src_filename = src_dataname;
	_des_trace_filename = _des_filename + "_trace";
	_src_trace_filename = _src_filename + "_trace";

	int pos = _des_filename.find_last_of('/');
	_des_path = _des_filename.substr(0, pos);
	_des_dataname = _des_filename.substr(pos + 1);
	pos = _src_filename.find_last_of('/');
	_src_dataname = _src_filename.substr(pos + 1);

	Init();

}

TraceMergerHDFS::~TraceMergerHDFS(){
	delete _des_meta;
	delete _src_meta;
//	delete _gf_des;
//	delete _gf_src;
}

bool TraceMergerHDFS::Init(){
	if(!GEFile::Exists(_des_trace_filename) || !GEFile::Exists(_src_trace_filename)){
		//error:data does not exit
		return false;
	}
	_des_meta = new MetaHDFS(_des_filename + "_meta");
	_src_meta = new MetaHDFS(_src_filename + "_meta");

	return _des_meta->MetaRead() && _src_meta->MetaRead();
}

bool TraceMergerHDFS::Merge(){
#if 0

	_gf_des = new GEFile(_des_trace_filename);
	_gf_src = new GEFile(_src_trace_filename);
	if(!GEFile::IsSplitFile(_des_trace_filename)){
		return false;
	}
	else{
		if(GEFile::IsSplitFile(_src_trace_filename)){
			Rename(0);
		}
		else{
			Rename(1);
		}
	}

	return MergeMeta();
#endif
	return true;
}

bool TraceMergerHDFS::Rename(int n){
#if 0
	GEFileIndex _index_des = _gf_des->Getindexfile();

	std::string new_name, former_name;
	char ext[16];
	int cur_index = _index_des.GetFileSplitNum();
	if(n == 0){
		GEFileIndex _index_src = _gf_src->Getindexfile();
		for(int i = 0; i < _index_src.GetFileSplitNum(); i++){
			former_name = _index_src.GetFileNameInd(i);
			sprintf (ext, ".part%03d", cur_index);
			new_name = _des_path + "/" + _des_dataname + "_trace" + ext;
			if(0 > rename(former_name.c_str(), new_name.c_str())){
				return false;
			}
			_index_des.SetFileNameInd(cur_index, new_name);
			_index_des.Serialize();
			cur_index++;
		}
	}
	if(n == 1){
		sprintf (ext, ".part%03d", cur_index);
		new_name = _des_path + "/" + _des_dataname + "_trace" + ext;
		if(0 > rename(new_name.c_str(), _src_trace_filename.c_str())){
			return false;
		}
		_index_des.SetFileNameInd(cur_index, new_name);
		_index_des.Serialize();
	}
	return true;
#endif
	return true;
}

bool TraceMergerHDFS::MergeMeta(){
	int64_t total_tracenum = _des_meta->MetaGettracenum() + _src_meta->MetaGettracenum();
	_des_meta->MetaSettracenum(total_tracenum);
	_des_meta->MetaUpdate();
	return true;
}

}

}
