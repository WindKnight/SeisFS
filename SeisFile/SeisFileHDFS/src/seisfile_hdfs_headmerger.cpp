/*
 * seisfile_hdfs_headmerger.cpp
 *
 *  Created on: Sep 21, 2018
 *      Author: cssl
 */

#include "seisfile_hdfs_headmerger.h"

namespace seisfs{

namespace file{

HeadMergerHDFS::HeadMergerHDFS(const std::string& des_dataname, const std::string& src_dataname){
	_des_filename = des_dataname;
	_src_filename = src_dataname;
	_des_head_filename = _des_filename + "_head";
	_src_head_filename = _src_filename + "_head";

	int pos = _des_filename.find_last_of('/');
	_des_path = _des_filename.substr(0, pos);
	_des_dataname = _des_filename.substr(pos + 1);
	pos = _src_filename.find_last_of('/');
	_src_dataname = _src_filename.substr(pos + 1);

	Init();

}

HeadMergerHDFS::~HeadMergerHDFS(){
	delete _des_meta;
	delete _src_meta;
//	delete _gf_des;
//	delete _gf_src;
}

bool HeadMergerHDFS::Init(){
	if(!GEFile::Exists(_des_head_filename) || !GEFile::Exists(_src_head_filename)){
		//error:data does not exit
		return false;
	}
	_des_meta = new MetaHDFS(_des_filename + "_meta");
	_src_meta = new MetaHDFS(_src_filename + "_meta");

	return _des_meta->MetaRead() && _src_meta->MetaRead();
}

bool HeadMergerHDFS::Merge(){
#if 0
	_gf_des = new GEFile(_des_head_filename);
	_gf_src = new GEFile(_src_head_filename);
	if(!GEFile::IsSplitFile(_des_head_filename)){
		return false;
	}
	else{
		if(GEFile::IsSplitFile(_src_head_filename)){
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

bool HeadMergerHDFS::Rename(int n){
#if 0
	GEFileIndex _index_des = _gf_des->Getindexfile();

	std::string new_name, former_name;
	char ext[16];
	int cur_index = _index_des.GetFileSplitNum();
	if(n == 0){
		GEFileIndex _index_src = _gf_src->Getindexfile();
		for(int i = 0; i < _index_src.GetFileSplitNum(); i++){
			former_name = _index_src.GetFileNameInd(i);
			//cout<<former_name<<endl;
			sprintf (ext, ".part%03d", cur_index);
			new_name = _des_path + "/" + _des_dataname + "_head" + ext;
			if(0 > rename(former_name.c_str(), new_name.c_str())){
				return false;
			}
			//cout<<new_name<<endl;
			_index_des.SetFileNameInd(cur_index, new_name);
			_index_des.Serialize();
			cur_index++;
		}
	}
	if(n == 1){
		sprintf (ext, ".part%03d", cur_index);
		new_name = _des_path + "/" + _des_dataname + "_head" + ext;
		if(0 > rename(new_name.c_str(), _src_head_filename.c_str())){
			return false;
		}
		_index_des.SetFileNameInd(cur_index, new_name);
		_index_des.Serialize();
	}
	return true;
#endif
	return true;
}

bool HeadMergerHDFS::MergeMeta(){
	int64_t total_headnum = _des_meta->MetaGetheadnum() + _src_meta->MetaGetheadnum();
	_des_meta->MetaSetheadnum(total_headnum);
	_des_meta->MetaUpdate();
	return true;
}

}

}
