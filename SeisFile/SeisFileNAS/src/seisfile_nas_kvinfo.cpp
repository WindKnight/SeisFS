/*
 * seisfile_nas_kvinfo.cpp
 *
 *  Created on: Jun 21, 2018
 *      Author: wyd
 */
#include "seisfile_nas_kvinfo.h"
#include "util/seisfs_util_log.h"
#include <cstring>

namespace seisfs {

namespace file {
	KVInfoNAS::~KVInfoNAS() {

	}

	KVInfoNAS::KVInfoNAS(const std::string& filename) {
		_filename = filename;

		seisnas_home_path_ = getenv("GEFILE_DISK_PATH");

		_kvinfo_filename = seisnas_home_path_ + "/" + filename + "_kvinfo";
		_key_filename = _kvinfo_filename + "_key";
		_value_filename = _kvinfo_filename + "_value";
		_meta_filename = _kvinfo_filename+"_meta";

		_gf_kvinfo_key = new GEFile(_key_filename);
		_gf_kvinfo_key->SetProjectName(_key_filename);
		_gf_kvinfo_value = new GEFile(_value_filename);
		_gf_kvinfo_value->SetProjectName(_value_filename);
		KVInfoNAS::Init();
	}

	bool KVInfoNAS::Init(){
		_gf_kvmeta = new GEFile(_meta_filename);
		_gf_kvmeta->SetProjectName(_meta_filename);

		_gf_kvmeta->Open(IO_READWRITE, false);
		_gf_kvinfo_key->Open(IO_READWRITE, true);
		_gf_kvinfo_value->Open(IO_READWRITE, true);

		if(_gf_kvmeta->GetLength() == 0){  //kv_info of the data first created
			char p[sizeof(uint32_t)];
			uint32_t num = 0;
			memcpy(p, &num, sizeof(uint32_t));
			_gf_kvmeta->Write(p, sizeof(uint32_t));  //to initialize keynum
			_gf_kvmeta->Write(p, sizeof(uint32_t));  //to initialize vailid keynum
		}
		else{  //initialize map
			KVInfoNAS::KV_MetaRead();
		}
		return true;
	}

	bool KVInfoNAS::Put(const std::string &key, const char* data, uint32_t size){
		map<string, string>::iterator it = _kv_map.find(key);
		if(it != _kv_map.end()){
			printf("the same key\n");
			return false;
		}
		char p1[key.length()+1];
		std::strcpy(p1, key.c_str());
		p1[key.length()] = '\0';
		_gf_kvinfo_key->SeekToEnd();
		_gf_kvinfo_key->Write(p1, key.length());
		_gf_kvinfo_value->SeekToEnd();
		_gf_kvinfo_value->Write(data, size);
		_kv_map.insert(map<string, string>::value_type(key, data));

		_gf_kvmeta->SeekToEnd();
		uint32_t key_pos = _gf_kvmeta->GetPosition();
		_kv_start_pos.insert(map<string, uint32_t>::value_type(key, key_pos));

		KVInfoNAS::KV_MetaWrite(key.length(), size);
		return true;
	}

	bool KVInfoNAS::Delete(const std::string &key){
		map<string, string>::iterator it;
		it = _kv_map.find(key);
		if(it == _kv_map.end()){
			printf("no such key\n");
			return false;
		}
		_kv_map.erase(it);

		/*
		 * to modify the key,value,and meta's valid bit,
		 * if achieve the invalid ratio set before, update meta file
		 */
		map<string, uint32_t>::iterator it_pos;
		it_pos = _kv_start_pos.find(key);
		int v = 0;
		char p[sizeof(int)];
		memcpy(p, &v, sizeof(int));

		uint32_t key_pos = it_pos->second;
		_gf_kvmeta->Seek(key_pos, SEEK_SET);
		_gf_kvmeta->Write(p, sizeof(int));

		KVInfoNAS::KV_GetCurrentMetaInfo();
		_valid_kvinfo_num--;
		KVInfoNAS::KV_UpdateCurrentMetaInfo();

		float judge = _valid_kvinfo_num / _kvinfo_num;
		if(judge < 0.8){
			KVInfoNAS::KV_Update();
		}

		return true;
	}

	bool KVInfoNAS::Get(const std::string &key, char*& data, uint32_t& size){
		map<string, string>::iterator it;
		it = _kv_map.find(key);
		if(it == _kv_map.end()){
			return false;
		}
		size = it->second.size();
		const char *p = new char[it->second.size() + 1];
		p = (it->second).c_str();
		data = new char[size+1];
		memcpy(data, p, size);
		data[size] = '\0';
		return true;
	}

	uint32_t KVInfoNAS::SizeOf(const std::string &key){
		map<string, string>::iterator it;
		it = _kv_map.find(key);
		if(it == _kv_map.end()){
			return 0;
		}
		return it->second.size();
	}

	bool KVInfoNAS::Exists(const std::string &key){
		map<string, string>::iterator it;
		it = _kv_map.find(key);
		if(it == _kv_map.end()){
			return false;
		}
		return true;
	}

	bool KVInfoNAS::GetKeys(std::vector<std::string> &Keys){
		map<string, string>::iterator it;
		for(it = _kv_map.begin(); it != _kv_map.end(); it++){
			Keys.push_back(it->first);
		}
		return true;
	}

	/*
	 *	meta structure
	 * 	uint32_t		uint32_t			(int		uint32_t		uint32_t)*  //appears _kvinfo_num times
	 * 	_kvinfo_num		_valid_kvinfo_num	valid		key_start_pos	value_start_pos
	 */

	void KVInfoNAS::KV_MetaRead(){
		KVInfoNAS::KV_GetCurrentMetaInfo();
		char p1[sizeof(int)], p2[sizeof(uint32_t)];
		int valid = 0;
		uint32_t keysize = 0, valuesize = 0;
		uint32_t key_pos = 0, value_pos = 0;
		uint32_t kv_start_pos = 0;			//record start position for modifying valid bit of that kv
		for(int i = 0; i < _kvinfo_num; i++){
			kv_start_pos = _gf_kvmeta->GetPosition();
			_gf_kvmeta->Read(p1, sizeof(int));			//valid bit
			memcpy(&valid, p1, sizeof(int));
			_gf_kvmeta->Read(p2, sizeof(uint32_t));		//key length
			memcpy(&keysize, p2, sizeof(uint32_t));
			_gf_kvmeta->Read(p2, sizeof(uint32_t));		//value length
			memcpy(&valuesize, p2, sizeof(uint32_t));
			/*if(valid == 1){
				_gf_kvinfo_key->Seek(key_pos, SEEK_SET);
				_gf_kvinfo_value->Seek(value_pos, SEEK_SET);
				char *keystring = new char[keysize];
				_gf_kvinfo_key->Read(keystring, keysize);
				keystring[keysize] = '\0';
				char *valuestring = new char[valuesize];
				valuestring[valuesize] = '\0';
				_gf_kvinfo_value->Read(valuestring, valuesize);
				_kv_map.insert(map<string,string>::value_type(keystring, valuestring));
			}*/
			////////////////////////////////////////////////////
			_gf_kvinfo_key->Seek(key_pos, SEEK_SET);
			_gf_kvinfo_value->Seek(value_pos, SEEK_SET);
			char keystring[keysize + 1];
			_gf_kvinfo_key->Read(keystring, keysize);
			keystring[keysize] = '\0';
			char valuestring[valuesize + 1];
			_gf_kvinfo_value->Read(valuestring, valuesize);
			valuestring[valuesize] = '\0';
			//_kv_start_pos.insert(pair<keystring, kv_start_pos>);
			_kv_start_pos.insert(map<string, uint32_t>::value_type(keystring, kv_start_pos));
			//printf("keystring is:%s,kv_start_pos is:%u\n",keystring, kv_start_pos);
			if(valid == 1){
				_kv_map.insert(map<string,string>::value_type(keystring, valuestring));
				//printf("keystring is:%s, valuestring is:%s\n",keystring,valuestring);
			}
			/////////////////////////////////////////////////

			key_pos += keysize;
			value_pos += valuesize;
		}
	}

	void KVInfoNAS::KV_MetaWrite(uint32_t key_length, uint32_t value_length){
		KVInfoNAS::KV_GetCurrentMetaInfo();
		_kvinfo_num++;
		_valid_kvinfo_num++;
		KVInfoNAS::KV_UpdateCurrentMetaInfo();

		char p[sizeof(uint32_t)], p1[sizeof(int)];
		int valid = 1;
		_gf_kvmeta->SeekToEnd();
		memcpy(p1, &valid, sizeof(int));
		_gf_kvmeta->Write(p1, sizeof(int));
		memcpy(p, &key_length, sizeof(uint32_t));
		_gf_kvmeta->Write(p, sizeof(uint32_t));
		memcpy(p, &value_length, sizeof(uint32_t));
		_gf_kvmeta->Write(p, sizeof(uint32_t));
	}

	void KVInfoNAS::KV_Update(){

		//copy three files
		std::string temp_key_filename = seisnas_home_path_ + "/" + _filename + "_keytemp";
		std::string temp_value_filename = seisnas_home_path_ + "/" + _filename + "_valuetemp";
		std::string temp_meta_filename = seisnas_home_path_ + "/" + _filename + "_metatemp";
		GEFile *key_temp = new GEFile(temp_key_filename);
		GEFile *value_temp = new GEFile(temp_value_filename);
		GEFile *meta_temp = new GEFile(temp_meta_filename);
		key_temp->Open(IO_READWRITE, false);
		value_temp->Open(IO_READWRITE, false);
		meta_temp->Open(IO_READWRITE, false);

		uint32_t length = 0;
		char p[1];

		length = _gf_kvinfo_key->GetLength();
		_gf_kvinfo_key->SeekToBegin();
		for(uint32_t i = 0; i < length; i++){
			_gf_kvinfo_key->Read(p, 1);
			key_temp->Write(p, 1);
		}

		length = _gf_kvinfo_value->GetLength();
		_gf_kvinfo_value->SeekToBegin();
		for(uint32_t i = 0; i < length; i++){
			_gf_kvinfo_value->Read(p, 1);
			value_temp->Write(p, 1);
		}

		length = _gf_kvmeta->GetLength();
		_gf_kvmeta->SeekToBegin();
		for(uint32_t i = 0; i < length; i++){
			_gf_kvmeta->Read(p, 1);
			meta_temp->Write(p, 1);
		}

		/*_gf_kvinfo_key->Close();
		_gf_kvinfo_value->Close();
		_gf_kvmeta->Close();*/
		_gf_kvinfo_key->Remove();
		_gf_kvinfo_value->Remove();
		_gf_kvmeta->Remove();
		delete _gf_kvinfo_key;
		delete _gf_kvinfo_value;
		delete _gf_kvmeta;

		/*std::string temp_key_filename = SEISNAS_HOME_PATH + _filename + "_keytemp";
		std::string temp_value_filename = SEISNAS_HOME_PATH + _filename + "_valuetemp";
		std::string temp_meta_filename = SEISNAS_HOME_PATH + _filename + "_metatemp";
		GEFile *key_temp = new GEFile(temp_key_filename);
		GEFile *value_temp = new GEFile(temp_value_filename);
		GEFile *meta_temp = new GEFile(temp_meta_filename);*/

		//create the three files again,update them
		_gf_kvinfo_key = new GEFile(_key_filename);
		_gf_kvinfo_key->SetProjectName(_key_filename);
		_gf_kvinfo_value = new GEFile(_value_filename);
		_gf_kvinfo_value->SetProjectName(_value_filename);
		_gf_kvmeta = new GEFile(_meta_filename);
		_gf_kvmeta->SetProjectName(_meta_filename);

		_gf_kvmeta->Open(IO_READWRITE, false);
		_gf_kvinfo_key->Open(IO_READWRITE, false);
		_gf_kvinfo_value->Open(IO_READWRITE, false);

		char p1[sizeof(uint32_t)], p2[sizeof(int)];
		uint32_t numofkv = _kv_map.size();

		memcpy(p1, &numofkv, sizeof(uint32_t));
		_gf_kvmeta->Write(p1, sizeof(uint32_t));		//_kvinfo_num
		_gf_kvmeta->Write(p1, sizeof(uint32_t));		//_valid_kvinfo_num

		_kv_start_pos.clear();
		map<string, string>::iterator it;

		uint32_t start_pos = 0;
		for(it = _kv_map.begin(); it != _kv_map.end(); it++){
			int valid = 1;
			uint32_t keysize = it->first.size(), valuesize = it->second.size();

			start_pos = _gf_kvmeta->GetPosition();
			_kv_start_pos.insert(map<string, uint32_t>::value_type(it->first, start_pos));

			//update temp meta file
			memcpy(p2, &valid, sizeof(int));
			_gf_kvmeta->Write(p2, sizeof(int));
			memcpy(p1, &keysize, sizeof(uint32_t));
			_gf_kvmeta->Write(p1, sizeof(uint32_t));
			memcpy(p1, &valuesize, sizeof(uint32_t));
			_gf_kvmeta->Write(p1, sizeof(uint32_t));

			//update temp key file
			char p3[it->first.size() + 1];
			memcpy(p3, (it->first).c_str(), it->first.size());
			p3[it->first.size()] = '\0';
			_gf_kvinfo_key->Write(p3, it->first.size());

			//update temp value file
			char p4[it->second.size() + 1];
			memcpy(p4, (it->second).c_str(), it->second.size());
			p4[it->second.size()] = '\0';
			_gf_kvinfo_value->Write(p4, it->second.size());
		}

		//update finished, delete three temp files
		/*key_temp->Close();
		value_temp->Close();
		meta_temp->Close();*/
		key_temp->Remove();
		value_temp->Remove();
		meta_temp->Remove();
		delete key_temp;
		delete value_temp;
		delete meta_temp;

	}

	void KVInfoNAS::KV_GetCurrentMetaInfo(){
		char p[sizeof(uint32_t)];
		_gf_kvmeta->SeekToBegin();
		_gf_kvmeta->Read(p, sizeof(uint32_t));
		memcpy(&_kvinfo_num, p, sizeof(uint32_t));
		_gf_kvmeta->Read(p, sizeof(uint32_t));
		memcpy(&_valid_kvinfo_num, p, sizeof(uint32_t));
	}

	void KVInfoNAS::KV_UpdateCurrentMetaInfo(){
		char p[sizeof(uint32_t)];
		_gf_kvmeta->SeekToBegin();
		memcpy(p, &_kvinfo_num, sizeof(uint32_t));
		_gf_kvmeta->Write(p, sizeof(uint32_t));
		memcpy(p, &_valid_kvinfo_num, sizeof(uint32_t));
		_gf_kvmeta->Write(p, sizeof(uint32_t));
	}


}
}
