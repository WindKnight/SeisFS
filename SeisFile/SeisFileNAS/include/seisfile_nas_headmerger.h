/*
 * seisfile_nas_headmerger.h
 *
 *  Created on: Sep 21, 2018
 *      Author: cssl
 */

#include "GEFile.h"
#include "GEIdxFile.h"
#include "seisfile_nas_meta.h"

namespace seisfs{

namespace file{

class MetaNAS;

class HeadMergerNAS{

public:

	HeadMergerNAS(const std::string& des_dataname, const std::string& src_dataname);

	~HeadMergerNAS();

	bool Merge();


private:

	bool Init();
	bool Rename(int n);
	bool MergeMeta();

	std::string _des_filename, _src_filename;
	std::string _des_head_filename, _src_head_filename;
	std::string _des_path;
	std::string _des_dataname, _src_dataname;

	GEFile *_gf_des, *_gf_src;
	MetaNAS *_des_meta, *_src_meta;

};

}

}
