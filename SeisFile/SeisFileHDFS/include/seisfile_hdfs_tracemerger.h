/*
 * seisfile_hdfs_tracemerger.h
 *
 *  Created on: Sep 25, 2018
 *      Author: cssl
 */

#include "GEFile.h"
#include "GEIdxFile.h"
#include "seisfile_hdfs_meta.h"

namespace seisfs{

namespace file{

class MetaHDFS;

class TraceMergerHDFS{

public:

	TraceMergerHDFS(const std::string& des_dataname, const std::string& src_dataname);

	~TraceMergerHDFS();

	bool Merge();


private:

	bool Init();
	bool Rename(int n);
	bool MergeMeta();

	std::string _des_filename, _src_filename;
	std::string _des_trace_filename, _src_trace_filename;
	std::string _des_path;
	std::string _des_dataname, _src_dataname;

//	GEFile *_gf_des, *_gf_src;
	int fd_des_, fd_src_;
	MetaHDFS *_des_meta, *_src_meta;

};

}

}
