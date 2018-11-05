/*
 * test.cpp
 *
 *  Created on: Apr 6, 2018
 *      Author: wyd
 */


#include "seisfs_config.h"
#include "seisfile_hdfs.h"
#include "seisfile_hdfs_reader.h"
#include <stdio.h>


int main() {

	seisfs::file::SeisFileHDFS *sf_hdfs = new seisfs::file::SeisFileHDFS("tmp");
	sf_hdfs->Init();
	seisfs::file::ReaderHDFS *reader_hdfs = (seisfs::file::ReaderHDFS *)sf_hdfs->OpenReader();
	std::vector<int64_t> traces;
	traces.push_back(123);
	traces.push_back(123);
	traces.push_back(123);

	std::vector<std::vector<std::string> > head_hostInfos;
	std::vector<std::vector<std::string> > trace_hostInfos;

	reader_hdfs->GetLocations(traces, head_hostInfos, trace_hostInfos);
	for(int trace_i = 0; trace_i < head_hostInfos.size(); ++trace_i) {
		printf("trace : %d, valid hosts : \n", traces[trace_i]);
		for(int host_i = 0; host_i < head_hostInfos[trace_i].size(); ++host_i) {
			printf("%s\n", head_hostInfos[trace_i][host_i].c_str());
		}
	}


	return 0;
}




