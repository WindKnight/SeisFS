/*
 * test.cpp
 *
 *  Created on: Apr 6, 2018
 *      Author: wyd
 */


#include "seisfs_config.h"
#include "seisfile.h"
#include "seisfile_kvinfo.h"
#include <stdio.h>


int main() {

	seisfs::file::SeisFile *seis_file = seisfs::file::SeisFile::New("tmp", seisfs::STABLE);
	seisfs::file::KVInfo *kv_info  = seis_file->OpenKVInfo();
	kv_info->Put("123", "123", 3);


	return 0;
}



