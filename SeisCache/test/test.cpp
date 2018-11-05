/*
 * test.cpp
 *
 *  Created on: Apr 13, 2018
 *      Author: wyd
 */

#include "seiscache_data.h"
#include "seisfs_row_filter.h"
#include "seiscache_reader.h"
#include "seiscache_head_reader.h"
#include "seiscache_trace_reader.h"
#include "seiscache_speed_reader.h"
#include "seiscache_head_speed_reader.h"
#include "seiscache_trace_speed_reader.h"
#include "seiscache_updater.h"
#include <stdio.h>
#include <stdlib.h>
#include "util/seisfs_util_log.h"
#include "util/seisfs_scoped_pointer.h"
//#include "gcache_seis_data.h"
#include "seiscache_error.h"
#include "seiscache_writer.h"
#include "seiscache_head_writer.h"
#include "seiscache_trace_writer.h"
#include "seiscache_reader.h"
#include "seiscache_kv_info.h"
#include "util/seisfs_util_string.h"

#define PRINT_ERR fprintf(stderr, "errno = %d, error is : %s file : %s, line : %d\n", \
		seisfs::cache::sc_errno, seisfs::cache::SCStrerror(seisfs::cache::sc_errno).c_str(),__FILE__, __LINE__);fflush(stderr);


bool TestWrite();
bool TestRead();
bool TestUpdate();
bool TestKVInfo();
bool TestRename();
bool TestCopy();
bool TestMerge();



int main(int argc, char **argv) {

	int test_round = 1;
	if(argc >= 2) {
		test_round = ToInt(argv[1]);
	}

	for(int i = 0; i < test_round; ++i) {
		if(!TestWrite()) {
			printf("Test Write error\n");
			return -1;
		}


//		if(!TestRead()) {
//			printf("Test Read error\n");
//			return -1;
//		}
//
//		if(!TestUpdate()) {
//			printf("Test Update error\n");
//			return -1;
//		}
//
//		if(!TestKVInfo()) {
//			printf("Test KVInfo error\n");
//			return -1;
//		}
////
//		if(!TestCopy()) {
//			printf("Test Copy error\n");
//			return -1;
//		}
//		if(!TestRename()) {
//			printf("Test Rename error\n");
//			return -1;
//		}

//		if(!TestMerge()) {
//			printf("Test Merge error\n");
//			return -1;
//		}

	}

	return 0;
}


bool TestWriteCreate() {

	printf(">>>>>>>>>>>>Testing Write Create<<<<<<<<<<<<<<<\n");

	std::string data_name = "/test_data_1";
	seisfs::ScopedPointer<seisfs::cache::SeisCache> data(new seisfs::cache::SeisCache(data_name));
	if(NULL == data.data()) {
		PRINT_ERR
		return false;
	}

	if(data->Exists()) {
		if(!data->Remove()) {
			PRINT_ERR
			return false;
		}
	}
#if 1
	seisfs::cache::HeadType head_type;
	for(int i = 0; i < 128; ++i) {
		head_type.type_arr.push_back(seisfs::cache::INT);
	}
	seisfs::cache::TraceType trace_type;
	trace_type.trace_size = 12000;
	seisfs::cache::Writer *writer = data->OpenWriter(&head_type, &trace_type, seisfs::AUTO);
	if(NULL == writer) {
		PRINT_ERR
		return false;
	}

	int *head = new int[128];
	float *trace = new float[3000];

	for(int trace_i = 0; trace_i < 100; ++trace_i) {
		for(int head_i = 0; head_i < 128; ++head_i) {
			head[head_i] = trace_i;
		}

		for(int samp_i = 0; samp_i < 3000; ++samp_i) {
			trace[samp_i] = 1.0 * trace_i;
		}

		if(!writer->Write(head, trace)) {
			PRINT_ERR
			return false;
		}
	}

	if(!writer->Sync()) {
		PRINT_ERR
		return false;
	}

	if(!writer->Close()) {
		PRINT_ERR
		return false;
	}
	delete writer;
#endif
	return true;
}

bool TestWriteAppend() {

	printf(">>>>>>>>>>>>Testing Write Append<<<<<<<<<<<<<<<\n");

	std::string data_name = "/test_data_1";

	seisfs::ScopedPointer<seisfs::cache::SeisCache> data(new seisfs::cache::SeisCache(data_name));
	if(NULL == data.data()) {
		PRINT_ERR
		return false;
	}

	int head_size = data->GetHeadSize();
	int trace_size = data->GetTraceSize();

	int64_t trace_num = data->GetTraceNum();

	printf("head_size = %d, trace_size = %d, trace_num = %lld\n", head_size, trace_size, trace_num);

	seisfs::cache::Writer *writer = data->OpenWriter(NULL, NULL);
	if(NULL == writer) {
		PRINT_ERR
		return false;
	}

	int head_len = head_size / 4;
	int trace_len = trace_size / 4;
	int *head_buf = new int[head_len];
	float *trace_buf = new float[trace_len];

	for(int trace_i = trace_num; trace_i < trace_num + 1000; ++trace_i) {
		for(int head_i = 0; head_i < 128; ++head_i) {
			head_buf[head_i] = trace_i;
		}

		for(int samp_i = 0; samp_i < 3000; ++samp_i) {
			trace_buf[samp_i] = 1.0 * trace_i;
		}

		if(!writer->Write(head_buf, trace_buf)) {
			PRINT_ERR
			return false;
		}
	}
	if(!writer->Sync()) {
		PRINT_ERR
		return false;
	}
	if(!writer->Close()) {
		PRINT_ERR
		return false;
	}
	delete writer;

	return true;
}

bool TestWriteSeparate() {

	printf(">>>>>>>>>>>>Testing Write Separate<<<<<<<<<<<<<<<\n");

	std::string data_name = "/test_data_1";
	seisfs::ScopedPointer<seisfs::cache::SeisCache> data(new seisfs::cache::SeisCache(data_name));
	if(NULL == data.data()) {
		PRINT_ERR
		return false;
	}

	int head_size = data->GetHeadSize();
	int trace_size = data->GetTraceSize();

	int64_t trace_num = data->GetTraceNum();

	printf("head_size = %d, trace_size = %d, trace_num = %lld\n", head_size, trace_size, trace_num);

	seisfs::cache::HeadWriter *head_writer = data->OpenHeadWriter();
	if(head_writer == NULL) {
		PRINT_ERR
		return false;
	}
	seisfs::cache::TraceWriter *trace_writer = data->OpenTraceWriter();
	if(trace_writer == NULL) {
		PRINT_ERR
		return false;
	}


	int head_len = head_size / 4;
	int trace_len = trace_size / 4;
	int *head_buf = new int[head_len];
	float *trace_buf = new float[trace_len];

	for(int trace_i = trace_num; trace_i < trace_num + 100000; ++trace_i) {
		for(int head_i = 0; head_i < 128; ++head_i) {
			head_buf[head_i] = trace_i;
		}

		if(!head_writer->Write(head_buf)) {
			PRINT_ERR
			return false;
		}
	}

	if(!head_writer->Sync()) {
		PRINT_ERR
		return false;
	}
	if(!head_writer->Close()) {
		PRINT_ERR
		return false;
	}
	delete head_writer;


	for(int trace_i = trace_num; trace_i < trace_num + 50000; ++trace_i) {
		for(int samp_i = 0; samp_i < 3000; ++samp_i) {
			trace_buf[samp_i] = 1.0 * trace_i;
		}

		if(!trace_writer->Write(trace_buf)) {
			PRINT_ERR
			return false;
		}
	}

	if(!trace_writer->Sync()) {
		PRINT_ERR
		return false;
	}
	if(!trace_writer->Close()) {
		PRINT_ERR
		return false;
	}
	delete trace_writer;

	trace_num = data->GetTraceNum();
	printf("After write separate, trace num is : %lld\n", trace_num);

	return true;

}


bool TestWrite() {
	if(!TestWriteCreate()) {
		printf("Test Write Create ERROR\n");
		return false;
	}
//	if(!TestWriteAppend()) {
//		printf("Test Write Append ERROR\n");
//		return false;
//	}
//	if(!TestWriteSeparate()) {
//		printf("Test Write Separate ERROR\n");
//		return false;
//	}
//	if(!TestWriteAppend()) {
//		printf("Test Write Append ERROR\n");
//		return false;
//	}

	return true;
}


bool TestReadSeq() {

	printf(">>>>>>>>>>>>Testing Read Seq<<<<<<<<<<<<<<<\n");

	std::string data_name = "/test_data_1";
	seisfs::ScopedPointer<seisfs::cache::SeisCache> data(new seisfs::cache::SeisCache(data_name));
	if(NULL == data.data()) {
		PRINT_ERR
		return false;
	}


	seisfs::cache::Reader *reader = data->OpenReader(NULL, NULL, seisfs::AUTO);
	if(NULL == reader) {
		PRINT_ERR
		return false;
	}
	printf("trace num from reader is : %lld\n", reader->GetTraceNum());

	seisfs::cache::HeadReader *head_reader = data->OpenHeadReader(NULL, NULL, seisfs::AUTO );
	if(NULL == head_reader) {
		PRINT_ERR;
		return false;
	}
	printf("trace num from head reader is : %lld\n", head_reader->GetTraceNum());

	seisfs::cache::TraceReader *trace_reader = data->OpenTraceReader(NULL, seisfs::AUTO);
	if(NULL == trace_reader) {
		PRINT_ERR
		return false;
	}
	printf("trace num from trace reader is : %lld\n", trace_reader->GetTraceNum());

	int *head = new int[128];
	float *trace = new float[3000];
	for(int i = 0; i < 100; ++i) {
		if(!reader->Next(head, trace)) {
			PRINT_ERR
			return false;
		} else {
			printf("read : %d, head : %d, trace : %f\n", i, head[0], trace[0]);
		}
	}

	reader->Close();
	delete reader;

	head_reader->Close();
	delete head_reader;

	trace_reader->Close();
	delete trace_reader;


	return true;
}

bool TestReadFilter(int64_t begin, int64_t end) {

	printf(">>>>>>>>>>>>Testing Read Filter<<<<<<<<<<<<<<<\n");

	std::string data_name = "/test_data_1";
	seisfs::ScopedPointer<seisfs::cache::SeisCache> data(new seisfs::cache::SeisCache(data_name));
	if(NULL == data.data()) {
		PRINT_ERR
		return false;
	}


	seisfs::RowFilter row_filter;
	seisfs::RowScope scope(begin,end);
	row_filter.AddFilter(scope);
//	row_filter.AddFilter(seisfs::RowScope(1056, 1097)).AddFilter(seisfs::RowScope(100, 200));

	seisfs::HeadFilter head_filter;
	head_filter.AddKey(1);
	head_filter.AddKey(3);
	head_filter.AddKey(4);
	head_filter.AddKey(5);

//	seisfs::cache::Reader *reader = data->OpenReader(&head_filter, &row_filter, seisfs::AUTO);
//	if(NULL == reader) {
//		PRINT_ERR
//		return false;
//	}
	seisfs::cache::HeadReader *head_reader = data->OpenHeadReader(&head_filter, &row_filter, seisfs::AUTO);

	int head_size = head_reader->GetHeadSize();
	int trace_num = head_reader->GetTraceNum();
	printf("After filter, head size = %d, trace_num = %d\n", head_size, trace_num);


	int *head = new int[128];
	float *trace = new float[3000];

	for(int i = 0; i < trace_num; ++i) {
		if(head_reader->HasNext()) {
			if(!head_reader->Next(head)) {
				PRINT_ERR
				return false;
			} else {
				printf("read : %d, head : %d\n", i, head[0]);
			}
		}
	}

	if(!head_reader->Get(trace_num - 1, head)) {
		PRINT_ERR
		return false;
	} else {
		printf("The last trace after filter is : head : %d\n", head[0]);
	}

	head_reader->Close();
	delete head_reader;


	return true;
}

bool TestReadSpeed(int begin, int end) {
	printf(">>>>>>>>>>>>Testing Read Speed<<<<<<<<<<<<<<<\n");
	std::string data_name = "/test_data_1";
	seisfs::ScopedPointer<seisfs::cache::SeisCache> data(new seisfs::cache::SeisCache(data_name));
	if(NULL == data.data()) {
		PRINT_ERR
		return false;
	}

	seisfs::RowFilter *row_filter = new seisfs::RowFilter();
	seisfs::RowScope scope(begin,end);
	row_filter->AddFilter(scope);

	seisfs::HeadFilter *head_filter = new seisfs::HeadFilter();
	head_filter->AddKey(1);
	head_filter->AddKey(3);
	head_filter->AddKey(4);
	head_filter->AddKey(5);


	int head_tmp[128];
	float trace_tmp[3000];

	seisfs::cache::TraceReader *trace_reader = data->OpenTraceReader(NULL);
	for(int i = 1000; i < 1100;++i) {
		trace_reader->Get(i, trace_tmp);
		printf("trace_i = %d, trace = %f\n", i, trace_tmp[0]);
	}

	seisfs::cache::HeadReader *head_reader = data->OpenHeadReader(NULL, NULL);
	for(int i = 1000; i < 1100;++i) {
		head_reader->Get(i, head_tmp);
		printf("trace_i = %d, head = %d\n", i, head_tmp[0]);
	}


	seisfs::cache::SpeedReader *spd_reader = data->OpenSpeedReader(NULL, row_filter, seisfs::AUTO);
	if(NULL == spd_reader) {
		printf("errno = %d, error is : %s\n", seisfs::cache::sc_errno, seisfs::cache::SCStrerror(seisfs::cache::sc_errno).c_str());
	}
/*	seisfs::cache::HeadSpeedReader *head_spd_reader = data->OpenHeadSpeedReader(head_filter, row_filter, seisfs::CACHE);
	if(NULL == head_spd_reader) {
		printf("errno = %d, error is : %s\n", seisfs::cache::sc_errno, seisfs::cache::SCStrerror(seisfs::cache::sc_errno).c_str());
	}*/

	int trace_size = spd_reader->GetTraceSize();
	int head_size = spd_reader->GetHeadSize();
	int trace_num = spd_reader->GetTraceNum();

	printf("head_size = %d, trace_size = %d, speed read trace num = %d\n", head_size, trace_size, trace_num);

	int traceSize = trace_size * trace_num;
	char * trace_buf = (char*)malloc(traceSize);
	if(trace_buf == NULL) {
		fprintf(stdout,"malloc trace space failure!\n");
		return false;
	}

	int headSize = head_size * trace_num;
	char *head_buf = (char*)malloc(headSize);
	if(head_buf == NULL) {
		fprintf(stdout,"malloc head space failure!\n");
		return false;
	}

	spd_reader->Get(head_buf, trace_buf);

	for(int i=0;i<trace_num;i++) {
		float * floatBuf = (float*) trace_buf;
		floatBuf = floatBuf + trace_size / 4 * i;
		fprintf(stdout,"%f ",floatBuf[2]);fflush(stdout);
	}
	fprintf(stdout,"\n");fflush(stdout);

//	head_spd_reader->Get(head_buf);

	for(int i=0;i<trace_num;i++) {
		int * intBuf = (int*) head_buf;
		intBuf = intBuf + head_size / 4 * i;
		fprintf(stdout,"%d ",intBuf[2]);fflush(stdout);
	}
	fprintf(stdout,"\n");fflush(stdout);


	spd_reader->Close();
	delete spd_reader;

//	head_spd_reader->Close();
//	delete head_spd_reader;



	return true;
}


bool TestRead() {
//	if(!TestReadSeq()) {
//		printf("Test Read Sequential ERROR\n");
//		return false;
//	}
//	if(!TestReadFilter(253, 265)) {
//		printf("Test Read Filter ERROR\n");
//		return false;
//	}
	if(!TestReadSpeed(1000,3100)) {
		printf("Test Read Speed ERROR\n");
		return false;
	}
	return true;
}



bool TestUpdateSeq() {
	printf(">>>>>>>>>>>>Testing Update Seq<<<<<<<<<<<<<<<\n");

	std::string data_name = "/test_data_1";
	seisfs::ScopedPointer<seisfs::cache::SeisCache> data(new seisfs::cache::SeisCache(data_name));
	if(NULL == data.data()) {
		PRINT_ERR
		return false;
	}

	int head_tmp[128];
	for(int i = 0; i < 128; ++i) {
		head_tmp[i] = -99;
	}
	float trace_tmp[3000];
	for(int i = 0; i < 3000; ++i) {
		trace_tmp[i] = -9999.0;
	}

	seisfs::cache::Updater *updater = data->OpenUpdater(NULL, NULL, seisfs::AUTO);
	if(NULL == updater) {
		PRINT_ERR
	}
	updater->Put(125, head_tmp, trace_tmp);
	updater->Put(154, head_tmp, trace_tmp);
	updater->Put(179, head_tmp, trace_tmp);
	updater->Put(221, head_tmp, trace_tmp);
	updater->Sync();
	updater->Close();

	TestReadSpeed(120,230);

	return true;
}


bool TestUpdateFilter() {
	printf(">>>>>>>>>>>>Testing Update Filter<<<<<<<<<<<<<<<\n");

	std::string data_name = "/test_data_1";
	seisfs::ScopedPointer<seisfs::cache::SeisCache> data(new seisfs::cache::SeisCache(data_name));
	if(NULL == data.data()) {
		PRINT_ERR
		return false;
	}

	int head_tmp[128];
	for(int i = 0; i < 128; ++i) {
		head_tmp[i] = -99;
	}
	float trace_tmp[3000];
	for(int i = 0; i < 3000; ++i) {
		trace_tmp[i] = -9999.0;
	}


	seisfs::RowFilter row_filter;
//	seisfs::RowScope scope(356,367);
	row_filter.AddFilter(seisfs::RowScope(356, 364)).AddFilter(seisfs::RowScope(25, 29));

	seisfs::HeadFilter head_filter;
	head_filter.AddKey(1);
	head_filter.AddKey(3);

	seisfs::cache::Updater *updater = data->OpenUpdater(&head_filter, &row_filter, seisfs::AUTO);
	if(NULL == updater) {
		PRINT_ERR
	}

//	updater->Put(1, head_tmp, trace_tmp);
//	updater->Put(2, head_tmp, trace_tmp);
//	updater->Put(3, head_tmp, trace_tmp);
//	updater->Put(4, head_tmp, trace_tmp);
//	updater->Put(25, head_tmp, trace_tmp);

	for(int i = 0; i < 12; ++i) {
		updater->Put(head_tmp, trace_tmp);
	}

	updater->Sync();
	updater->Close();

	TestReadSpeed(0,400);
	TestReadFilter(0, 400);

	return true;


	return true;
}
bool TestUpdate() {
	if(!TestUpdateSeq()) {
		printf("Test Update Sequential ERROR\n");
		return false;
	}

	if(!TestUpdateFilter()) {
		printf("Test Update Filter ERROR\n");
		return false;
	}

	return true;
}

bool TestKVInfo() {
	printf(">>>>>>>>>>>>Testing KVInfo<<<<<<<<<<<<<<<\n");

	std::string data_name = "/test_data_1";
	seisfs::ScopedPointer<seisfs::cache::SeisCache> data(new seisfs::cache::SeisCache(data_name));
	if(NULL == data.data()) {
		PRINT_ERR
		return false;
	}

	seisfs::cache::KVInfo *kv_info = data->OpenKVInfo(seisfs::AUTO);
	kv_info->Put("name", "/test_data_1", 13);
	kv_info->Put("time", "2018-10-11", 11);
	int size = 1024;
	kv_info->Put("size", (char*)&size, sizeof(size));


	std::vector<std::string> key_arr;
	bool ret = kv_info->GetKeys(key_arr);
	if(!ret) {
		PRINT_ERR;
		return false;
	}
	int key_num = key_arr.size();
	printf("key_num = %d\n", key_num);

	for(int i = 0; i < key_num; ++i) {
		char *buf;
		uint32_t buf_size;
		ret = kv_info->Get(key_arr[i], buf, buf_size);
		if(!ret) {
			PRINT_ERR;
			return false;
		}

		if(key_arr[i] == "size") {
			int size_tmp;
			memcpy(&size_tmp, buf, buf_size);

			printf("key : %s, size = %d, value = %d\n", key_arr[i].c_str(), buf_size, size_tmp);

		} else
			printf("key : %s, size = %d, value = %s\n", key_arr[i].c_str(), buf_size, buf);
	}


	return true;
}


bool TestCopy() {
	printf(">>>>>>>>>>>>Testing Copy<<<<<<<<<<<<<<<\n");

	std::string data_name = "/test_data_1";
	std::string data_name_2 = "/test_data_2";
	seisfs::ScopedPointer<seisfs::cache::SeisCache> data(new seisfs::cache::SeisCache(data_name));
	if(NULL == data.data()) {
		PRINT_ERR
		return false;
	}

	bool ret = data->Copy(data_name_2);
	if(!ret) {
		PRINT_ERR;
		return false;
	}

	seisfs::ScopedPointer<seisfs::cache::SeisCache> data_2(new seisfs::cache::SeisCache(data_name_2));
	if(NULL == data.data()) {
		PRINT_ERR
		return false;
	}


	seisfs::RowFilter row_filter;
	seisfs::RowScope scope(0,400);
	row_filter.AddFilter(scope);

	seisfs::HeadFilter head_filter;
	head_filter.AddKey(1);
	head_filter.AddKey(3);
	head_filter.AddKey(4);
	head_filter.AddKey(5);

	seisfs::cache::HeadReader *head_reader = data_2->OpenHeadReader(&head_filter, &row_filter, seisfs::AUTO);

	int head_size = head_reader->GetHeadSize();
	int trace_num = head_reader->GetTraceNum();

	int *head = new int[128];
	float *trace = new float[3000];

	for(int i = 0; i < trace_num; ++i) {
		if(head_reader->HasNext()) {
			if(!head_reader->Next(head)) {
				PRINT_ERR
				return false;
			} else {
				printf("read : %d, head : %d\n", i, head[0]);
			}
		}
	}

	head_reader->Close();
	delete head_reader;

	return true;
}

bool TestRename() {
	printf(">>>>>>>>>>>>Testing Rename<<<<<<<<<<<<<<<\n");

	std::string data_name = "/test_data_1";
	std::string data_name_2 = "/test_data_3";
	seisfs::ScopedPointer<seisfs::cache::SeisCache> data(new seisfs::cache::SeisCache(data_name));
	if(NULL == data.data()) {
		PRINT_ERR
		return false;
	}

	bool ret = data->Rename(data_name_2);
	if(!ret) {
		PRINT_ERR;
		return false;
	}

	seisfs::ScopedPointer<seisfs::cache::SeisCache> data_2(new seisfs::cache::SeisCache(data_name_2));
	if(NULL == data.data()) {
		PRINT_ERR
		return false;
	}


	seisfs::RowFilter row_filter;
	seisfs::RowScope scope(0,400);
	row_filter.AddFilter(scope);

	seisfs::HeadFilter head_filter;
	head_filter.AddKey(1);
	head_filter.AddKey(3);
	head_filter.AddKey(4);
	head_filter.AddKey(5);

	seisfs::cache::HeadReader *head_reader = data_2->OpenHeadReader(&head_filter, &row_filter, seisfs::AUTO);

	int head_size = head_reader->GetHeadSize();
	int trace_num = head_reader->GetTraceNum();

	int *head = new int[128];
	float *trace = new float[3000];

	for(int i = 0; i < trace_num; ++i) {
		if(head_reader->HasNext()) {
			if(!head_reader->Next(head)) {
				PRINT_ERR
				return false;
			} else {
				printf("read : %d, head : %d\n", i, head[0]);
			}
		}
	}

	head_reader->Close();
	delete head_reader;

	if(data->Exists()) {
		printf("Rename error, old data still exist\n");
		return false;
	}

	return true;
}

bool TestMerge() {
	printf(">>>>>>>>>>>>Testing Merge<<<<<<<<<<<<<<<\n");
	return true;
}

