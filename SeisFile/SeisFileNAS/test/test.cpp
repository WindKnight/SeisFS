/*
 * test.cpp
 *
 *  Created on: Apr 6, 2018
 *      Author: wyd
 */


#include "seisfs_config.h"
#include "seisfile_nas.h"
#include "seisfile_nas_kvinfo.h"
#include "seisfile_nas_writer.h"
#include "seisfile_nas_meta.h"
#include "seisfile_nas_reader.h"
#include "seisfile_nas_traceupdater.h"
#include "seisfile_nas_headupdater.h"
#include "seisfile_nas_tracewriter.h"

#include "seisfile_nas_headmerger.h"
#include "seisfile_nas_tracemerger.h"

#include <stdio.h>
#include <iostream>
#include <cstring>

#include "seisfs_head_filter.h"
#include "seisfs_row_filter.h"


int main() {

	//test Writer

	/*seisfs::file::HeadType *headtype=new seisfs::file::HeadType;
	seisfs::file::TraceType *tracetype=new seisfs::file::TraceType;
	std::vector<uint16_t> v;
	v.push_back(3);
	v.push_back(4);
	v.push_back(5);
	v.push_back(6);
	v.push_back(8);
	headtype->head_size = v;
	tracetype->trace_size = 26;*/

	//seisfs::file::SeisFileNAS *sf_nas = new seisfs::file::SeisFileNAS("file1");
	//seisfs::file::Writer *writer = sf_nas->OpenWriter(*headtype, *tracetype, 30);
	//std::string s[5] = {"aaabbbbccccceeeeeeffffffff","00011112222233333344444444","+++----*****!!!!!!@@@@@@@@","AAABBBBCCCCCEEEEEEFFFFFFFF","99988887777766666655555555"};
	/*for(int j = 0; j < 10; j++){
		for(int i = 0; i < 5; i++){
			char *head = new char[s[i].length()+1];
			std::strcpy(head, s[i].c_str());
			char *trace = new char[s[i].length()+1];
			std::strcpy(trace, s[i].c_str());
			writer->Write(head,trace);
		}
	}*/
	/*char *head = new char[s[0].length()+1];
	std::strcpy(head, s[0].c_str());
	char *trace = new char[s[0].length()+1];
	std::strcpy(trace, s[0].c_str());
	writer->Write(head,trace);*/
	//writer->Close();

	//test tracewriter
	/*seisfs::file::TraceWriter *tracewriter = sf_nas->OpenTraceWriter();
	char *trace = new char[s[0].length()+1];
	std::strcpy(trace, s[0].c_str());
	tracewriter->Write(trace);
	tracewriter->Close();*/
	//std::cout<<"trace num is:"<<sf_nas->GetTraceNum()<<endl;
	//test headwriter
	/*seisfs::file::HeadWriter *headwriter = sf_nas->OpenHeadWriter();
	char *head = new char[s[0].length()+1];
	std::strcpy(head, s[0].c_str());
	headwriter->Write(head);
	headwriter->Close();*/

	//std::cout<<"file exits:"<<sf_nas->Exists()<<endl;

	//seisfs::file::SeisFileNAS *sf_nas1 = new seisfs::file::SeisFileNAS("file1");
	/*seisfs::file::Writer *writer1 = sf_nas1->OpenWriter();
	char *head1 = new char[s[1].length()+1];
	std::strcpy(head1, s[1].c_str());
	char *trace1 = new char[s[1].length()+1];
	std::strcpy(trace1, s[1].c_str());
	writer1->Write(head1,trace1);
	printf("head num is:%u\n",sf_nas1->GetTraceNum());
	writer1->Close();*/
	//sf_nas->Remove();

	//return 0;

	//test tracewriter
	/*seisfs::file::TraceType *tracetype=new seisfs::file::TraceType;
	tracetype->trace_size = 26;

	seisfs::file::SeisFileNAS *sf_nas = new seisfs::file::SeisFileNAS("file1");
	seisfs::file::TraceWriter *tracewriter = sf_nas->OpenTraceWriter(*tracetype, 30);
	std::string s[5] = {"aaabbbbccccceeeeeeffffffff","00011112222233333344444444","+++----*****!!!!!!@@@@@@@@","AAABBBBCCCCCEEEEEEFFFFFFFF","99988887777766666655555555"};
	for(int i = 0; i < 5; i++){
		//char trace[s[i].length()+1];
		//memcpy(trace, s[i].c_str(), s[i].length());
		//tracewriter->Write(trace);
		char *trace = new char[s[i].length()+1];
		std::strcpy(trace, s[i].c_str());
		tracewriter->Write(trace);
	}
	tracewriter->Close();
	return 0;*/


	//test Writer(much data)

	/*seisfs::file::HeadType *headtype=new seisfs::file::HeadType;
	seisfs::file::TraceType *tracetype=new seisfs::file::TraceType;
	std::vector<uint16_t> v;
	int64_t num = 30000;
	int64_t size = sizeof(int)*num;
	for(int64_t i = 0; i < num; i++){
		v.push_back(sizeof(int));
	}
	headtype->head_size = v;
	tracetype->trace_size = size;

	seisfs::file::SeisFileNAS *sf_nas = new seisfs::file::SeisFileNAS("file1");
	seisfs::file::Writer *writer = sf_nas->OpenWriter(*headtype, *tracetype, 30);
	int64_t j, line = num;
	for(j = 0; j < line; j++){
		char *head = new char[size+1],*trace = new char[size+1];
		memset(head, 0, size+1);
		memset(trace, 0, size+1);
		char *p1 = head, *p2 = trace;
		int64_t tempnum = j;
		for(int i = 1; i <= num; i++){
			memcpy(p1, &tempnum, sizeof(int));
			memcpy(p2, &j, sizeof(int));
			tempnum++;
			p1 += sizeof(int);
			p2 += sizeof(int);
		}
		//int t;
		//memcpy(&t, head+sizeof(int),sizeof(int));
		//printf("%s\n",head+2*sizeof(int));
		writer->Write(head, trace);
		delete head;
		delete trace;
	}
	writer->Close();
	return 0;*/
	//test writer(100,000,000 trace)
	/*seisfs::file::HeadType *headtype=new seisfs::file::HeadType;
	seisfs::file::TraceType *tracetype=new seisfs::file::TraceType;
	std::vector<uint16_t> v;
	int64_t num = 1000;
	int64_t headlength = sizeof(int64_t)*128;
	int64_t tracelength = sizeof(int64_t)*3000;
	for(int i = 0; i < 128; i++){
		v.push_back(8);
	}
	headtype->head_size = v;
	tracetype->trace_size = tracelength;

	seisfs::file::SeisFileNAS *sf_nas = new seisfs::file::SeisFileNAS("file1");
	seisfs::file::Writer *writer = sf_nas->OpenWriter(*headtype, *tracetype, 30);
	int64_t j, line = num;
	for(j = 0; j < line; j++){
		char *head = new char[headlength+1],*trace = new char[tracelength+1];
		memset(head, 0, headlength+1);
		memset(trace, 0, tracelength+1);
		char *p1 = head, *p2 = trace;

		int64_t tempnum = j;
		for(int i = 0; i < 3000; i++){
			memcpy(p2, &tempnum, sizeof(int64_t));
			p2 += sizeof(int64_t);
		}
		for(int i = 0; i < 128; i++){
			memcpy(p1, &tempnum, sizeof(int64_t));
			tempnum++;
			p1 += sizeof(int64_t);
		}
		writer->Write(head, trace);
		delete head;
		delete trace;
	}
	writer->Close();*/
	/////////////////////////////////////
	//test writer (50,000,000, all 0)
	//////////////////////////////////////
	/*seisfs::file::HeadType *headtype=new seisfs::file::HeadType;
	seisfs::file::TraceType *tracetype=new seisfs::file::TraceType;
	std::vector<uint16_t> v;
	int64_t num = 1000000;
	int64_t headlength = sizeof(int)*128;
	int64_t tracelength = sizeof(float)*3000;
	for(int i = 0; i < 128; i++){
		v.push_back(sizeof(int));
	}
	headtype->head_size = v;
	tracetype->trace_size = tracelength;*/

	//seisfs::file::SeisFileNAS *sf_nas = new seisfs::file::SeisFileNAS("file2");
	/*seisfs::file::Writer *writer = sf_nas->OpenWriter(*headtype, *tracetype, 30);
	int64_t j, line = num;
	char *head = new char[headlength+1],*trace = new char[tracelength+1];
	memset(head, 0, headlength+1);
	memset(trace, 0, tracelength+1);
	for(j = 0; j < line; j++){
		writer->Write(head, trace);
	}
	writer->Close();*/
	/*sf_nas->Remove();

	seisfs::file::Writer *writer1 = sf_nas->OpenWriter(*headtype, *tracetype, 30);
	for(j = 0; j < line; j++){
		writer1->Write(head, trace);
	}
	writer1->Close();*/
	//delete head;
	//delete trace;
	/*sf_nas->SetLifetimeStat(seisfs::NORMAL);
	printf("head size is:%d\n",sf_nas->GetHeadSize());
	printf("trace num is:%d\n",sf_nas->GetTraceNum());
	printf("last modify time is:%ld\n",sf_nas->LastAccessed());
	printf("key num is:%d\n",sf_nas->GetKeyNum());
	printf("trace size is:%u\n",sf_nas->GetTraceSize());
	std::cout<<sf_nas->GetName()<<endl;
	std::cout<<sf_nas->Remove()<<endl;*/

	/*if(sf_nas->Exists()){
		std::cout<<"exits"<<endl;
	}
	else{
		std::cout<<"not exits"<<endl;
	}
	std::cout<<"out"<<endl;*/

	/////////////////////////////////////
	//test Reader(50,000,000, all 0)
	seisfs::file::SeisFileNAS *sf_nas1 = new seisfs::file::SeisFileNAS("file2");
	seisfs::file::Reader *reader = sf_nas1->OpenReader();
	seisfs::HeadFilter head_filter;
	for(int i = 0; i < 128; i++){
		head_filter.AddKey(i);
	}
	seisfs::RowScope row_scope;
	row_scope.SetStartTrace(0);
	row_scope.SetStopTrace(-1);
	seisfs::RowFilter row_filter(row_scope);
	reader->SetHeadFilter(head_filter);
	reader->SetRowFilter(row_filter);

	printf("trace size is:%lu\n",reader->GetTraceNum());
	printf("head size is:%d\n",reader->GetHeadSize());
	printf("trace num is:%ld\n",reader->GetTraceNum());
	printf("tell is:%ld\n",reader->Tell());
	printf("trace size is:%u\n",reader->GetTraceSize());


	//sf_nas1->Remove();

	//time_t start,stop,totaltime;
	//time(&start);
	/*for(int64_t i = 0; i < 50000000; i ++){
		char *head = new char[sizeof(int)*128];
		char *trace = new char[sizeof(float)*3000];
		reader->Next(head, trace);
		delete head;
		delete trace;
	}*/
	//reader->Close();
	return 0;
	//time(&stop);
	//totaltime = stop - start;
	//printf("total time is:%lds\n",totaltime);
	/////////////////////////////////////
	//test Reader(much data 100000000)

	/*seisfs::file::SeisFileNAS *sf_nas = new seisfs::file::SeisFileNAS("file2");
	seisfs::file::Reader *reader = sf_nas->OpenReader();
	seisfs::HeadFilter head_filter;
	head_filter.AddKey(0);
	head_filter.AddKey(5);
	head_filter.AddKey(8);
	head_filter.AddKey(63);
	head_filter.AddKey(120);

	seisfs::RowScope row_scope;
	row_scope.SetStartTrace(0);
	row_scope.SetStopTrace(-1);
	seisfs::RowFilter row_filter(row_scope);
	reader->SetHeadFilter(head_filter);
	reader->SetRowFilter(row_filter);

	//int64_t num = 20000;
	char *head = new char[sizeof(int64_t)*128];
	char *trace = new char[sizeof(int64_t)*3000];
	reader->Get(999, head, trace);

	int64_t hh = 0, tt = 0;
	memcpy(&hh, head, sizeof(int64_t));
	memcpy(&tt, trace, sizeof(int64_t));
	printf("head is:%ld,trace is:%ld\n", hh, tt);

	printf("\nhead is:");
	for(int i = 0; i < 5; i++){
		memcpy(&hh, head, sizeof(int64_t));
		printf("%ld ", hh);
		head += sizeof(int64_t);
	}
	printf("\ntrace is:");
	for(int i = 0; i < 30; i++){
		memcpy(&tt, trace, sizeof(int64_t));
		printf("%ld ", tt);
		trace += sizeof(int64_t);
	}
	printf("\n");
	return 0;*/
	/////////////////////////////////////
	//test headreader(much data 100000000)
	/*seisfs::file::SeisFileNAS *sf_nas = new seisfs::file::SeisFileNAS("file1");
	seisfs::file::HeadReader *headreader = sf_nas->OpenHeadReader();
	seisfs::HeadFilter head_filter;
	head_filter.AddKey(0);
	head_filter.AddKey(5);
	head_filter.AddKey(8);
	head_filter.AddKey(63);
	head_filter.AddKey(120);

	seisfs::RowScope row_scope;
	row_scope.SetStartTrace(0);
	row_scope.SetStopTrace(-1);
	seisfs::RowFilter row_filter(row_scope);
	headreader->SetHeadFilter(head_filter);
	headreader->SetRowFilter(row_filter);

	char *head = new char[sizeof(int64_t)*128];
	headreader->Get(1005, head);

	int64_t hh = 0, tt = 0;
	memcpy(&hh, head, sizeof(int64_t));
	printf("head is:%ld,trace is:%ld\n", hh, tt);

	printf("\nhead is:");
	for(int i = 0; i < 5; i++){
		memcpy(&hh, head, sizeof(int64_t));
		printf("%ld ", hh);
		head += sizeof(int64_t);
	}
	printf("\n");
	return 0;*/

	//test tracewriter(much data 100000000)
	/*seisfs::file::SeisFileNAS *sf_nas = new seisfs::file::SeisFileNAS("file1");
	seisfs::file::TraceReader *tracereader = sf_nas->OpenTraceReader();
	seisfs::RowScope row_scope;
	row_scope.SetStartTrace(0);
	row_scope.SetStopTrace(-1);
	seisfs::RowFilter row_filter(row_scope);
	tracereader->SetRowFilter(row_filter);

	char *trace = new char[sizeof(int64_t)*3000];
	tracereader->Get(1005, trace);

	int64_t hh = 0, tt = 0;
	memcpy(&tt, trace, sizeof(int64_t));
	printf("head is:%ld,trace is:%ld\n", hh, tt);

	printf("\ntrace is:");
	for(int i = 0; i < 30; i++){
		memcpy(&tt, trace, sizeof(int64_t));
		printf("%ld ", tt);
		trace += sizeof(int64_t);
	}
	printf("\n");
	return 0;*/


	/////////////////////////////////////
	//test Reader

	/*seisfs::file::SeisFileNAS *sf_nas = new seisfs::file::SeisFileNAS("file1");
	seisfs::file::Reader *reader = sf_nas->OpenReader();

	seisfs::HeadFilter head_filter;
	head_filter.AddKey(4);
	//head_filter.AddKey(0);
	//head_filter.AddKey(1);
	head_filter.AddKey(2);
	//head_filter.AddKey(3);*/

	/*seisfs::RowScope row_scope[5];
	for(int i = 0; i < 4; i++){
		row_scope[i].SetStartTrace(i*10);
		row_scope[i].SetStopTrace(i*10 + 4);
	}
	row_scope[4].SetStartTrace(40);
	row_scope[4].SetStopTrace(-1);

	seisfs::RowFilter row_filter;
	for(int i = 0; i < 5; i++){
		row_filter.AddFilter(row_scope[i]);
	}*/

	/*seisfs::RowScope row_scope[2];
	row_scope[0].SetStartTrace(0);
	row_scope[0].SetStopTrace(1);
	row_scope[1].SetStartTrace(3);
	row_scope[1].SetStopTrace(14);
	seisfs::RowFilter row_filter;
	row_filter.AddFilter(row_scope[0]);
	row_filter.AddFilter(row_scope[1]);

	reader->SetHeadFilter(head_filter);
	reader->SetRowFilter(row_filter);

	char *head = new char[20];
	char *trace = new char[30];*/


	/*reader->Get(0, head, trace);
	printf("bbb\n");
	printf("%s ",head);
	printf("%s\n",trace);*/
	//reader->Seek(3);
	/*printf("head size is:%u\n", reader->GetHeadSize());
	printf("trace size is:%u\n", sf_nas->GetTraceSize());
	printf("cur is:%ld\n", reader->Tell());
	reader->Next(head,trace);
	printf("%s ",head);
	printf("%s\n",trace);
	printf("cur is:%ld\n", reader->Tell());
	reader->Next(head,trace);
	printf("%s ",head);
	printf("%s\n",trace);
	printf("cur is:%ld\n", reader->Tell());
	reader->Next(head,trace);
	printf("%s ",head);
	printf("%s\n",trace);
	printf("cur is:%ld\n", reader->Tell());*/
	/*reader->Get(1,head,trace);
	printf("%s ",head);
	printf("%s\n",trace);
	reader->Get(2,head,trace);
	printf("%s ",head);
	printf("%s\n",trace);*/
	/*printf("head size is:%u\n", reader->GetHeadSize());
	printf("total trace num is:%u\n", reader->GetTraceNum());*/
	//reader->Close();

	//return 0;
	/////////////////////////////////////


	/////////////////////////////////////
	//test traceupdater

	/*seisfs::file::SeisFileNAS *sf_nas = new seisfs::file::SeisFileNAS("file1");
	seisfs::file::TraceUpdater *traceupdater = sf_nas->OpenTraceUpdater();

	seisfs::RowScope row_scope[5];
	for(int i = 0; i < 4; i++){
		row_scope[i].SetStartTrace(i*10);
		row_scope[i].SetStopTrace(i*10 + 4);
	}
	row_scope[4].SetStartTrace(40);
	row_scope[4].SetStopTrace(-1);

	seisfs::RowFilter row_filter;
	for(int i = 0; i < 5; i++){
		row_filter.AddFilter(row_scope[i]);
	}

	traceupdater->SetRowFilter(row_filter);

	char trace[27] = "abcdefghijklmnopqrstuvwxyz";
	traceupdater->Put(15, trace);
	traceupdater->Close();
	return 0;*/
	/////////////////////////////////////


	//test traceupdater(much data)
	/*seisfs::file::SeisFileNAS *sf_nas = new seisfs::file::SeisFileNAS("file1");
	seisfs::file::TraceUpdater *traceupdater = sf_nas->OpenTraceUpdater();

	seisfs::RowScope row_scope;
	row_scope.SetStartTrace(0);
	row_scope.SetStopTrace(-1);
	seisfs::RowFilter row_filter(row_scope);
	traceupdater->SetRowFilter(row_filter);

	char trace[sizeof(int64_t)*3000 + 1];
	char *p = trace;
	int64_t tp = 999;
	for(int i = 0; i < 3000; i++){
		memcpy(p, &tp, sizeof(int64_t));
		p += sizeof(int64_t);
	}
	trace[sizeof(int64_t)*3000 + 1] = '\0';
	traceupdater->Put(0, trace);
	traceupdater->Close();
	return 0;*/


	/////////////////////////////////////
	//test headupdater

	/*seisfs::file::SeisFileNAS *sf_nas = new seisfs::file::SeisFileNAS("file1");
	seisfs::file::HeadUpdater *headupdater = sf_nas->OpenHeadUpdater();

	seisfs::HeadFilter head_filter;
	head_filter.AddKey(0);
	head_filter.AddKey(2);

	seisfs::RowScope row_scope[5];
	for(int i = 0; i < 4; i++){
		row_scope[i].SetStartTrace(i*10);
		row_scope[i].SetStopTrace(i*10 + 4);
	}
	row_scope[4].SetStartTrace(40);
	row_scope[4].SetStopTrace(-1);

	seisfs::RowFilter row_filter;
	for(int i = 0; i < 5; i++){
		row_filter.AddFilter(row_scope[i]);
	}

	headupdater->SetHeadFilter(head_filter);
	headupdater->SetRowFilter(row_filter);

	//const char *head = new char[8];
	//const char *head = "YYYYYYYY";
	std::string str = "00000000";
	char *head = new char[str.length()+1];
	std::strcpy(head, str.c_str());

	headupdater->Put(30, head);
	headupdater->Close();

	return 0;*/
	/////////////////////////////////////
	//test headupdater(much data)  //modified trace num: 0, 9999
	/*seisfs::file::SeisFileNAS *sf_nas = new seisfs::file::SeisFileNAS("file1");
	seisfs::file::HeadUpdater *headupdater = sf_nas->OpenHeadUpdater();
	seisfs::HeadFilter head_filter;
	head_filter.AddKey(0);
	head_filter.AddKey(5);
	head_filter.AddKey(8);
	head_filter.AddKey(63);
	head_filter.AddKey(120);

	seisfs::RowScope row_scope;
	row_scope.SetStartTrace(0);
	row_scope.SetStopTrace(-1);
	seisfs::RowFilter row_filter(row_scope);

	headupdater->SetHeadFilter(head_filter);
	headupdater->SetRowFilter(row_filter);

	char head[sizeof(int64_t)*5 + 1];
	char *p = head;
	int64_t i = 66666;
	for(int j = 0; j < 5; j++){
		memcpy(p, &i, sizeof(int64_t));
		p += sizeof(int64_t);
	}
	head[sizeof(int64_t)*5 + 1] = '\0';
	headupdater->Put(9999, head);
	headupdater->Close();
	return 0;*/

	/////////////////////////////////////
	//test kvinfo
	/*seisfs::file::SeisFileNAS *sf_nas = new seisfs::file::SeisFileNAS("file1");
	seisfs::file::KVInfo *kvinfo = sf_nas->OpenKVInfo();


	char key[10000][10];
	for(int64_t i = 0; i < 10000; i++){
		sprintf(key[i], "%ld", i);
		kvinfo->Put(key[i] ,key[i], strlen(key[i]));
	}

	char *p = NULL;
	uint32_t size;
	bool judge = false;
	judge = kvinfo->Get(key[100], p, size);
	if(judge == true){
		printf("%s\n",p);
		printf("%u\n",size);
	}
	p=NULL;
	judge = kvinfo->Get(key[1000], p, size);
	if(judge == true){
		printf("%s\n",p);
		printf("%u\n",size);
	}
	p=NULL;
	judge = kvinfo->Get(key[891], p, size);
	if(judge == true){
		printf("%s\n",p);
		printf("%u\n",size);
	}

	kvinfo->Delete("891");
	kvinfo->Delete("1000");

	p=NULL;
	judge = kvinfo->Get(key[100], p, size);
	if(judge == true){
		printf("%s\n",p);
		printf("%u\n",size);
	}
	p=NULL;
	judge = kvinfo->Get(key[1000], p, size);
	if(judge == true){
		printf("%s\n",p);
		printf("%u\n",size);
	}
	p=NULL;
	judge = kvinfo->Get(key[891], p, size);
	if(judge == true){
		printf("%s\n",p);
		printf("%u\n",size);
	}

	return 0;*/

	/*printf("%ld",sizeof(int64_t));\
	return 0;*/

	//test headmerger
	/*seisfs::file::HeadMergerNAS *hd = new seisfs::file::HeadMergerNAS("/d0/data/seisfiletest/seisfiledisk/file1","/d0/data/seisfiletest/seisfiledisk/file2");
	hd->Merge();
	return 0;*/

	//test tracemerger
	/*seisfs::file::TraceMergerNAS *hd = new seisfs::file::TraceMergerNAS("/d0/data/seisfiletest/seisfiledisk/file1","/d0/data/seisfiletest/seisfiledisk/file2");
	hd->Merge();
	return 0;*/

	//test seisfilenas merger
//	seisfs::file::SeisFileNAS *sf_nas = new seisfs::file::SeisFileNAS("file1");
//	sf_nas->Merge("file2");
//	return 0;

	//test rename
	/*seisfs::file::SeisFileNAS *sf_nas = new seisfs::file::SeisFileNAS("file2");
	//sf_nas->Rename("file2");
	std::cout<<"exits?: "<<sf_nas->Exists()<<endl;
	return 0;*/
}




