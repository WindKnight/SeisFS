/*
 * tmp_test.cpp
 *
 *  Created on: Oct 11, 2017
 *      Author: nick
 */
#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK
#include <boost/test/unit_test.hpp>
#include "tmp/gcache_advice.h"
#include "tmp/gcache_tmp_dir.h"
#include "tmp/gcache_tmp_file.h"
#include "tmp/gcache_tmp_reader.h"
#include "tmp/gcache_tmp_writer.h"
#include "tmp/gcache_tmp_kvtable.h"

using namespace seisfs;
using namespace boost;

StorageArch storageArch = STORAGE_LOCAL;

BOOST_AUTO_TEST_SUITE(tmp_local_test)

BOOST_AUTO_TEST_CASE(tmp_local_1) {
    Dir *dir = Dir::New("", storageArch);
    BOOST_REQUIRE_EQUAL(dir->Remove(), true);
    delete dir;
}

BOOST_AUTO_TEST_CASE(tmp_local_2) {
    File *file = File::New("", storageArch);
    delete file;
}

BOOST_AUTO_TEST_CASE(tmp_local_3) {
    Dir *dir = Dir::New("/folder", storageArch);
    BOOST_REQUIRE_MESSAGE(dir!=NULL, "dir is null");
    BOOST_REQUIRE_EQUAL(dir->Exist(), false);
    BOOST_REQUIRE_EQUAL(dir->MakeDirs(), true);
    BOOST_REQUIRE_EQUAL(dir->Exist(), true);
    BOOST_REQUIRE_EQUAL(dir->Rename("/newfolder"), true);
    BOOST_REQUIRE_EQUAL(dir->Rename("/mydocument/newfolder"), false);
    BOOST_REQUIRE_EQUAL(dir->Remove(), true);
    delete dir;
}

BOOST_AUTO_TEST_CASE(tmp_local_4) {
    Dir *dir = Dir::New("/folder", storageArch);
    BOOST_REQUIRE_MESSAGE(dir!=NULL, "dir is null");
    BOOST_REQUIRE_EQUAL(dir->Exist(), false);
    BOOST_REQUIRE_EQUAL(dir->MakeDirs(), true);
    BOOST_REQUIRE_EQUAL(dir->Exist(), true);

    File *file = File::New("/folder/data", storageArch);
    BOOST_REQUIRE_MESSAGE(file!=NULL, "file is null");
    Writer* writer = file->OpenWriter(10, CACHE_L1, FADV_NONE);
    BOOST_REQUIRE_MESSAGE(writer!=NULL, "writer is null");
    char data[] = { "hello world!" };
    BOOST_REQUIRE_EQUAL(writer->Write(data, strlen(data)), strlen(data));
    delete writer;
    delete file;

    BOOST_REQUIRE_EQUAL(dir->Remove(true), true);
    delete dir;
}

BOOST_AUTO_TEST_CASE(tmp_local_5) {

    Dir *dir = Dir::New("/folder", storageArch);
    BOOST_REQUIRE_MESSAGE(dir!=NULL, "dir is null");
    BOOST_REQUIRE_EQUAL(dir->Exist(), false);
    BOOST_REQUIRE_EQUAL(dir->MakeDirs(), true);
    BOOST_REQUIRE_EQUAL(dir->Exist(), true);

    File *file = File::New("/folder/data", storageArch);
    BOOST_REQUIRE_MESSAGE(file!=NULL, "file is null");
    BOOST_REQUIRE_EQUAL(file->Exists(), false);
    Writer* writer = file->OpenWriter(10, CACHE_L1, FADV_NONE);
    BOOST_REQUIRE_MESSAGE(writer!=NULL, "writer is null");
    int64_t datLen = 1024 * 1024 * 1024 * 1.5;    //1.5GB
    char* data = new char[datLen];
    for (int i = 0; i < datLen; ++i) {
        data[i] = i % 127;
    }
    for (int i = 0; i < 3; ++i) {        //写入4.5GB
        BOOST_REQUIRE_EQUAL(writer->Write(data, datLen), datLen);
    }
    BOOST_REQUIRE_EQUAL(file->Copy("/folder/dataInSameLevel"), true);
    BOOST_REQUIRE_EQUAL(file->Copy("/folder/dataInAnotherLevel", CACHE_L2), true);

    delete data;
    delete writer;
    delete file;

    BOOST_REQUIRE_EQUAL(dir->Remove(true), true);
    delete dir;
}

BOOST_AUTO_TEST_CASE(tmp_local_6) {
    Dir *dir = Dir::New("/folder", storageArch);
    BOOST_REQUIRE_MESSAGE(dir!=NULL, "dir is null");
    BOOST_REQUIRE_EQUAL(dir->Exist(), false);
    BOOST_REQUIRE_EQUAL(dir->MakeDirs(), true);
    BOOST_REQUIRE_EQUAL(dir->Exist(), true);

    File *file = File::New("/folder/data", storageArch);
    BOOST_REQUIRE_MESSAGE(file!=NULL, "file is null");
    BOOST_REQUIRE_EQUAL(file->Exists(), false);

    Writer* writer = file->OpenWriter(10, CACHE_L1, FADV_NONE);
    BOOST_REQUIRE_MESSAGE(writer!=NULL, "writer is null");
    char *data = "hello,world!";
    BOOST_REQUIRE_EQUAL(writer->Write(data, strlen(data)), strlen(data));
    delete writer;
    delete file;

    Dir *newdir = Dir::New("/newfolder", storageArch);
    BOOST_REQUIRE_MESSAGE(newdir!=NULL, "newdir is null");
    BOOST_REQUIRE_EQUAL(newdir->Exist(), false);
    BOOST_REQUIRE_EQUAL(newdir->MakeDirs(), true);
    BOOST_REQUIRE_EQUAL(newdir->Exist(), true);
    BOOST_REQUIRE_EQUAL(dir->Move("/newfolder"), true);

    delete dir;
    BOOST_REQUIRE_EQUAL(newdir->Remove(true), true);
    delete newdir;
}

BOOST_AUTO_TEST_CASE(tmp_local_7) {
    Dir *dirFolder = Dir::New("/folder", storageArch);
    BOOST_REQUIRE_MESSAGE(dirFolder!=NULL, "dirFolder is null");
    BOOST_REQUIRE_EQUAL(dirFolder->MakeDirs(), true);

    Dir *dir = Dir::New("/folder/ice", storageArch);
    BOOST_REQUIRE_MESSAGE(dir!=NULL, "dir is null");
    BOOST_REQUIRE_EQUAL(dir->MakeDirs(), true);

    File *file = File::New("/folder/data", storageArch);
    BOOST_REQUIRE_MESSAGE(file!=NULL, "file is null");

    Writer* writer = file->OpenWriter(10, CACHE_L1, FADV_NONE);
    BOOST_REQUIRE_MESSAGE(writer!=NULL, "writer is null");

    char *data = "hello,world!";

    BOOST_REQUIRE_EQUAL(writer->Write(data, strlen(data)), 12);

    File *newfile = File::New("/folder/ice/data1", storageArch);
    BOOST_REQUIRE_MESSAGE(newfile!=NULL, "newfile is null");

    Writer* newwriter = newfile->OpenWriter(10, CACHE_L1, FADV_NONE);
    BOOST_REQUIRE_MESSAGE(newwriter!=NULL, "newwriter is null");

    char *newdata = "new hello,world!";

    BOOST_REQUIRE_EQUAL(newwriter->Write(newdata, strlen(newdata)), 16);

    Dir *newdir = Dir::New("/newfolder", storageArch);
    BOOST_REQUIRE_MESSAGE(newdir!=NULL, "newdir is null");
    BOOST_REQUIRE_EQUAL(newdir->MakeDirs(), true);

    BOOST_REQUIRE_EQUAL(dirFolder->Copy("/newfolder"), true);

    BOOST_REQUIRE_EQUAL(newdir->Remove(true), true);

    delete newdir;
    delete dirFolder;
    delete dir;
    delete file;
    delete writer;
    delete newwriter;
    delete newfile;

}

BOOST_AUTO_TEST_CASE(tmp_local_8) {
    Dir *dirFolder = Dir::New("/folder", storageArch);
    BOOST_REQUIRE_MESSAGE(dirFolder!=NULL, "dirFolder is null");
    BOOST_REQUIRE_EQUAL(dirFolder->MakeDirs(), true);

    File *file = File::New("/folder/data", storageArch);
    BOOST_REQUIRE_MESSAGE(file!=NULL, "file is null");

    Writer* writer = file->OpenWriter(10, CACHE_L1, FADV_NONE);
    BOOST_REQUIRE_MESSAGE(writer!=NULL, "writer is null");

    char *data = "hello,world!";
    BOOST_REQUIRE_EQUAL(writer->Write(data, strlen(data)), strlen(data));

    BOOST_REQUIRE_EQUAL(writer->Truncate(4), true);
    BOOST_REQUIRE_EQUAL(file->Size(), 4);

    BOOST_REQUIRE_EQUAL(dirFolder->Remove(true), true);
    delete dirFolder;
    delete file;
    delete writer;
}

BOOST_AUTO_TEST_CASE(tmp_local_9) {
    Dir *dirFolder = Dir::New("/folder", storageArch);
    BOOST_REQUIRE_MESSAGE(dirFolder!=NULL, "dirFolder is null");
    BOOST_REQUIRE_EQUAL(dirFolder->MakeDirs(), true);

    File *file = File::New("/folder/data", storageArch);
    BOOST_REQUIRE_MESSAGE(file!=NULL, "file is null");

    Writer* writer = file->OpenWriter(10, CACHE_L1, FADV_NONE);
    BOOST_REQUIRE_MESSAGE(writer!=NULL, "writer is null");

    char *data = "hello,world!\0";
    char *newdata = "012345678901\0";
    BOOST_REQUIRE_EQUAL(writer->Write(data, 13), 13);

    BOOST_REQUIRE_EQUAL(writer->Seek(0, SEEK_SET), true);
    BOOST_REQUIRE_EQUAL(writer->Write(newdata, 13), 13);

    Reader* reader = file->OpenReader(FADV_NONE);
    BOOST_REQUIRE_MESSAGE(reader!=NULL, "reader is null");

    char *readData = new char[12];
    BOOST_REQUIRE_EQUAL(reader->Read(readData, 13), 13);
    BOOST_REQUIRE_EQUAL(strcmp(readData, newdata), 0);

    char *readData1 = new char[12];
    BOOST_REQUIRE_EQUAL(reader->Seek(1, SEEK_SET), true);
    BOOST_REQUIRE_EQUAL(reader->ReadLine(readData1, 12), 11);
    BOOST_REQUIRE_EQUAL(strcmp(readData1, newdata + 1), 0);

    std::string readStr;
    BOOST_REQUIRE_EQUAL(reader->Seek(0, SEEK_SET), true);
    BOOST_REQUIRE_EQUAL(reader->ReadLine(readStr), 13);
    BOOST_REQUIRE_EQUAL(strcmp(readStr.c_str(), newdata), 0);

    delete file;
    delete writer;
    delete reader;
    delete readData;
    delete readData1;

    BOOST_REQUIRE_EQUAL(dirFolder->Remove(true), true);
    delete dirFolder;
}

BOOST_AUTO_TEST_CASE(tmp_local_10) {
    Dir *dirFolder = Dir::New("/folder", storageArch);
    BOOST_REQUIRE_MESSAGE(dirFolder!=NULL, "dirFolder is null");
    BOOST_REQUIRE_EQUAL(dirFolder->MakeDirs(), true);

    File *file = File::New("/folder/data", storageArch);
    BOOST_REQUIRE_MESSAGE(file!=NULL, "file is null");
    Writer* writer = file->OpenWriter(10, CACHE_L1, FADV_NONE);
    BOOST_REQUIRE_MESSAGE(writer!=NULL, "writer is null");

    char *data = "hello,world!";
    char *newdata = "012345678901";

    BOOST_REQUIRE_EQUAL(writer->Write(data, strlen(data)), 12);
    BOOST_REQUIRE_EQUAL(writer->Seek(0, SEEK_SET), true);
    BOOST_REQUIRE_EQUAL(writer->Write(newdata, strlen(newdata)), 12);

    std::string fileName = file->GetName();
    printf("file name is %s\n", fileName.c_str());

    BOOST_REQUIRE_EQUAL(file->Lifetime(), 10);
    BOOST_REQUIRE_EQUAL(file->SetLifetime(20), true);
    BOOST_REQUIRE_EQUAL(file->Lifetime(), 20);
    BOOST_REQUIRE_EQUAL(file->LifetimeExpired(), false);

    int64_t time_ms = file->LastModified();
    printf("last modification is %ld ms.\n", time_ms);

    delete file;
    delete writer;

    BOOST_REQUIRE_EQUAL(dirFolder->Remove(true), true);
    delete dirFolder;
}

BOOST_AUTO_TEST_CASE(tmp_local_11) {
    KVTable *table = KVTable::New("TravelTime", storageArch);
    BOOST_REQUIRE_MESSAGE(table!=NULL, "table is null");
    BOOST_REQUIRE_EQUAL(table->Open(30, CMP_NON, CACHE_L1), true);

    for (int i = 0; i < 1000; ++i) {
        char key[10];
        char tmp_buf[1024];
        sprintf(key, "key %d", i);
        sprintf(tmp_buf, "%d hello world!", i);
        BOOST_REQUIRE_EQUAL(table->Put(key, tmp_buf, strlen(tmp_buf) + 1), true);
    }
    char *data;
    int64_t data_size;
    for (int i = 0; i < 1000; ++i) {
        char key[10];
        sprintf(key, "key %d", i);
        BOOST_REQUIRE_EQUAL(table->Get(key, data, data_size), true);
        printf("key=%s,data=%s,dataSize=%ld\n", key, data, data_size);
    }

    std::list<std::string> key_list = table->GetKeys();
    printf("key_list size %d\n", key_list.size());
    for (std::list<std::string>::iterator list_iter = key_list.begin(); list_iter != key_list.end();
            list_iter++) {
        printf("key=%s\n", (*list_iter).c_str());
    }

    BOOST_REQUIRE_EQUAL(table->Remove(), true);
    delete table;
}

BOOST_AUTO_TEST_CASE(tmp_local_12) {
    KVTable *table = KVTable::New("TravelTime", storageArch);
    BOOST_REQUIRE_MESSAGE(table!=NULL, "table is null");
    BOOST_REQUIRE_EQUAL(table->Open(30, CMP_NON, CACHE_L1), true);

    for (int i = 0; i < 1000; ++i) {
        char key[10];
        char tmp_buf[1024];
        sprintf(key, "key %d", i);
        sprintf(tmp_buf, "%d hello world!", i);
        BOOST_REQUIRE_EQUAL(table->Put(key, tmp_buf, strlen(tmp_buf) + 1), true);
    }
    int64_t data_size;
    for (int i = 0; i < 990; ++i) {
        char key[10];
        sprintf(key, "key %d", i);
        BOOST_REQUIRE_EQUAL(table->Delete(key), true);
    }

    std::list<std::string> key_list = table->GetKeys();
    printf("key_list size %d\n", key_list.size());
    for (std::list<std::string>::iterator list_iter = key_list.begin(); list_iter != key_list.end();
            list_iter++) {
        char *data;
        int64_t dataSize;
        BOOST_REQUIRE_EQUAL(table->Get(*list_iter, data, dataSize), true);
        printf("key=%s,data=%s,dataSize=%d\n", list_iter->c_str(), data, dataSize);
        delete data;
    }

    BOOST_REQUIRE_EQUAL(table->Remove(), true);
    delete table;
}

BOOST_AUTO_TEST_CASE(tmp_local_13) {
    KVTable *table = KVTable::New("TravelTime", storageArch);
    BOOST_REQUIRE_MESSAGE(table!=NULL, "table is null");
    BOOST_REQUIRE_EQUAL(table->Open(30, CMP_NON, CACHE_L1), true);
    BOOST_REQUIRE_EQUAL(table->Exists(), true);

    std::string tableName = table->GetName();
    printf("tableName is %s\n", tableName.c_str());
    table->Rename("NewTravelTime");
    tableName = table->GetName();
    printf("table new name is %s\n", tableName.c_str());
    BOOST_REQUIRE_EQUAL(table->Remove(), true);
    BOOST_REQUIRE_EQUAL(table->Exists(), false);
    delete table;
}

BOOST_AUTO_TEST_CASE(tmp_local_14) {
    KVTable *table = KVTable::New("TravelTime", storageArch);
    BOOST_REQUIRE_MESSAGE(table!=NULL, "table is null");
    BOOST_REQUIRE_EQUAL(table->Open(30, CMP_NON, CACHE_L1), true);

    for (int i = 0; i < 1000; ++i) {
        char key[10];
        char tmp_buf[1024];
        sprintf(key, "key %d", i);
        sprintf(tmp_buf, "%d hello world!", i);
        BOOST_REQUIRE_EQUAL(table->Put(key, tmp_buf, strlen(tmp_buf) + 1), true);
    }

    int days = table->Lifetime();
    printf("table lifetime is %d days\n", days);
    BOOST_REQUIRE_EQUAL(table->SetLifetime(10), true);
    days = table->Lifetime();
    printf("table lifetime is %d days\n", days);
    BOOST_REQUIRE_EQUAL(table->LifetimeExpired(), false);
    int64_t tableSize = table->Size();
    printf("the size of table is %ld bytes\n", tableSize);
    int64_t sizeOfKey = table->SizeOf("key 100");
    printf("the size of key 100 is %ld bytes\n", sizeOfKey);

    delete table;

}

BOOST_AUTO_TEST_CASE(tmp_local_15) {
    KVTable *table = KVTable::New("TravelTime", storageArch);
    BOOST_REQUIRE_MESSAGE(table!=NULL, "table is null");
    BOOST_REQUIRE_EQUAL(table->Open(30, CMP_GZIP, CACHE_L1), true);

    for (int i = 0; i < 1000; ++i) {
        char key[10];
        char tmp_buf[1024];
        sprintf(key, "key %d", i);
        sprintf(tmp_buf, "%d hello world!", i);
        BOOST_REQUIRE_EQUAL(table->Put(key, tmp_buf, strlen(tmp_buf) + 1), true);
    }
    char *data;
    int64_t data_size;
    for (int i = 0; i < 1000; ++i) {
        char key[10];
        sprintf(key, "key %d", i);
        BOOST_REQUIRE_EQUAL(table->Get(key, data, data_size), true);
        printf("key=%s,data=%s,dataSize=%ld\n", key, data, data_size);
    }

    std::list<std::string> key_list = table->GetKeys();
    printf("key_list size %d\n", key_list.size());
    for (std::list<std::string>::iterator list_iter = key_list.begin(); list_iter != key_list.end();
            list_iter++) {
        printf("key=%s\n", (*list_iter).c_str());
    }

    BOOST_REQUIRE_EQUAL(table->Remove(), true);
    delete table;
}

BOOST_AUTO_TEST_CASE(tmp_local_16) {
    KVTable *table = KVTable::New("TravelTime", storageArch);
    BOOST_REQUIRE_MESSAGE(table!=NULL, "table is null");
    BOOST_REQUIRE_EQUAL(table->Open(30, CMP_GZIP, CACHE_L1), true);

    for (int i = 0; i < 1000; ++i) {
        char key[10];
        char tmp_buf[1024];
        sprintf(key, "key %d", i);
        sprintf(tmp_buf, "%d hello world!", i);
        BOOST_REQUIRE_EQUAL(table->Put(key, tmp_buf, strlen(tmp_buf) + 1), true);
    }
    int64_t data_size;
    for (int i = 0; i < 990; ++i) {
        char key[10];
        sprintf(key, "key %d", i);
        BOOST_REQUIRE_EQUAL(table->Delete(key), true);
    }

    std::list<std::string> key_list = table->GetKeys();
    printf("key_list size %d\n", key_list.size());
    for (std::list<std::string>::iterator list_iter = key_list.begin(); list_iter != key_list.end();
            list_iter++) {
        char *data;
        int64_t dataSize;
        BOOST_REQUIRE_EQUAL(table->Get(*list_iter, data, dataSize), true);
        printf("key=%s,data=%s,dataSize=%d\n", list_iter->c_str(), data, dataSize);
        delete data;
    }

    BOOST_REQUIRE_EQUAL(table->Remove(), true);
    delete table;
}

BOOST_AUTO_TEST_CASE(tmp_local_17) {
    KVTable *table = KVTable::New("TravelTime", storageArch);
    BOOST_REQUIRE_MESSAGE(table!=NULL, "table is null");
    BOOST_REQUIRE_EQUAL(table->Open(30, CMP_GZIP, CACHE_L1), true);
    BOOST_REQUIRE_EQUAL(table->Exists(), true);

    std::string tableName = table->GetName();
    printf("tableName is %s\n", tableName.c_str());
    table->Rename("NewTravelTime");
    tableName = table->GetName();
    printf("table new name is %s\n", tableName.c_str());
    BOOST_REQUIRE_EQUAL(table->Remove(), true);
    BOOST_REQUIRE_EQUAL(table->Exists(), false);
    delete table;
}

BOOST_AUTO_TEST_CASE(tmp_local_18) {
    KVTable *table = KVTable::New("TravelTime", storageArch);
    BOOST_REQUIRE_MESSAGE(table!=NULL, "table is null");
    BOOST_REQUIRE_EQUAL(table->Open(30, CMP_GZIP, CACHE_L1), true);

    for (int i = 0; i < 1000; ++i) {
        char key[10];
        char tmp_buf[1024];
        sprintf(key, "key %d", i);
        sprintf(tmp_buf, "%d hello world!", i);
        BOOST_REQUIRE_EQUAL(table->Put(key, tmp_buf, strlen(tmp_buf) + 1), true);
    }

    int days = table->Lifetime();
    printf("table lifetime is %d days\n", days);
    BOOST_REQUIRE_EQUAL(table->SetLifetime(10), true);
    days = table->Lifetime();
    printf("table lifetime is %d days\n", days);
    BOOST_REQUIRE_EQUAL(table->LifetimeExpired(), false);
    int64_t tableSize = table->Size();
    printf("the size of table is %ld bytes\n", tableSize);
    int64_t sizeOfKey = table->SizeOf("key 100");
    printf("the size of key 100 is %ld bytes\n", sizeOfKey);

    delete table;

}

BOOST_AUTO_TEST_CASE(tmp_local_19) {
    char tmp_buf_none[5000];
    char tmp_buf_raw[5000];
    char tmp_buf_random[5000];
    char tmp_buf_sequential[5000];
    char tmp_buf_mmap[5000];

    File *file = File::New("/data", storageArch);
    BOOST_REQUIRE_MESSAGE(file!=NULL, "file is null");
    Writer* writer = file->OpenWriter(10);
    BOOST_REQUIRE_MESSAGE(writer!=NULL, "writer is null");
    char data[5000];
    for (int i = 0; i < 5000; ++i) {
        data[i] = i % 128;
    }
    BOOST_REQUIRE_EQUAL(writer->Write(data, 5000), 5000);

    Reader *reader_none = file->OpenReader(FADV_NONE);
    Reader *reader_raw = file->OpenReader(FADV_RAW);
    Reader *reader_random = file->OpenReader(FADV_RANDOM);
    Reader *reader_sequential = file->OpenReader(FADV_SEQUENTIAL);
    Reader *reader_mmap = file->OpenReader(FADV_MMAP);

    reader_none->Read(tmp_buf_none, 100);
    reader_raw->Read(tmp_buf_raw, 100);
    reader_random->Read(tmp_buf_random, 100);
    reader_sequential->Read(tmp_buf_sequential, 100);
    reader_mmap->Read(tmp_buf_mmap, 100);

    for (int i = 0; i < 100; ++i) {
        if (tmp_buf_none[i] != tmp_buf_raw[i] || tmp_buf_none[i] != tmp_buf_random[i]
                || tmp_buf_none[i] != tmp_buf_sequential[i] || tmp_buf_none[i] != tmp_buf_mmap[i]) {
            BOOST_REQUIRE_EQUAL(true, false);
        }
    }

    reader_none->Read(tmp_buf_none, 1000);
    reader_raw->Read(tmp_buf_raw, 1000);
    reader_random->Read(tmp_buf_random, 1000);
    reader_sequential->Read(tmp_buf_sequential, 1000);
    reader_mmap->Read(tmp_buf_mmap, 1000);

    for (int i = 0; i < 100; ++i) {
        if (tmp_buf_none[i] != tmp_buf_raw[i] || tmp_buf_none[i] != tmp_buf_random[i]
                || tmp_buf_none[i] != tmp_buf_sequential[i] || tmp_buf_none[i] != tmp_buf_mmap[i]) {
            BOOST_REQUIRE_EQUAL(true, false);
        }
    }

    reader_raw->Seek(20, SEEK_CUR);
    reader_none->Seek(20, SEEK_CUR);
    reader_random->Seek(20, SEEK_CUR);
    reader_sequential->Seek(20, SEEK_CUR);
    reader_mmap->Seek(20, SEEK_CUR);

    reader_none->Read(tmp_buf_none, 1000);
    reader_raw->Read(tmp_buf_raw, 1000);
    reader_random->Read(tmp_buf_random, 1000);
    reader_sequential->Read(tmp_buf_sequential, 1000);
    reader_mmap->Read(tmp_buf_mmap, 1000);

    for (int i = 0; i < 100; ++i) {
        if (tmp_buf_none[i] != tmp_buf_raw[i] || tmp_buf_none[i] != tmp_buf_random[i]
                || tmp_buf_none[i] != tmp_buf_sequential[i] || tmp_buf_none[i] != tmp_buf_mmap[i]) {
            BOOST_REQUIRE_EQUAL(true, false);
        }
    }
    delete reader_none;
    delete reader_raw;
    delete reader_random;
    delete reader_sequential;
    delete reader_mmap;
    delete file;
}

BOOST_AUTO_TEST_SUITE_END()

