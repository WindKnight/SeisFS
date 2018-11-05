/*
 * utils.cpp
 *
 *  Created on: Jun 12, 2016
 *      Author: neo
 */

#include "gcache_hdfs_utils.h"
#include "gcache_string.h"
#include "gcache_log.h"

hdfsFS getHDFS(std::string path) {
    std::string uri = path;
    std::string new_uri = "";
    struct hdfsBuilder *builder = hdfsNewBuilder();
    new_uri = Replace(uri, '/', ':');
    std::vector<std::string> uri_split = SplitString(new_uri, ":");
    if (uri_split.size() < 4) {
        Err("wrong path when getHDFS");
        return NULL;
    }
    std::string protocol = uri_split[0];
    std::string address = uri_split[1];
    std::string port = uri_split[2];
    std::string dir_name = uri_split[3];
    hdfsBuilderSetNameNode(builder, address.c_str());
    hdfsBuilderSetNameNodePort(builder, ToInt64(port));
//        hdfsBuilderSetUserName(builder,"wzb");
    hdfsFS fs = hdfsBuilderConnect(builder);
    //输出连接信息
//        std::string erro = hdfsGetLastError();
//        printf("hdfsBuilderConnect to Path------->");
//        printf("%s\n",path.c_str());
//        printf("hdfsBuilderConnect-------->%s\n",erro.c_str());
    return fs;

}

char* hdfsFgets(char* buffer, int32_t size, hdfsFS fs, hdfsFile file) {
    register char *cs;
    cs = buffer;
    register char read[1];
    while (--size > 0 && (hdfsRead(fs, file, read, 1) > 0)) {
        if ((*cs++ = read[0]) == '\n') break;

    }
    *cs = '\0';
    return (cs == buffer) ? NULL : buffer;

}

std::string hdfsGetPath(std::string path) {
    std::string para_filename = path;
    int pos = para_filename.find("://");
    if (pos == -1) {
        Err("wrong path when hdfsGetPath");
        return "";
    }
    if (para_filename.size() <= (unsigned int) (pos + 3)) {
        Err("wrong path when hdfsGetPath");
        return "";
    }
    std::string new_para_filename = para_filename.substr(pos + 3);

    int path_pos = new_para_filename.find("/");
    if (path_pos == -1) {
        Err("wrong path when hdfsGetPath");
        return "";
    }
    if (new_para_filename.size() <= (unsigned int) path_pos) {
        Err("wrong path when hdfsGetPath");
        return "";
    }
    std::string path_ret = new_para_filename.substr(path_pos);
    return path_ret;

}

int hdfsTruncateWait(hdfsFS fs, const char *path, tOffset pos) {
    int shouldWait;
    if (-1 == hdfsTruncate(fs, path, pos, &shouldWait)) {
        return -1;
    }

    hdfsFile fd = NULL;
    Warn("Sleep for Truncate to affect");
    int max_error = 0;
    while (fd == NULL) {
        fd = hdfsOpenFile(fs, path, O_WRONLY | O_APPEND, 0, REPLICATION, 0);
        sleep(1);
        max_error++;
        if (max_error > MAX_TRUNCATE_WAIT) {
            Err("Sleep for Truncate reach maximum time.");
            return -1;
        }
    }
    hdfsCloseFile(fs, fd);
    sleep(1);
    return 0;
}
