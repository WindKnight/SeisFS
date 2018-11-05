/*
 * gcache_tmp_file_utils.h
 *
 *  Created on: Jan 19, 2016
 *      Author: zch
 */

#ifndef GCACHE_TMP_FILE_UTILS_H_
#define GCACHE_TMP_FILE_UTILS_H_

#include <sys/types.h>
#include <list>
#include <string>

#include "gcache_global.h"
#include "gcache_advice.h"

NAMESPACE_BEGIN_SEISFS
namespace tmp {

class File;

class FileUtils {
public:
    static FileUtils* GetInstance(StorageArch s_arch);

    bool MakeDirs(const std::string& path);

    bool Remove(const std::string& windcard_path, bool recursive = true);

    std::list<std::string> ListFiles(const std::string& path, const std::string& wildcard_filter);

    std::list<std::string> ListDirs(const std::string& path, const std::string& wildcard_filter);

    bool IsDir(const std::string& dir_name);

    bool IsFile(const std::string& file_name);

    bool IsKVTable(const std::string& kvtable_name);

    int64_t SizeOf(const std::string& name);

    void Touch(const std::string& name);

    bool Move(const std::string& src_name, const std::string& dest_name);

    bool Copy(const std::string& src_name, const std::string& dest_name);

    static bool Copy(const File* src_file, const File* dest_file);

private:

    FileUtils() {
    }
    ;

    static FileUtils* _fileUtils;

};

NAMESPACE_END_SEISFS
}

#endif /* GCACHE_TMP_FILE_UTILS_H_ */
