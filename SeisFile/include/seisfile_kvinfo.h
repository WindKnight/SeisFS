/*
 * seisfile_kvinfo.h
 *
 *  Created on: Mar 26, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_KVINFO_H_
#define SEISFILE_KVINFO_H_

#include <stdint.h>
#include <string>
#include <vector>

namespace seisfs {

namespace file {

class KVInfo {
public:

    virtual ~KVInfo(){};

    virtual bool Put(const std::string &key, const char* data, uint32_t size) = 0;

    virtual bool Delete(const std::string &key) = 0;

    virtual bool Get(const std::string &key, char*& data, uint32_t& size) = 0;

    virtual uint32_t SizeOf(const std::string &key) = 0;

    virtual bool Exists(const std::string &key) = 0;

    virtual bool GetKeys(std::vector<std::string> &Keys) = 0;

private:

};

}

}

#endif /* SEISFILE_KVINFO_H_ */
