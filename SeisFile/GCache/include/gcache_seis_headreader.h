/*
 * gcache_seis_headreader.h
 *
 *  Created on: Jun 25, 2018
 *      Author: wyd
 */

#ifndef GCACHE_SEIS_HEADREADER_H_
#define GCACHE_SEIS_HEADREADER_H_


#include "seisfile_headreader.h"
#include <vector>
#include <string>


namespace seisfs {
namespace file {

class SeisHeadReader : public HeadReader {

public:

    virtual ~SeisHeadReader(){}

    /*
     * caller should not delete the pointer of filter after calling this function
     */
    virtual bool SetHeadFilter(const HeadFilter& head_filter) = 0;

    /*
     * caller should not delete the pointer of filter after calling this function
     */
    virtual bool SetRowFilter(const RowFilter& row_filter) = 0;

    virtual int GetHeadSize() = 0; //return length of one head, in bytes

    virtual int64_t GetTraceNum() = 0;

    /**
     * Move the read offset to head_idx(th) head.
     */
    virtual bool Seek(int64_t head_idx) = 0;

    virtual int64_t Tell() = 0;

    virtual bool Get(int64_t head_idx, void* head) = 0;

    /**
     * Return true if there are more heads. The routine usually accompany with NextHead
     */
    virtual bool HasNext() = 0;

    virtual bool Next(void* head) = 0;

    virtual bool Close() = 0;

    virtual bool GetLocations(const std::vector<int64_t> &head_id,
                std::vector<std::vector<std::string> > &hostInfos) = 0;

private:


};

}


}


#endif /* GCACHE_SEIS_HEADREADER_H_ */
