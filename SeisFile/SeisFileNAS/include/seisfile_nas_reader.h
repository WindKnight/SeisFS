/*
 * seisfile_reader.h
 *
 *  Created on: Mar 26, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_NAS_READER_H_
#define SEISFILE_NAS_READER_H_
#include <stdint.h>
#include <GEFile.h>
#include <seisfs_head_filter.h>
#include <seisfs_row_filter.h>
#include <seisfile_reader.h>
#include <seisfile_nas_meta.h>

namespace seisfs {

class RowFilter;
class HeadFilter;
namespace file {


class MetaNAS;
class Headtype;
class Tracetype;

//static int64_t _last_trace_num;




class ReaderNAS : public Reader {
public:
	virtual ~ReaderNAS();

	//ReaderNAS(const std::string& filename);
	ReaderNAS(const std::string& filename, MetaNAS *meta);


    bool SetHeadFilter(const HeadFilter& head_filter);


    bool SetRowFilter(const RowFilter& row_filter);

    uint64_t GetTraceNum(); // return trace number;

    int GetTraceSize(); //return length of one trace, in bytes

    int GetHeadSize();//return length of one head, in bytes

    bool Get(int64_t trace_num, void *head, void* trace);

    /**
     * Move the read offset to trace_idx(th) trace.
     */

    int64_t Tell();

    bool Seek(int64_t trace_num);
    /**
     * Return true if there are more traces. The routine usually accompany with NextTrace
     */
    bool HasNext();

    bool Next(void* head, void* trace);

    bool Close();

    bool Init();

private:
    int64_t GetRealNum(int64_t trace_num);

    HeadFilter _head_filter;
    RowFilter _row_filter;

    int64_t _cur_logic_num;
    //int64_t _actual_trace_num;
    int64_t _last_actual_num;

    std::string _head_filename;
    std::string _trace_filename;
    std::string _meta_filename;
    std::string _filename;
    std::string seisnas_home_path_;
    GEFile *_gf_head, *_gf_trace;
    GEFile *_gf_head_next, *_gf_trace_next;
    HeadType _head_type;
    TraceType _trace_type;
    Lifetime _lifetime_days;
    MetaNAS *_meta;

    //static int64_t _last_trace_num;

};
//int64_t ReaderNAS::_last_trace_num = 0;
}

}

#endif /* SEISFILE_NAS_READER_H_ */
