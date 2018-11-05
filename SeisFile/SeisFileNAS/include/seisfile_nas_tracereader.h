/*
 * seisfile_tracereader.h
 *
 *  Created on: Mar 26, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_NAS_TRACEREADER_H_
#define SEISFILE_NAS_TRACEREADER_H_

#include "seisfile_tracereader.h"
#include <GEFile.h>
#include <seisfs_head_filter.h>
#include <seisfs_row_filter.h>
#include <seisfile_nas_meta.h>

namespace seisfs {

class RowFilter;

namespace file {

class MetaNAS;
class Tracetype;

class TraceReaderNAS : public TraceReader{
public:

    /**
     * Destructor.
     */
	virtual ~TraceReaderNAS();

	//TraceReaderNAS(const std::string& filename);
	TraceReaderNAS(const std::string& filename, MetaNAS *meta);

    /*
     * caller should not delete the pointer of filter after calling this function
     */
    bool SetRowFilter(const RowFilter& row_filter);

    /**
     * Get the total trace rows of seis.
     */
	int64_t GetTraceNum();

    /**
     * Get the trace length.
     */
	int64_t GetTraceSize();//return length of one trace, in bytes

    /**
     * Move the read offset to trace_idx(th) trace.
     */
	bool Seek(int64_t trace_num);

	int64_t Tell();

    /**
     * Get a trace by trace index.
     */
	bool Get(int64_t trace_num, void* trace);

    /**
     * Return true if there are more traces. The routine usually accompany with NextTrace.
     */
	bool HasNext();

    /**
     * Get a trace by filter.
     */
	bool Next(void* trace);

    /**
     * Close file.
     */
	bool Close();

	bool Init();

private:
	int64_t GetRealNum(int64_t trace_num);

	RowFilter _row_filter;

	int64_t _cur_logic_num;
	int64_t _last_actual_num;

	std::string _trace_filename;
	std::string _meta_filename;
	std::string _filename;
//	const std::string SEISNAS_HOME_PATH = "/d0/data/seisfiletest/seisfiledisk/";
	std::string seisnas_home_path_;
	GEFile *_gf_trace;
    GEFile *_gf_trace_next;

	TraceType _trace_type;
	Lifetime _lifetime_days;
	MetaNAS *_meta;

};

}

}

#endif /* SEISFILE_NAS_TRACEREADER_H_ */
