/*
 * seisfile_tracewriter.h
 *
 *  Created on: Mar 26, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_HDFS_TRACEWRITER_H_
#define SEISFILE_HDFS_TRACEWRITER_H_

#include <stdint.h>
#include <GEFile.h>
#include "seisfile_hdfs_meta.h"
#include "seisfile_tracewriter.h"


namespace seisfs {

namespace file {

class TraceWriterHDFS : public TraceWriter{
public:

    /**
     * Destructor.
     */
	virtual ~TraceWriterHDFS();

	//TraceWriterHDFS(const TraceType &trace_type,const std::string& filename, Lifetime lifetime_days);
	TraceWriterHDFS(const std::string& filename, MetaHDFS *meta);

	bool Init();

    /**
     * Append a trace.
     */
	bool Write(const void* trace);

    /**
     * Returns the position that data is written to.
     */
	int64_t Pos();

    /**
     * Transfers all modified in-core data of the file to the disk device where that file resides.
     */
	bool Sync();

    /**
     *  Close file.
     */
	bool Close();

	bool Truncate(int64_t traces);

private:
	std::string _trace_filename;
	//std::string _meta_filename;
	std::string _filename;
//	const std::string SEISHDFS_HOME_PATH = "/d0/data/seisfiletest/seisfiledisk/";
	std::string seisnas_home_path_;
//	GEFile *_gf_trace;
	int fd_trace_;
	TraceType _trace_type;
	Lifetime _lifetime_days;
	MetaHDFS *_meta;

};

}

}

#endif /* SEISFILE_HDFS_TRACEWRITER_H_ */
