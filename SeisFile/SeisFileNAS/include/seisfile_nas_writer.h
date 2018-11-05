/*
 * seisfile_writer.h
 *
 *  Created on: Mar 26, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_NAS_WRITER_H_
#define SEISFILE_NAS_WRITER_H_

#include <stdint.h>
#include <seisfile_nas_meta.h>
#include "seisfile_writer.h"
#include "GEFile.h"



namespace seisfs {

namespace file {

class HeadWriterNAS;
class TraceWriterNAS;
class MetaNAS;
//class GEFile;

//static int64_t tracenum_count = 0;
//static int64_t total_trace_num;

class WriterNAS : public Writer{
public:

	//WriterNAS(const std::string& filename, MetaNAS *meta);
	//WriterNAS(const std::string& filename);
	 //WriterNAS(const std::string& filename, const HeadType &head_type, const TraceType& trace_type, Lifetime lifetime_days);

	 WriterNAS(const std::string& filename, MetaNAS *meta);



     virtual ~WriterNAS();

    /**
     * Append a trace head
     */
     bool Write(const void* head, const void* trace);

     bool Init();

    /**
     * Returns the position that data is written to.
     */
     int64_t Pos();

    /**
     * Transfers all modified in-core data of the file to the disk device where that file resides.
     */
     bool Sync();

    /**
     * Close file.
     */
     bool Close();

     bool Truncate(int64_t trace_num);

private:

     int64_t total_trace_num;

     std::string _head_filename;
     std::string _trace_filename;
     std::string _meta_filename;
     std::string _filename;
     std::string seisnas_home_path_;
     GEFile *_gf_head, *_gf_trace;//, *_gf_meta;
     int64_t _head_length;
     HeadType _head_type;
     TraceType _trace_type;
     Lifetime _lifetime_days;
     MetaNAS *_meta;
};

}

}

#endif /* SEISFILE_NAS_WRITER_H_ */
