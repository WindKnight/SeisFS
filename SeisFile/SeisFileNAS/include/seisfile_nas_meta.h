/*
 * seisfile_meta.h
 *
 *  Created on: Mar 26, 2018
 *      Author: wyd
 */

#ifndef SEISFILE_NAS_META_H_
#define SEISFILE_NAS_META_H_


#include "seisfs_meta.h"
#include <vector>
#include <string>
#include <stddef.h>
#include <time.h>
#include "seisfile_meta.h"
#include "GEFile.h"
#include "seisfile_nas_metastruct.h"

namespace seisfs {

	namespace file {

	#define LIFETIME_NULL 		-1
	#define LIFETIME_DEFAULT 	30
	struct Metastruct;

	class MetaNAS {
	public:

		/*MetaNAS(const std::string &data_name, const HeadType &head_type, const TraceType& trace_type, Lifetime lifetime_days);

		MetaNAS(const std::string &data_name, const HeadType &head_type, Lifetime lifetime_days);

		MetaNAS(const std::string &data_name, const TraceType& trace_type, Lifetime lifetime_days);*/

		MetaNAS(const std::string &data_name);

		~MetaNAS(){};

		void Init();

		void CreateMetaFile();

		bool MetafileExits();

		bool MetaRead();

		bool MetaUpdate();

		void MetaheadnumPlus();

		void MetatracenumPlus();

		HeadType MetaGetheadtype();

		TraceType MetaGettracetype();

		void MetaSetheadtype(HeadType headtype);

		void MetaSettracetype(TraceType tracetype);

		void MetaSetheadnum(const uint32_t& headnum);

		void MetaSettracenum(const uint32_t& tracenum);

		uint32_t MetaGetheadnum();

		uint32_t MetaGettracenum();

		uint32_t MetaGetheadlength();

		uint32_t MetaGettracelength();

		uint32_t MetaGetkeynum();

		time_t MetaGetLastModify();

		bool MetaSetLifetime(Lifetime lifetime_days);

		Lifetime MetaGetLifetime();

		LifetimeStat MetaGetLifetimestat();

		LifetimeStat MetaSetLifetimestat(LifetimeStat lt_stat);

	private:
		std::string _filename;
		Metastruct *_meta;
		GEFile *_gf_meta;
		HeadType _head_type;
		TraceType _trace_type;

	};


	}

}

#endif /* SEISFILE_NAS_META_H_ */
