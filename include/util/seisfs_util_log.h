/***************************************************************************
* Copyright (c) 2013, R&D Center of BGP
* All rights reserved.
*
* author         : wyd
* Created on     : 2018.04.23
 ***************************************************************************/


#ifndef SEISFS_UTIL_LOG_H_
#define SEISFS_UTIL_LOG_H_

#include <string>
#include <stdio.h>
#include <unistd.h>

#define PRINTL printf("file : %s, line : %d\n", __FILE__, __LINE__);fflush(stdout);


extern std::string g_logAppTag;

extern "C" {

        int Warn(const char *fmt, ...);
        int Err(const char *fmt, ...);
        int Info(const char *fmt, ...);
        int Debug(const char *fmt, ...);

        std::string GetTimeString();

        void SetLogAppTag(const std::string& appTag);

}


#endif /* SEISFS_UTIL_LOG_H_ */
