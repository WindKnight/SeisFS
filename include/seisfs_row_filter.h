/*
 * seisfs_row_filter.h
 *
 *  Created on: Mar 26, 2018
 *      Author: wyd
 */

#ifndef SEISFS_ROW_FILTER_H_
#define SEISFS_ROW_FILTER_H_


#include <vector>
#include <stdint.h>

namespace seisfs {

class RowScope {
public:
	RowScope() {
		start_trace_ = 0;
		stop_trace_ = -1; //TODO use INTMAX?
	}
	RowScope(int64_t start_trace) {
		start_trace_ = start_trace;
		stop_trace_ = -1;
	}
	RowScope(int64_t start_trace, int64_t stop_trace) {
		start_trace_ = start_trace;
		stop_trace_ = stop_trace;
	}
	~RowScope() {

	}


    /*
     * start trace could be bigger than stop trace, which means the data will be read in descending order
     */
    inline void SetStartTrace(int64_t start_trace) {
    	start_trace_ = start_trace;
    }
    inline void SetStopTrace(int64_t stop_trace) {
    	stop_trace_ = stop_trace;
    }

    inline int64_t GetStartTrace() const {
    	return start_trace_;
    }
    inline int64_t GetStopTrace() const {
    	return stop_trace_;
    }
private:

    int64_t start_trace_;
    int64_t stop_trace_;
};



class RowFilter {
public:

	RowFilter();
	RowFilter(const RowFilter &row_filter);
	RowFilter(const RowScope &row_scope);
	~RowFilter();

	RowFilter& AddFilter(const RowScope &scope);
	const std::vector<RowScope>& GetAllScope() const;

protected:
	std::vector<RowScope> scope_arr_;

};

}
#endif /* SEISFS_ROW_FILTER_H_ */
