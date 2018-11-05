/*
 * seisfs_head_filter.h
 *
 *  Created on: Mar 26, 2018
 *      Author: wyd
 */

#ifndef SEISFS_HEAD_FILTER_H_
#define SEISFS_HEAD_FILTER_H_

#include <list>
#include <set>

namespace seisfs {

class HeadFilter {
public:
	HeadFilter();
	HeadFilter(const HeadFilter& head_filter);
	~HeadFilter();

    void AddKey(int key_id);

    int NumKeys() const; //return key number

    const std::list<int>& GetKeyList() const;

private:
    std::list<int> key_arr_;
    std::set<int> key_set_;
};



}

#endif /* SEISFS_HEAD_FILTER_H_ */
