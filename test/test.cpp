/*
 * test.cpp
 *
 *  Created on: Apr 6, 2018
 *      Author: wyd
 */

#include "seisfs_head_filter.h"
#include "seisfs_row_filter.h"
#include <stdio.h>


int main() {

	seisfs::HeadFilter head_filter;
	head_filter.AddKey(23);
	head_filter.AddKey(13);

	printf("head_filter_num = %d\n", head_filter.NumKeys());
	const std::list<int> &key_list = head_filter.GetKeyList();
	for(std::list<int>::const_iterator it = key_list.begin(); it != key_list.end(); ++it) {
		printf("key = %d\n", *it);
	}
	key_list.size();


	return 0;
}



