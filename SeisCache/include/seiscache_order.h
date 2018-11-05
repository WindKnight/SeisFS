/*
 * seiscache_order.h
 *
 *  Created on: Apr 13, 2018
 *      Author: wyd
 */

#ifndef SEISCACHE_ORDER_H_
#define SEISCACHE_ORDER_H_


namespace seisfs {

namespace cache {

class Order {
public:

	Order();
	~Order();

	enum SortOrder {
		ASC = 0,
		DESC = 1
	};

	Order& OrderBy(int key_id, bool gather_flag = false, SortOrder order = ASC);

private:
};

}

}

#endif /* SEISCACHE_ORDER_H_ */
