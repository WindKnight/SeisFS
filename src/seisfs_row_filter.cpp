/*
 * seisfs_row_filter.cpp
 *
 *  Created on: Apr 5, 2018
 *      Author: wyd
 */

#include "seisfs_row_filter.h"


namespace seisfs {



RowFilter::RowFilter() {

}
RowFilter::RowFilter(const RowFilter &row_filter) {
	scope_arr_ = row_filter.scope_arr_;
}
RowFilter::RowFilter(const RowScope &row_scope) {
	scope_arr_.clear();
	scope_arr_.push_back(row_scope);
}
RowFilter::~RowFilter() {

}

RowFilter& RowFilter::AddFilter(const RowScope &scope) {
	scope_arr_.push_back(scope);
	return *this;
}
const std::vector<RowScope>& RowFilter::GetAllScope() const {
	return scope_arr_;
}



}


