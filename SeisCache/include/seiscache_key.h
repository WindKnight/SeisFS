/*
 * seiscache_key.h
 *
 *  Created on: Apr 12, 2018
 *      Author: wyd
 */

#ifndef SEISCACHE_KEY_H_
#define SEISCACHE_KEY_H_


namespace seisfs {

namespace cache {

class Key {
public:
	enum KeyOperator {
		NONE			= -1,
		EQUAL 			= 0,
		RANGE			= 1,
		RANGE_GROUP 	= 2,
		RANGE_TOLERANCE = 3,
	};

	Key();

	~Key();

	Key(const Key& key);

	template <class KTYPE>
	Key(int key_id, KTYPE value) {

	}

	template <class KINT>
	Key(int key_id, KINT start_value, KINT end_value, KINT inc, int group = 1) {

	}

	template <class KFLOAT>
	Key(int key_id, KFLOAT start_value, KFLOAT end_value, KFLOAT inc, float tolerance = 0.0f) {

	}

	Key& operator=(const Key& key);

	KeyOperator GetOperator() const;

	int GetKeyID() const;

	template <typename KTYPE>
	void SetKey(int key_id, KTYPE value) {

	}

	template <class KTYPE>
	void GetKeyValue(KTYPE& value) const {

	}

	/*
	 * int key filter
	 */


	template <class KINT>
	void SetKey(int key_id, KINT start_value, KINT end_value, KINT inc, int group = 1) {

	}

	template <class KINT>
	void GetKeyValue(KINT& start_value, KINT& end_value, KINT& inc, int& group) const {

	}

	/*
	 * float key filter
	 */

	template <class KFLOAT>
	void SetKey(int key_id, KFLOAT start_value, KFLOAT end_value, KFLOAT inc, float tolerance = 0.0f) {

	}

	template <class KFLOAT>
	void GetKeyValue(KFLOAT& start_value, KFLOAT& end_value, KFLOAT& inc, float& tolerance) const {

	}

private:
	KeyOperator key_operator_;

};

}

}


#endif /* SEISCACHE_KEY_H_ */
