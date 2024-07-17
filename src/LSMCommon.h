//
// Created by Madhav Shekhar Sharma on 7/17/24.
//

#ifndef ASLAM_SRC_LSMCOMMON_H_
#define ASLAM_SRC_LSMCOMMON_H_

#include <utility>

enum class GetResult { NotFound, Found, Tombstone };

template<typename V>
using GetResultPair = std::pair<GetResult, V>;

#endif//ASLAM_SRC_LSMCOMMON_H_
