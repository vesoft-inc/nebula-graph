/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTIL_TIMETYPE_H
#define UTIL_TIMETYPE_H

#include "base/Base.h"

namespace nebula {
namespace time {
enum DateTimeType {
    T_Error     = -1,
    T_Null      = 0,
    T_Date      = 1,
    T_DateTime  = 2,
    T_Timestamp = 3,
};

struct Timezone {
    Timezone() : eastern(0), hour(0), minute(0) {}
    uint8_t             eastern;
    uint8_t             hour;
    uint8_t             minute;
};

struct Interval {
    int16_t  year;
    int8_t   month;
    int8_t   day;
    int8_t   hour;
    int8_t   minute;
    int8_t   second;
    uint64_t secondPart;
};

enum IntervalType {
    MICROSECOND,
    SECOND,
    MINUTE,
    HOUR,
    DAY,
    WEEK,
    MONTH,
    YEAR,
};
}  // namespace time
}  // namespace nebula
#endif  //  UTIL_TIMETYPE_H

