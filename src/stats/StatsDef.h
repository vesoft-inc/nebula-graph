/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STATS_STATSDEF_H_
#define STATS_STATSDEF_H_

#include "common/base/Base.h"
#include "common/stats/StatsManager.h"

DECLARE_int32(slow_query_threshold_us);


namespace nebula {

extern int32_t kNumQueries;
extern int32_t kNumSlowQueries;
extern int32_t kNumQueryErrors;
extern int32_t kQueryLatencyUs;
extern int32_t kSlowQueryLatencyUs;

void initCounters();

}  // namespace nebula
#endif  // STATS_STATSDEF_H_
