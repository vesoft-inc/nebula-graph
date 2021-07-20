/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "scheduler/Scheduler.h"

namespace nebula {
namespace graph {

Scheduler::Scheduler(QueryContext* qctx) : qctx_(DCHECK_NOTNULL(qctx)) {}

}   // namespace graph
}   // namespace nebula
