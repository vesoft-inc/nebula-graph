/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef SCHEDULER_SCHEDULER_H_
#define SCHEDULER_SCHEDULER_H_

#include <memory>

namespace nebula {
namespace graph {

class PlanNode;

class Scheduler {
public:
    // TODO(yee): Use same folly executors as graph daemon process
    Scheduler() = default;

    void schedule(std::shared_ptr<PlanNode> planRoot);

private:
};

}   // namespace graph
}   // namespace nebula

#endif   // SCHEDULER_SCHEDULER_H_
