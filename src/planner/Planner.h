/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNER_H_
#define PLANNER_PLANNER_H_

#include <memory>
#include <set>

namespace nebula {
namespace graph {

class OptRule;
class PlanNode;

class Planner final {
public:
    explict Planner(std::set<std::shared_ptr<OptRule>> rules);

    std::shared_ptr<PlanNode> optimize(std::shared_ptr<PlanNode> node) const;

private:
    std::set<std::shared_ptr<OptRule>> rules_;
};

}   // namespace graph
}   // namespace nebula
#endif   // PLANNER_PLANNER_H_
