/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/Planner.h"

#include "validator/Validator.h"

namespace nebula {
namespace graph {
std::unordered_map<Sentence::Kind, std::vector<Planner*>> Planner::plannersMap_;

StatusOr<SubPlan> Planner::toPlan(Validator* validator) {
    const auto* sentence = validator->sentence();
    auto planners = plannersMap_.find(sentence->kind());
    if (planners == plannersMap_.end()) {
        return Status::Error("No planners for sentence: %s", sentence->toString().c_str());
    }
    for (auto* planner : planners->second) {
        if (planner->match(validator)) {
            return planner->transform(validator);
        }
    }
    return Status::Error("No planner matches sentence: %s", sentence->toString().c_str());
}
}  // namespace graph
}  // namespace nebula
