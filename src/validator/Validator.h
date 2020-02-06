/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_VALIDATOR_H_
#define VALIDATOR_VALIDATOR_H_

#include "base/Base.h"
#include "planner/PlanNode.h"
#include "parser/Sentence.h"

namespace nebula {
namespace graph {
class Validator {
public:
    explicit Validator(Sentence* sentence) : sentence_(sentence) {};

    virtual ~Validator() = default;

    static std::unique_ptr<Validator> makeValidator(Sentence* sentence);

    Status validate();

    auto start() const {
        return start_;
    }

    auto end() const {
        return end_;
    }

protected:
    virtual Status validateImpl() = 0;

    virtual Status toPlan() = 0;

protected:
    Sentence*                       sentence_;
    std::shared_ptr<StartNode>      start_;
    std::shared_ptr<PlanNode>       end_;
};
}  // namespace graph
}  // namespace nebula
#endif
