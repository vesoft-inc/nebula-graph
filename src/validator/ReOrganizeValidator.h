/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/base/Base.h"
#include "validator/Validator.h"
#include "parser/TraverseSentences.h"

namespace nebula {
namespace graph {

class OrderByValidator : public Validator {
public:
    OrderByValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {}

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    std::unique_ptr<OrderFactors>   orderFactors_;
};

}  // namespace graph
}  // namespace nebula
