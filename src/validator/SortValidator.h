/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_SORTVALIDATOR_H_
#define VALIDATOR_SORTVALIDATOR_H_

#include "common/base/Base.h"
#include "validator/Validator.h"

namespace nebula {
namespace graph {
class SortValidator final : public Validator {
public:
    SortValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {}

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    std::vector<OrderFactor*>     factors_;
};
}  // namespace graph
}  // namespace nebula
#endif   // VALIDATOR_SORTVALIDATOR_H_
