/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_SEQUENTIALVALIDATOR_H_
#define VALIDATOR_SEQUENTIALVALIDATOR_H_

#include "base/Base.h"
#include "validator/Validator.h"
#include "parser/SequentialSentences.h"

/**
 * A SequentialValidator is the entrance of validators.
 */
namespace nebula {
namespace graph {
class SequentialValidator final : public Validator {
public:
    explicit SequentialValidator(Sentence* sentence) : Validator(sentence) {};

private:
    Status validateImpl() override;

    /**
     * Each sentence would be converted to a sub-plan, and they would
     * be cascaded together into a complete execution plan.
     */
    Status toPlan() override;

private:
    std::vector<std::unique_ptr<Validator>>     validators_;
};
}  // namespace graph
}  // namespace nebula
#endif
