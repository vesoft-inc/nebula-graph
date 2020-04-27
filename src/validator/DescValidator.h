/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_DESCVALIDATOR_H_
#define VALIDATOR_DESCVALIDATOR_H_

#include "base/Base.h"
#include "validator/Validator.h"
#include "parser/MaintainSentences.h"
#include "parser/AdminSentences.h"

namespace nebula {
namespace graph {
class DescSpaceValidator final : public Validator {
public:
    DescSpaceValidator(Sentence* sentence, ValidateContext* context)
            : Validator(sentence, context) {
        sentence_ = static_cast<DescribeSpaceSentence*>(sentence);
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    DescribeSpaceSentence                   *sentence_{nullptr};
    std::string                              spaceName_;
};

class DescTagValidator final : public Validator {
public:
    DescTagValidator(Sentence* sentence, ValidateContext* context)
            : Validator(sentence, context) {
        sentence_ = static_cast<DescribeTagSentence*>(sentence);
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    DescribeTagSentence                   *sentence_{nullptr};
    std::string                            tagName_;
};

class DescEdgeValidator final : public Validator {
public:
    DescEdgeValidator(Sentence* sentence, ValidateContext* context)
            : Validator(sentence, context) {
        sentence_ = static_cast<DescribeEdgeSentence*>(sentence);
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    DescribeEdgeSentence               *sentence_{nullptr};
    std::string                         edgeName_;
};
}  // namespace graph
}  // namespace nebula
#endif  // VALIDATOR_DESCVALIDATOR_H_
