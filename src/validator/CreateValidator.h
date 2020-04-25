/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_CREATEVALIDATOR_H_
#define VALIDATOR_CREATEVALIDATOR_H_

#include "base/Base.h"
#include "validator/Validator.h"
#include "parser/MaintainSentences.h"
#include "parser/AdminSentences.h"

namespace nebula {
namespace graph {
class CreateSpaceValidator final : public Validator {
public:
    CreateSpaceValidator(Sentence* sentence, ValidateContext* context)
            : Validator(sentence, context) {
        sentence_ = static_cast<CreateSpaceSentence*>(sentence);
    }

public:
    meta::SpaceDesc getSpaceDesc() {
        return std::move(spaceDesc_);
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    CreateSpaceSentence               *sentence_{nullptr};
    meta::SpaceDesc                   spaceDesc_;
};

class CreateTagValidator final : public Validator {
public:
    CreateTagValidator(Sentence* sentence, ValidateContext* context)
    : Validator(sentence, context) {
        sentence_ = static_cast<CreateTagSentence*>(sentence);
    }

private:
    Status validateImpl() override;
    Status toPlan() override;

private:
    CreateTagSentence               *sentence_{nullptr};
    meta::cpp2::Schema               schema_;
};

class CreateEdgeValidator final : public Validator {
public:
    CreateEdgeValidator(Sentence* sentence, ValidateContext* context)
    : Validator(sentence, context) {
        sentence_ = static_cast<CreateEdgeSentence*>(sentence);
    }

private:
    Status validateImpl() override;
    Status toPlan() override;

private:
    CreateEdgeSentence               *sentence_{nullptr};
    meta::cpp2::Schema                schema_;
};
}  // namespace graph
}  // namespace nebula
#endif
