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
#include "clients/meta/MetaClient.h"

namespace nebula {
namespace graph {
class CreateSpaceValidator final : public Validator {
public:
    CreateSpaceValidator(Sentence* sentence, ValidateContext* context)
            : Validator(sentence, context) {
        sentence_ = static_cast<CreateSpaceSentence*>(sentence);
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    CreateSpaceSentence               *sentence_{nullptr};
    meta::SpaceDesc                    spaceDesc_;
    bool                               ifNotExist_;
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
    std::string                      tagName_;
    meta::cpp2::Schema               schema_;
    bool                             ifNotExist_;
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
    std::string                       edgeName_;
    meta::cpp2::Schema                schema_;
    bool                              ifNotExist_;
};
}  // namespace graph
}  // namespace nebula
#endif
