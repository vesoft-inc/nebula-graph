/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/base/Base.h"
#include "validator/Validator.h"
#include "parser/AdminSentences.h"

namespace nebula {
namespace graph {

class AdminJobValidator final : public Validator {
public:
    AdminJobValidator(Sentence* sentence, ValidateContext* context)
            : Validator(sentence, context) {
        sentence_ = static_cast<AdminJobSentence*>(sentence);
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    AdminJobSentence               *sentence_{nullptr};
};

}  // namespace graph
}  // namespace nebula
