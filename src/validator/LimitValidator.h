/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_LIMITVALIDATOR_H_
#define VALIDATOR_LIMITVALIDATOR_H_

#include "validator/Validator.h"

namespace nebula {

class Sentence;

namespace graph {

class QueryContext;

class LimitValidator final : public Validator {
public:
    LimitValidator(Sentence *sentence, QueryContext *qctx);

    Status validateImpl() override;
    Status toPlan() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // VALIDATOR_LIMITVALIDATOR_H_
