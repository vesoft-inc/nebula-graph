/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "validator/Validator.h"
#include "parser/UserSentences.h"

namespace nebula {
namespace graph {

class CreateUserValidator final : public Validator {
public:
    CreateUserValidator(Sentence* sentence, QueryContext* context)
            : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;
};

class DropUserValidator final : public Validator {
public:
    DropUserValidator(Sentence* sentence, QueryContext* context)
            : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;
};

class UpdateUserValidator final : public Validator {
public:
    UpdateUserValidator(Sentence* sentence, QueryContext* context)
            : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;
};

class ChangePasswordValidator final : public Validator {
public:
    ChangePasswordValidator(Sentence* sentence, QueryContext* context)
            : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;
};

class GrantRoleValidator final : public Validator {
public:
    GrantRoleValidator(Sentence* sentence, QueryContext* context)
            : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;
};

class RevokeRoleValidator final : public Validator {
public:
    RevokeRoleValidator(Sentence* sentence, QueryContext* context)
            : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;
};

}  // namespace graph
}  // namespace nebula
