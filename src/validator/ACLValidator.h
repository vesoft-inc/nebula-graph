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
    CreateUserValidator(Sentence* sentence, ValidateContext* context)
            : Validator(sentence, context) {
        sentence_ = static_cast<CreateUserSentence*>(sentence);
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    CreateUserSentence               *sentence_{nullptr};
};

class DropUserValidator final : public Validator {
public:
    DropUserValidator(Sentence* sentence, ValidateContext* context)
            : Validator(sentence, context) {
        sentence_ = static_cast<DropUserSentence*>(sentence);
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    DropUserSentence               *sentence_{nullptr};
};

class UpdateUserValidator final : public Validator {
public:
    UpdateUserValidator(Sentence* sentence, ValidateContext* context)
            : Validator(sentence, context) {
        sentence_ = static_cast<AlterUserSentence*>(sentence);
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    AlterUserSentence               *sentence_{nullptr};
};

class ChangePasswordValidator final : public Validator {
public:
    ChangePasswordValidator(Sentence* sentence, ValidateContext* context)
            : Validator(sentence, context) {
        sentence_ = static_cast<ChangePasswordSentence*>(sentence);
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    ChangePasswordSentence               *sentence_{nullptr};
};

class GrantRoleValidator final : public Validator {
public:
    GrantRoleValidator(Sentence* sentence, ValidateContext* context)
            : Validator(sentence, context) {
        sentence_ = static_cast<GrantSentence*>(sentence);
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    GrantSentence               *sentence_{nullptr};
};

class RevokeRoleValidator final : public Validator {
public:
    RevokeRoleValidator(Sentence* sentence, ValidateContext* context)
            : Validator(sentence, context) {
        sentence_ = static_cast<RevokeSentence*>(sentence);
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    RevokeSentence               *sentence_{nullptr};
};

}  // namespace graph
}  // namespace nebula
