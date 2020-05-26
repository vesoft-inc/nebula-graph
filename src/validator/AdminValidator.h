/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_ADMINVALIDATOR_H_
#define VALIDATOR_ADMINVALIDATOR_H_

#include "common/base/Base.h"
#include "validator/Validator.h"
#include "parser/MaintainSentences.h"
#include "parser/AdminSentences.h"
#include "common/clients/meta/MetaClient.h"

namespace nebula {
namespace graph {
class CreateSpaceValidator final : public Validator {
public:
    CreateSpaceValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    meta::SpaceDesc                    spaceDesc_;
    bool                               ifNotExist_;
};

class DescSpaceValidator final : public Validator {
public:
    DescSpaceValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;
};

class ShowSpacesValidator final : public Validator {
public:
    ShowSpacesValidator(Sentence* sentence, ValidateContext* context)
            : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;
};

class DropSpaceValidator final : public Validator {
public:
    DropSpaceValidator(Sentence* sentence, ValidateContext* context)
            : Validator(sentence, context) {
        sentence_ = static_cast<DropSpaceSentence*>(sentence);
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    DropSpaceSentence                       *sentence_{nullptr};
};

class CreateSnapshotValidator final : public Validator {
public:
    CreateSnapshotValidator(Sentence* sentence, ValidateContext* context)
            : Validator(sentence, context) {
        UNUSED(sentence);
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;
};

class DropSnapshotValidator final : public Validator {
public:
    DropSnapshotValidator(Sentence* sentence, ValidateContext* context)
            : Validator(sentence, context) {
        sentence_ = static_cast<DropSnapshotSentence*>(sentence);
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    DropSnapshotSentence                   *sentence_{nullptr};
};

class ShowSnapshotsValidator final : public Validator {
public:
    ShowSnapshotsValidator(Sentence* sentence, ValidateContext* context)
            : Validator(sentence, context) {
        UNUSED(sentence);
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;
};
}  // namespace graph
}  // namespace nebula
#endif  // VALIDATOR_ADMINVALIDATOR_H_
