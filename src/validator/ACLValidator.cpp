/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "common/base/Base.h"
#include "common/clients/meta/MetaClient.h"

#include "util/SchemaUtil.h"
#include "planner/Admin.h"
#include "validator/ACLValidator.h"

namespace nebula {
namespace graph {

static std::size_t kUsernameMaxLength = 16;
static std::size_t kPasswordMaxLength = 24;

// create user
GraphStatus CreateUserValidator::validateImpl() {
    const auto *sentence = static_cast<const CreateUserSentence*>(sentence_);
    if (sentence->getAccount()->size() > kUsernameMaxLength) {
        return GraphStatus::setOutOfLenOfUsername();
    }
    if (sentence->getPassword()->size() > kPasswordMaxLength) {
        return GraphStatus::setOutOfLenOfPassword();
    }
    return GraphStatus::OK();
}

GraphStatus CreateUserValidator::toPlan() {
    auto sentence = static_cast<CreateUserSentence*>(sentence_);
    return genSingleNodePlan<CreateUser>(sentence->getAccount(),
                                         sentence->getPassword(),
                                         sentence->ifNotExists());
}

// drop user
GraphStatus DropUserValidator::validateImpl() {
    const auto *sentence = static_cast<const DropUserSentence*>(sentence_);
    if (sentence->getAccount()->size() > kUsernameMaxLength) {
        return GraphStatus::setOutOfLenOfUsername();
    }
    return GraphStatus::OK();
}

GraphStatus DropUserValidator::toPlan() {
    auto sentence = static_cast<DropUserSentence*>(sentence_);
    return genSingleNodePlan<DropUser>(sentence->getAccount(),
                                       sentence->ifExists());
}

// update user
GraphStatus UpdateUserValidator::validateImpl() {
    const auto *sentence = static_cast<const AlterUserSentence*>(sentence_);
    if (sentence->getAccount()->size() > kUsernameMaxLength) {
        return GraphStatus::setOutOfLenOfUsername();
    }
    if (sentence->getPassword()->size() > kPasswordMaxLength) {
        return GraphStatus::setOutOfLenOfPassword();
    }
    return GraphStatus::OK();
}

GraphStatus UpdateUserValidator::toPlan() {
    auto sentence = static_cast<AlterUserSentence*>(sentence_);
    return genSingleNodePlan<UpdateUser>(sentence->getAccount(),
                                         sentence->getPassword());
}

// show users
GraphStatus ShowUsersValidator::validateImpl() {
    return GraphStatus::OK();
}

GraphStatus ShowUsersValidator::toPlan() {
    return genSingleNodePlan<ListUsers>();
}

// change password
GraphStatus ChangePasswordValidator::validateImpl() {
    const auto *sentence = static_cast<const ChangePasswordSentence*>(sentence_);
    if (sentence->getAccount()->size() > kUsernameMaxLength) {
        return GraphStatus::setOutOfLenOfUsername();
    }
    if (sentence->getOldPwd()->size() > kPasswordMaxLength) {
        return GraphStatus::setOutOfLenOfPassword();
    }
    if (sentence->getNewPwd()->size() > kPasswordMaxLength) {
        return GraphStatus::setOutOfLenOfPassword();
    }
    return GraphStatus::OK();
}

GraphStatus ChangePasswordValidator::toPlan() {
    auto sentence = static_cast<ChangePasswordSentence*>(sentence_);
    return genSingleNodePlan<ChangePassword>(sentence->getAccount(),
                                             sentence->getOldPwd(),
                                             sentence->getNewPwd());
}

// grant role
GraphStatus GrantRoleValidator::validateImpl() {
    const auto *sentence = static_cast<const GrantSentence*>(sentence_);
    if (sentence->getAccount()->size() > kUsernameMaxLength) {
        return GraphStatus::setOutOfLenOfUsername();
    }
    return GraphStatus::OK();
}

GraphStatus GrantRoleValidator::toPlan() {
    auto sentence = static_cast<GrantSentence*>(sentence_);
    return genSingleNodePlan<GrantRole>(sentence->getAccount(),
                                        sentence->getAclItemClause()->getSpaceName(),
                                        sentence->getAclItemClause()->getRoleType());
}

// revoke role
GraphStatus RevokeRoleValidator::validateImpl() {
    const auto *sentence = static_cast<const RevokeSentence*>(sentence_);
    if (sentence->getAccount()->size() > kUsernameMaxLength) {
        return GraphStatus::setOutOfLenOfUsername();
    }
    return GraphStatus::OK();
}

GraphStatus RevokeRoleValidator::toPlan() {
    auto sentence = static_cast<RevokeSentence*>(sentence_);
    return genSingleNodePlan<RevokeRole>(sentence->getAccount(),
                                         sentence->getAclItemClause()->getSpaceName(),
                                         sentence->getAclItemClause()->getRoleType());
}

// show roles in space
GraphStatus ShowRolesInSpaceValidator::validateImpl() {
    return GraphStatus::OK();
}

GraphStatus ShowRolesInSpaceValidator::toPlan() {
    auto sentence = static_cast<ShowRolesSentence*>(sentence_);
    auto spaceIdResult = qctx_->schemaMng()->toGraphSpaceID(*sentence->name());
    if (!spaceIdResult.ok()) {
        return GraphStatus::setSpaceNotFound(*sentence->name());
    }
    auto spaceId = spaceIdResult.value();
    return genSingleNodePlan<ListRoles>(spaceId);
}

}  // namespace graph
}  // namespace nebula
