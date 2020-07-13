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

// create user
Status CreateUserValidator::validateImpl() {
    return Status::OK();
}

Status CreateUserValidator::toPlan() {
    auto sentence = static_cast<CreateUserSentence*>(sentence_);
    return genSingleNodePlan<CreateUser>(std::move(*sentence->moveAccount()),
                                         std::move(*sentence->movePassword()),
                                         sentence->ifNotExists());
}

// drop user
Status DropUserValidator::validateImpl() {
    return Status::OK();
}

Status DropUserValidator::toPlan() {
    auto sentence = static_cast<DropUserSentence*>(sentence_);
    return genSingleNodePlan<DropUser>(std::move(*sentence->moveAccount()),
                                       sentence->ifExists());
}

// update user
Status UpdateUserValidator::validateImpl() {
    return Status::OK();
}

Status UpdateUserValidator::toPlan() {
    auto sentence = static_cast<AlterUserSentence*>(sentence_);
    return genSingleNodePlan<UpdateUser>(std::move(*sentence->moveAccount()),
                                         std::move(*sentence->movePassword()));
}

// show users
Status ShowUsersValidator::validateImpl() {
    return Status::OK();
}

Status ShowUsersValidator::toPlan() {
    return genSingleNodePlan<ListUsers>();
}

// change password
Status ChangePasswordValidator::validateImpl() {
    return Status::OK();
}

Status ChangePasswordValidator::toPlan() {
    auto sentence = static_cast<ChangePasswordSentence*>(sentence_);
    return genSingleNodePlan<ChangePassword>(std::move(*sentence->moveAccount()),
                                             std::move(*sentence->moveOldPwd()),
                                             std::move(*sentence->moveNewPwd()));
}

// grant role
Status GrantRoleValidator::validateImpl() {
    return Status::OK();
}

Status GrantRoleValidator::toPlan() {
    auto sentence = static_cast<GrantSentence*>(sentence_);
    return genSingleNodePlan<GrantRole>(std::move(*sentence->moveAccount()),
                                        std::move(
                                            *sentence->mutableAclItemClause()->moveSpaceName()),
                                        sentence->getAclItemClause()->getRoleType());
}

// revoke role
Status RevokeRoleValidator::validateImpl() {
    return Status::OK();
}

Status RevokeRoleValidator::toPlan() {
    auto sentence = static_cast<RevokeSentence*>(sentence_);
    return genSingleNodePlan<RevokeRole>(std::move(*sentence->moveAccount()),
                                         std::move(
                                             *sentence->mutableAclItemClause()->moveSpaceName()),
                                         sentence->getAclItemClause()->getRoleType());
}

// show roles in space
Status ShowRolesInSpaceValidator::validateImpl() {
    return Status::OK();
}

Status ShowRolesInSpaceValidator::toPlan() {
    auto sentence = static_cast<ShowRolesSentence*>(sentence_);
    auto spaceIdResult = qctx_->schemaMng()->toGraphSpaceID(*sentence->name());
    NG_RETURN_IF_ERROR(spaceIdResult);
    auto spaceId = spaceIdResult.value();
    return genSingleNodePlan<ListRoles>(spaceId);
}

}  // namespace graph
}  // namespace nebula
