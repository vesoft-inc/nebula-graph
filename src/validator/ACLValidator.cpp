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
    SINGLE_NODE_PLAN_TEMPLATE(CreateUser,
                             *sentence_->getAccount(),
                             *sentence_->getPassword(),
                              sentence_->ifNotExists());
}

// drop user
Status DropUserValidator::validateImpl() {
    return Status::OK();
}

Status DropUserValidator::toPlan() {
    SINGLE_NODE_PLAN_TEMPLATE(DropUser,
                             *sentence_->getAccount(),
                              sentence_->ifExists());
}

// update user
Status UpdateUserValidator::validateImpl() {
    return Status::OK();
}

Status UpdateUserValidator::toPlan() {
    SINGLE_NODE_PLAN_TEMPLATE(UpdateUser,
                             *sentence_->getAccount(),
                             *sentence_->getPassword());
}

// change password
Status ChangePasswordValidator::validateImpl() {
    return Status::OK();
}

Status ChangePasswordValidator::toPlan() {
    SINGLE_NODE_PLAN_TEMPLATE(ChangePassword,
                             *sentence_->getAccount(),
                             *sentence_->getOldPwd(),
                             *sentence_->getNewPwd());
}

// grant role
Status GrantRoleValidator::validateImpl() {
    return Status::OK();
}

Status GrantRoleValidator::toPlan() {
    SINGLE_NODE_PLAN_TEMPLATE(GrantRole,
                             *sentence_->getAccount(),
                             *sentence_->getAclItemClause()->getSpaceName(),
                              sentence_->getAclItemClause()->getRoleType());
}

// revoke role
Status RevokeRoleValidator::validateImpl() {
    return Status::OK();
}

Status RevokeRoleValidator::toPlan() {
    SINGLE_NODE_PLAN_TEMPLATE(RevokeRole,
                             *sentence_->getAccount(),
                             *sentence_->getAclItemClause()->getSpaceName(),
                              sentence_->getAclItemClause()->getRoleType());
}

}  // namespace graph
}  // namespace nebula
