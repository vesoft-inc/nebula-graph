/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "service/PermissionManager.h"

namespace nebula {
namespace graph {

// static
Status PermissionManager::canReadSpace(const Session *session, GraphSpaceID spaceId) {
    if (!FLAGS_enable_authorize) {
        return Status::OK();
    }
    if (session->isGod()) {
        return Status::OK();
    }
    auto havePermission = Status::PermissionError();
    auto roleResult = session->roleWithSpace(spaceId);
    if (!roleResult.ok()) {
        return havePermission;
    }
    auto role = roleResult.value();
    switch (role) {
        case meta::cpp2::RoleType::GOD :
        case meta::cpp2::RoleType::ADMIN :
        case meta::cpp2::RoleType::DBA :
        case meta::cpp2::RoleType::USER :
        case meta::cpp2::RoleType::GUEST : {
            havePermission = Status::OK();
            break;
        }
    }
    return havePermission;
}

// static
Status PermissionManager::canReadSchemaOrData(const Session *session) {
    if (session->space() == -1) {
        LOG(ERROR) << "The space name is not set";
        return Status::PermissionError();
    }
    if (session->isGod()) {
        return Status::OK();
    }
    auto havePermission = Status::PermissionError();
    auto roleResult = session->roleWithSpace(session->space());
    if (!roleResult.ok()) {
        return havePermission;
    }
    auto role = roleResult.value();
    switch (role) {
        case meta::cpp2::RoleType::GOD :
        case meta::cpp2::RoleType::ADMIN :
        case meta::cpp2::RoleType::DBA :
        case meta::cpp2::RoleType::USER :
        case meta::cpp2::RoleType::GUEST : {
            havePermission = Status::OK();
            break;
        }
    }
    return havePermission;
}

// static
Status PermissionManager::canWriteSpace(const Session *session) {
    if (session->isGod()) {
        return Status::OK();
    } else {
        return Status::PermissionError();
    }
}

// static
Status PermissionManager::canWriteSchema(const Session *session) {
    if (session->space() == -1) {
        LOG(ERROR) << "The space name is not set";
        return Status::PermissionError();
    }
    if (session->isGod()) {
        return Status::OK();
    }
    auto havePermission = Status::PermissionError();
    auto roleResult = session->roleWithSpace(session->space());
    if (!roleResult.ok()) {
        return havePermission;
    }
    auto role = roleResult.value();
    switch (role) {
        case meta::cpp2::RoleType::GOD :
        case meta::cpp2::RoleType::ADMIN :
        case meta::cpp2::RoleType::DBA : {
            havePermission = Status::OK();
            break;
        }
        case meta::cpp2::RoleType::USER :
        case meta::cpp2::RoleType::GUEST :
            break;
    }
    return havePermission;
}

// static
Status PermissionManager::canWriteUser(const Session *session) {
    if (session->isGod()) {
        return Status::OK();
    } else {
        return Status::PermissionError();
    }
}

Status PermissionManager::canWriteRole(const Session *session,
                                     meta::cpp2::RoleType targetRole,
                                     GraphSpaceID spaceId,
                                     const std::string& targetUser) {
    if (!FLAGS_enable_authorize) {
        return Status::OK();
    }
    /**
     * Reject grant or revoke to himself.
     */
     if (session->user() == targetUser) {
         return Status::PermissionError();
     }
    /*
     * Reject any user grant or revoke role to GOD
     */
    if (targetRole == meta::cpp2::RoleType::GOD) {
        return Status::PermissionError();
    }
    /*
     * God user can be grant or revoke any one.
     */
    if (session->isGod()) {
        return Status::OK();
    }
    /**
     * Only allow ADMIN user grant or revoke other user to DBA, USER, GUEST.
     */
    auto roleResult = session->roleWithSpace(spaceId);
    if (!roleResult.ok()) {
        return Status::PermissionError();
    }
    auto role = roleResult.value();
    if (role == meta::cpp2::RoleType::ADMIN && targetRole != meta::cpp2::RoleType::ADMIN) {
        return Status::OK();
    }
    return Status::PermissionError();
}

// static
Status PermissionManager::canWriteData(const Session *session) {
    if (session->space() == -1) {
        LOG(ERROR) << "The space name is not set";
        return Status::PermissionError();
    }
    if (session->isGod()) {
        return Status::OK();
    }
    auto havePermission = Status::PermissionError();
    auto roleResult = session->roleWithSpace(session->space());
    if (!roleResult.ok()) {
        return havePermission;
    }
    auto role = roleResult.value();
    switch (role) {
        case meta::cpp2::RoleType::GOD :
        case meta::cpp2::RoleType::ADMIN :
        case meta::cpp2::RoleType::DBA :
        case meta::cpp2::RoleType::USER : {
            havePermission = Status::OK();
            break;
        }
        case meta::cpp2::RoleType::GUEST :
            break;
    }
    return havePermission;
}
}   // namespace graph
}   // namespace nebula
