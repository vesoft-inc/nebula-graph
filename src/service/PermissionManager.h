/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef SERVICE_PERMISSIONMANAGER_H_
#define SERVICE_PERMISSIONMANAGER_H_

#include "common/base/Base.h"
#include "common/clients/meta/MetaClient.h"
#include "parser/AdminSentences.h"
#include "service/GraphFlags.h"
#include "service/Session.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

class PermissionManager final {
public:
    PermissionManager() = delete;
    static Status canReadSpace(const Session *session, GraphSpaceID spaceId);
    static Status canReadSchemaOrData(const Session *session);
    static Status canWriteSpace(const Session *session);
    static Status canWriteSchema(const Session *session);
    static Status canWriteUser(const Session *session);
    static Status canWriteRole(const Session *session,
                             meta::cpp2::RoleType targetRole,
                             GraphSpaceID spaceId,
                             const std::string& targetUser);
    static Status canWriteData(const Session *session);
};
}  // namespace graph
}  // namespace nebula

#endif   // COMMON_PERMISSION_PERMISSIONMANAGER_H_
