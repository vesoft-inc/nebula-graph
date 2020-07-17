/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_ADMIN_H_
#define PLANNER_ADMIN_H_

#include "planner/Query.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "common/clients/meta/MetaClient.h"

/**
 * All admin-related nodes would be put in this file.
 * These nodes would not exist in a same plan with maintain-related/
 * mutate-related/query-related nodes. And they are also isolated
 * from each other. This would be guaranteed by parser and validator.
 */
namespace nebula {
namespace graph {

// Some template node such as Create template for the node create something(user,tag...)
// Fit the conflict create process
class CreateNode : public SingleDependencyNode {
protected:
    CreateNode(ExecutionPlan* plan, Kind kind, PlanNode* input, bool ifNotExist = false)
        : SingleDependencyNode(plan, kind, input), ifNotExist_(ifNotExist) {}

public:
    bool ifNotExist() const {
        return ifNotExist_;
    }

private:
    bool ifNotExist_{false};
};

class DropNode : public SingleDependencyNode {
protected:
    DropNode(ExecutionPlan* plan, Kind kind, PlanNode* input, bool ifExist = false)
        : SingleDependencyNode(plan, kind, input), ifExist_(ifExist) {}

public:
    bool ifExist() const {
        return ifExist_;
    }

private:
    bool ifExist_{false};
};

class CreateSpace final : public SingleInputNode {
public:
    static CreateSpace* make(ExecutionPlan* plan,
                             PlanNode* input,
                             meta::SpaceDesc props,
                             bool ifNotExists) {
    return new CreateSpace(plan,
                           input,
                           std::move(props),
                           ifNotExists);
    }

    std::string explain() const override {
        return "CreateSpace";
    }

public:
    const meta::SpaceDesc& getSpaceDesc() const {
        return props_;
    }

    bool getIfNotExists() const {
        return ifNotExist_;
    }

private:
    CreateSpace(ExecutionPlan* plan,
                PlanNode* input,
                meta::SpaceDesc props,
                bool ifNotExists)
        : SingleInputNode(plan, Kind::kCreateSpace, input),
          props_(std::move(props)), ifNotExist_(ifNotExists) {}

private:
    meta::SpaceDesc     props_;
    bool                ifNotExist_{false};
};

class DropSpace final : public SingleInputNode {
public:
    static DropSpace* make(ExecutionPlan* plan,
                           PlanNode* input,
                           std::string spaceName,
                           bool ifExists) {
        return new DropSpace(plan, input, std::move(spaceName), ifExists);
    }

    std::string explain() const override {
        return "DropSpace";
    }

    const std::string& getSpaceName() const {
        return spaceName_;
    }

    bool getIfExists() const {
        return ifExists_;
    }

private:
    DropSpace(ExecutionPlan* plan,
              PlanNode* input,
              std::string spaceName,
              bool ifExists)
        : SingleInputNode(plan, Kind::kDropSpace, input) {
        spaceName_ = std::move(spaceName);
        ifExists_ = ifExists;
    }

private:
    std::string           spaceName_;
    bool                  ifExists_;
};

class DescSpace final : public SingleInputNode {
public:
    static DescSpace* make(ExecutionPlan* plan,
                           PlanNode* input,
                           std::string spaceName) {
    return new DescSpace(plan, input, std::move(spaceName));
    }

    std::string explain() const override {
        return "DescSpace";
    }

    const std::string& getSpaceName() const {
        return spaceName_;
    }

private:
    DescSpace(ExecutionPlan* plan,
              PlanNode* input,
              std::string spaceName)
        : SingleInputNode(plan, Kind::kDescSpace, input) {
        spaceName_ = std::move(spaceName);
    }

private:
    std::string           spaceName_;
};

class ShowSpaces final : public SingleInputNode {
public:
    static ShowSpaces* make(ExecutionPlan* plan, PlanNode* input) {
        return new ShowSpaces(plan, input);
    }

    std::string explain() const override {
        return "ShowSpaces";
    }

private:
    explicit ShowSpaces(ExecutionPlan* plan, PlanNode* input)
            : SingleInputNode(plan, Kind::kShowSpaces, input) {}
};

class Config final : public SingleInputNode {
public:
    std::string explain() const override {
        return "Config";
    }
};

class ShowCreateSpace final : public SingleInputNode {
public:
    static ShowCreateSpace* make(ExecutionPlan* plan,
                                 PlanNode* input,
                                 std::string spaceName) {
        return new ShowCreateSpace(plan, input, std::move(spaceName));
    }

    std::string explain() const override {
        return "ShowCreateSpace";
    }

    const std::string& getSpaceName() const {
        return spaceName_;
    }

private:
    ShowCreateSpace(ExecutionPlan* plan,
                    PlanNode* input,
                    std::string spaceName)
        : SingleInputNode(plan, Kind::kShowCreateSpace, input) {
        spaceName_ = std::move(spaceName);
    }

private:
    std::string           spaceName_;
};

class Balance final : public SingleInputNode {
public:
    std::string explain() const override {
        return "Balance";
    }
};

class CreateSnapshot final : public SingleInputNode {
public:
    static CreateSnapshot* make(ExecutionPlan* plan, PlanNode* input) {
        return new CreateSnapshot(plan, input);
    }

    std::string explain() const override {
        return "CreateSnapshot";
    }

private:
    explicit CreateSnapshot(ExecutionPlan* plan, PlanNode* input)
        : SingleInputNode(plan, Kind::kCreateSnapshot, input) {}
};

class DropSnapshot final : public SingleInputNode {
public:
    static DropSnapshot* make(ExecutionPlan* plan,
                              PlanNode* input,
                              std::string snapshotName) {
        return new DropSnapshot(plan, input, std::move(snapshotName));
    }

    std::string explain() const override {
        return "DropSnapshot";
    }

    const std::string& getShapshotName() const {
        return shapshotName_;
    }

private:
    explicit DropSnapshot(ExecutionPlan* plan,
                          PlanNode* input,
                          std::string snapshotName)
        : SingleInputNode(plan, Kind::kDropSnapshot, input) {
        shapshotName_ = std::move(snapshotName);
    }

private:
    std::string           shapshotName_;
};

class ShowSnapshots final : public SingleInputNode {
public:
    static ShowSnapshots* make(ExecutionPlan* plan, PlanNode* input) {
        return new ShowSnapshots(plan, input);
    }

    std::string explain() const override {
        return "ShowSnapshots";
    }

private:
    explicit ShowSnapshots(ExecutionPlan* plan, PlanNode* input)
        : SingleInputNode(plan, Kind::kShowSnapshots, input) {}
};

class Download final : public SingleInputNode {
public:
    std::string explain() const override {
        return "Download";
    }
};

class Ingest final : public SingleInputNode {
public:
    std::string explain() const override {
        return "Ingest";
    }
};

// User related Node
class CreateUser final : public CreateNode {
public:
    static CreateUser* make(ExecutionPlan*     plan,
                            PlanNode*          dep,
                            const std::string* username,
                            const std::string* password,
                            bool ifNotExists) {
        return new CreateUser(plan,
                              dep,
                              username,
                              password,
                              ifNotExists);
    }

    std::string explain() const override {
        return "CreateUser";
    }

    const std::string* username() const {
        return username_;
    }

    const std::string* password() const {
        return password_;
    }

private:
    CreateUser(ExecutionPlan* plan,
               PlanNode* dep,
               const std::string* username,
               const std::string* password,
               bool ifNotExists)
        : CreateNode(plan, Kind::kCreateUser, dep, ifNotExists),
          username_(username),
          password_(password) {}

private:
    const std::string* username_;
    const std::string* password_;
};

class DropUser final : public DropNode {
public:
    static DropUser* make(ExecutionPlan*     plan,
                          PlanNode*          dep,
                          const std::string* username,
                          bool ifNotExists) {
        return new DropUser(plan,
                            dep,
                            username,
                            ifNotExists);
    }

    std::string explain() const override {
        return "DropUser";
    }

    const std::string* username() const {
        return username_;
    }

private:
    DropUser(ExecutionPlan* plan, PlanNode* dep, const std::string* username, bool ifNotExists)
        : DropNode(plan, Kind::kDropUser, dep, ifNotExists),
          username_(username) {}

private:
    const std::string* username_;
};

class UpdateUser final : public SingleDependencyNode {
public:
    static UpdateUser* make(ExecutionPlan*     plan,
                            PlanNode*          dep,
                            const std::string* username,
                            const std::string* password) {
        return new UpdateUser(plan,
                              dep,
                              username,
                              password);
    }

    std::string explain() const override {
        return "UpdateUser";
    }

    const std::string* username() const {
        return username_;
    }

    const std::string* password() const {
        return password_;
    }

private:
    UpdateUser(ExecutionPlan* plan,
               PlanNode* dep,
               const std::string* username,
               const std::string* password)
        : SingleDependencyNode(plan, Kind::kUpdateUser, dep),
          username_(username),
          password_(password) {}

private:
    const std::string* username_;
    const std::string* password_;
};

class GrantRole final : public SingleDependencyNode {
public:
    static GrantRole* make(ExecutionPlan* plan,
                           PlanNode*      dep,
                           const std::string* username,
                           const std::string* spaceName,
                           meta::cpp2::RoleType role) {
        return new GrantRole(plan,
                             dep,
                             username,
                             spaceName,
                             role);
    }

    std::string explain() const override {
        return "GrantRole";
    }

    const std::string* username() const {
        return username_;
    }

    const std::string* spaceName() const {
        return spaceName_;
    }

    meta::cpp2::RoleType role() const {
        return role_;
    }

private:
    GrantRole(ExecutionPlan* plan,
              PlanNode* dep,
              const std::string* username,
              const std::string* spaceName,
              meta::cpp2::RoleType role)
        : SingleDependencyNode(plan, Kind::kGrantRole, dep),
          username_(username),
          spaceName_(spaceName),
          role_(role) {}

private:
    const std::string* username_;
    const std::string* spaceName_;
    meta::cpp2::RoleType role_;
};

class RevokeRole final : public SingleDependencyNode {
public:
    static RevokeRole* make(ExecutionPlan* plan,
                            PlanNode*      dep,
                            const std::string* username,
                            const std::string* spaceName,
                            meta::cpp2::RoleType role) {
        return new RevokeRole(plan,
                              dep,
                              username,
                              spaceName,
                              role);
    }

    std::string explain() const override {
        return "RevokeRole";
    }

    const std::string* username() const {
        return username_;
    }

    const std::string* spaceName() const {
        return spaceName_;
    }

    meta::cpp2::RoleType role() const {
        return role_;
    }

private:
    RevokeRole(ExecutionPlan* plan,
               PlanNode*      dep,
               const std::string* username,
               const std::string* spaceName,
               meta::cpp2::RoleType role)
        : SingleDependencyNode(plan, Kind::kRevokeRole, dep),
          username_(username),
          spaceName_(spaceName),
          role_(role) {}

private:
    const std::string*          username_;
    const std::string*          spaceName_;
    meta::cpp2::RoleType role_;
};

class ChangePassword final : public SingleDependencyNode {
public:
    static ChangePassword* make(ExecutionPlan*     plan,
                                PlanNode*          dep,
                                const std::string* username,
                                const std::string* password,
                                const std::string* newPassword) {
        return new ChangePassword(plan,
                                  dep,
                                  username,
                                  password,
                                  newPassword);
    }

    std::string explain() const override {
        return "ChangePassword";
    }

    const std::string* username() const {
        return username_;
    }

    const std::string* password() const {
        return password_;
    }

    const std::string* newPassword() const {
        return newPassword_;
    }

private:
    ChangePassword(ExecutionPlan* plan,
                   PlanNode* dep,
                   const std::string* username,
                   const std::string* password,
                   const std::string* newPassword)
        : SingleDependencyNode(plan, Kind::kChangePassword, dep),
          username_(username),
          password_(password),
          newPassword_(newPassword) {}

private:
    const std::string* username_;
    const std::string* password_;
    const std::string* newPassword_;
};


class ListUserRoles final : public SingleDependencyNode {
public:
    static ListUserRoles* make(ExecutionPlan*     plan,
                               PlanNode*          dep,
                               const std::string* username) {
        return new ListUserRoles(plan,
                                 dep,
                                 username);
    }

    std::string explain() const override {
        return "ListUserRoles";
    }

    const std::string* username() const {
        return username_;
    }

private:
    ListUserRoles(ExecutionPlan* plan, PlanNode* dep, const std::string* username)
        : SingleDependencyNode(plan, Kind::kListUserRoles, dep),
          username_(username) {}

private:
    const std::string* username_;
};

class ListUsers final : public SingleDependencyNode {
public:
    static ListUsers* make(ExecutionPlan* plan, PlanNode* dep) {
        return new ListUsers(plan, dep);
    }

    std::string explain() const override {
        return "ListUsers";
    }

private:
    explicit ListUsers(ExecutionPlan* plan, PlanNode* dep)
        : SingleDependencyNode(plan, Kind::kListUsers, dep) {}
};

class ListRoles final : public SingleDependencyNode {
public:
    static ListRoles* make(ExecutionPlan* plan, PlanNode* dep, GraphSpaceID space) {
        return new ListRoles(plan, dep, space);
    }

    std::string explain() const override {
        return "ListRoles";
    }

    GraphSpaceID space() const {
        return space_;
    }

private:
    explicit ListRoles(ExecutionPlan* plan, PlanNode* dep, GraphSpaceID space)
        : SingleDependencyNode(plan, Kind::kListRoles, dep), space_(space) {}

    GraphSpaceID space_{-1};
};

}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_ADMIN_H_
