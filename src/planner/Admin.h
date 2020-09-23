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
    CreateNode(int64_t id,
               Kind kind,
               PlanNode* input,
               bool ifNotExist,
               SymbolTable* symTable)
        : SingleDependencyNode(id, kind, input, symTable), ifNotExist_(ifNotExist) {}

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

public:
    bool ifNotExist() const {
        return ifNotExist_;
    }

private:
    bool ifNotExist_{false};
};

class DropNode : public SingleDependencyNode {
protected:
    DropNode(int64_t id, Kind kind, PlanNode* input, bool ifExist, SymbolTable* symTable)
        : SingleDependencyNode(id, kind, input, symTable), ifExist_(ifExist) {}

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

public:
    bool ifExist() const {
        return ifExist_;
    }

private:
    bool ifExist_{false};
};

class ShowHosts final : public SingleDependencyNode {
    // TODO(shylock) meta/storage/graph enumerate
public:
    static ShowHosts* make(QueryContext* qctx, PlanNode* dep) {
        return qctx->objPool()->add(new ShowHosts(qctx->genId(), dep, qctx->symTable()));
    }

private:
    ShowHosts(int64_t id, PlanNode* dep, SymbolTable* symTable)
        : SingleDependencyNode(id, Kind::kShowHosts, dep, symTable) {}
};

class CreateSpace final : public SingleInputNode {
public:
    static CreateSpace* make(QueryContext* qctx,
                             PlanNode* input,
                             meta::cpp2::SpaceDesc spaceDesc,
                             bool ifNotExists) {
        return qctx->objPool()->add(new CreateSpace(
            qctx->genId(), input, std::move(spaceDesc), ifNotExists, qctx->symTable()));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

public:
    const meta::cpp2::SpaceDesc& getSpaceDesc() const {
        return spaceDesc_;
    }

    bool getIfNotExists() const {
        return ifNotExists_;
    }

private:
    CreateSpace(int64_t id,
                PlanNode* input,
                meta::cpp2::SpaceDesc spaceDesc,
                bool ifNotExists,
                SymbolTable* symTable)
        : SingleInputNode(id, Kind::kCreateSpace, input, symTable) {
        spaceDesc_ = std::move(spaceDesc);
        ifNotExists_ = ifNotExists;
    }

private:
    meta::cpp2::SpaceDesc     spaceDesc_;
    bool                      ifNotExists_{false};
};

class DropSpace final : public SingleInputNode {
public:
    static DropSpace* make(QueryContext* qctx,
                           PlanNode* input,
                           std::string spaceName,
                           bool ifExists) {
        return qctx->objPool()->add(new DropSpace(
            qctx->genId(), input, std::move(spaceName), ifExists, qctx->symTable()));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    const std::string& getSpaceName() const {
        return spaceName_;
    }

    bool getIfExists() const {
        return ifExists_;
    }

private:
    DropSpace(int64_t id,
              PlanNode* input,
              std::string spaceName,
              bool ifExists,
              SymbolTable* symTable)
        : SingleInputNode(id, Kind::kDropSpace, input, symTable) {
        spaceName_ = std::move(spaceName);
        ifExists_ = ifExists;
    }

private:
    std::string           spaceName_;
    bool                  ifExists_;
};

class DescSpace final : public SingleInputNode {
public:
    static DescSpace* make(QueryContext* qctx, PlanNode* input, std::string spaceName) {
        return qctx->objPool()->add(
            new DescSpace(qctx->genId(), input, std::move(spaceName), qctx->symTable()));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    const std::string& getSpaceName() const {
        return spaceName_;
    }

private:
    DescSpace(int64_t id,
              PlanNode* input,
              std::string spaceName,
              SymbolTable* symTable)
        : SingleInputNode(id, Kind::kDescSpace, input, symTable) {
        spaceName_ = std::move(spaceName);
    }

private:
    std::string           spaceName_;
};

class ShowSpaces final : public SingleInputNode {
public:
    static ShowSpaces* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new ShowSpaces(qctx->genId(), input, qctx->symTable()));
    }

private:
    explicit ShowSpaces(int64_t id, PlanNode* input, SymbolTable* symTable)
        : SingleInputNode(id, Kind::kShowSpaces, input, symTable) {}
};

class ShowConfigs final : public SingleInputNode {
public:
    static ShowConfigs* make(QueryContext* qctx, PlanNode* input, meta::cpp2::ConfigModule module) {
        return qctx->objPool()->add(
            new ShowConfigs(qctx->genId(), input, module, qctx->symTable()));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    meta::cpp2::ConfigModule getModule() const {
        return module_;
    }

private:
    ShowConfigs(int64_t id,
                PlanNode* input,
                meta::cpp2::ConfigModule module,
                SymbolTable* symTable)
        : SingleInputNode(id, Kind::kShowConfigs, input, symTable)
        , module_(module) {}

private:
    meta::cpp2::ConfigModule    module_;
};

class SetConfig final : public SingleInputNode {
public:
    static SetConfig* make(QueryContext* qctx,
                           PlanNode* input,
                           meta::cpp2::ConfigModule module,
                           std::string name,
                           Value value) {
        return qctx->objPool()->add(new SetConfig(
            qctx->genId(), input, module, std::move(name), std::move(value), qctx->symTable()));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    meta::cpp2::ConfigModule getModule() const {
        return module_;
    }

    const std::string& getName() const {
        return name_;
    }

    const Value& getValue() const {
        return value_;
    }

private:
    SetConfig(int64_t id,
              PlanNode* input,
              meta::cpp2::ConfigModule module,
              std::string name,
              Value value,
              SymbolTable* symTable)
        : SingleInputNode(id, Kind::kSetConfig, input, symTable),
          module_(module),
          name_(std::move(name)),
          value_(std::move(value)) {}

private:
    meta::cpp2::ConfigModule module_;
    std::string name_;
    Value value_;
};

class GetConfig final : public SingleInputNode {
public:
    static GetConfig* make(QueryContext* qctx,
                           PlanNode* input,
                           meta::cpp2::ConfigModule module,
                           std::string name) {
        return qctx->objPool()->add(
            new GetConfig(qctx->genId(), input, module, std::move(name), qctx->symTable()));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    meta::cpp2::ConfigModule getModule() const {
        return module_;
    }

    const std::string& getName() const {
        return name_;
    }

private:
    explicit GetConfig(int64_t id,
                       PlanNode* input,
                       meta::cpp2::ConfigModule module,
                       std::string name,
                       SymbolTable* symTable)
        : SingleInputNode(id, Kind::kGetConfig, input, symTable),
          module_(module),
          name_(std::move(name)) {}

private:
    meta::cpp2::ConfigModule module_;
    std::string name_;
};

class ShowCreateSpace final : public SingleInputNode {
public:
    static ShowCreateSpace* make(QueryContext* qctx, PlanNode* input, std::string spaceName) {
        return qctx->objPool()->add(
            new ShowCreateSpace(qctx->genId(), input, std::move(spaceName), qctx->symTable()));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    const std::string& getSpaceName() const {
        return spaceName_;
    }

private:
    ShowCreateSpace(int64_t id, PlanNode* input, std::string spaceName, SymbolTable* symTable)
        : SingleInputNode(id, Kind::kShowCreateSpace, input, symTable) {
        spaceName_ = std::move(spaceName);
    }

private:
    std::string spaceName_;
};

class CreateSnapshot final : public SingleInputNode {
public:
    static CreateSnapshot* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new CreateSnapshot(qctx->genId(), input, qctx->symTable()));
    }

private:
    explicit CreateSnapshot(int64_t id, PlanNode* input, SymbolTable* symTable)
        : SingleInputNode(id, Kind::kCreateSnapshot, input, symTable) {}
};

class DropSnapshot final : public SingleInputNode {
public:
    static DropSnapshot* make(QueryContext* qctx, PlanNode* input, std::string snapshotName) {
        return qctx->objPool()->add(
            new DropSnapshot(qctx->genId(), input, std::move(snapshotName), qctx->symTable()));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    const std::string& getShapshotName() const {
        return snapshotName_;
    }

private:
    explicit DropSnapshot(int64_t id,
                          PlanNode* input,
                          std::string snapshotName,
                          SymbolTable* symTable)
        : SingleInputNode(id, Kind::kDropSnapshot, input, symTable) {
        snapshotName_ = std::move(snapshotName);
    }

private:
    std::string snapshotName_;
};

class ShowSnapshots final : public SingleInputNode {
public:
    static ShowSnapshots* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new ShowSnapshots(qctx->genId(), input, qctx->symTable()));
    }

private:
    explicit ShowSnapshots(int64_t id, PlanNode* input, SymbolTable* symTable)
        : SingleInputNode(id, Kind::kShowSnapshots, input, symTable) {}
};

class Download final : public SingleInputNode {
public:
};

class Ingest final : public SingleInputNode {
public:
};

// User related Node
class CreateUser final : public CreateNode {
public:
    static CreateUser* make(QueryContext* qctx,
                            PlanNode* dep,
                            const std::string* username,
                            const std::string* password,
                            bool ifNotExists) {
        return qctx->objPool()->add(
            new CreateUser(qctx->genId(), dep, username, password, ifNotExists, qctx->symTable()));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    const std::string* username() const {
        return username_;
    }

    const std::string* password() const {
        return password_;
    }

private:
    CreateUser(int64_t id,
               PlanNode* dep,
               const std::string* username,
               const std::string* password,
               bool ifNotExists,
               SymbolTable* symTable)
        : CreateNode(id, Kind::kCreateUser, dep, ifNotExists, symTable),
          username_(username),
          password_(password) {}

private:
    const std::string* username_;
    const std::string* password_;
};

class DropUser final : public DropNode {
public:
    static DropUser* make(QueryContext* qctx,
                          PlanNode* dep,
                          const std::string* username,
                          bool ifNotExists) {
        return qctx->objPool()->add(
            new DropUser(qctx->genId(), dep, username, ifNotExists, qctx->symTable()));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    const std::string* username() const {
        return username_;
    }

private:
    DropUser(int64_t id,
             PlanNode* dep,
             const std::string* username,
             bool ifNotExists,
             SymbolTable* symTable)
        : DropNode(id, Kind::kDropUser, dep, ifNotExists, symTable), username_(username) {}

private:
    const std::string* username_;
};

class UpdateUser final : public SingleDependencyNode {
public:
    static UpdateUser* make(QueryContext* qctx,
                            PlanNode* dep,
                            const std::string* username,
                            const std::string* password) {
        return qctx->objPool()->add(
            new UpdateUser(qctx->genId(), dep, username, password, qctx->symTable()));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    const std::string* username() const {
        return username_;
    }

    const std::string* password() const {
        return password_;
    }

private:
    UpdateUser(int64_t id,
               PlanNode* dep,
               const std::string* username,
               const std::string* password,
               SymbolTable* symTable)
        : SingleDependencyNode(id, Kind::kUpdateUser, dep, symTable),
          username_(username),
          password_(password) {}

private:
    const std::string* username_;
    const std::string* password_;
};

class GrantRole final : public SingleDependencyNode {
public:
    static GrantRole* make(QueryContext* qctx,
                           PlanNode* dep,
                           const std::string* username,
                           const std::string* spaceName,
                           meta::cpp2::RoleType role) {
        return qctx->objPool()->add(
            new GrantRole(qctx->genId(), dep, username, spaceName, role, qctx->symTable()));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

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
    GrantRole(int64_t id,
              PlanNode* dep,
              const std::string* username,
              const std::string* spaceName,
              meta::cpp2::RoleType role,
              SymbolTable* symTable)
        : SingleDependencyNode(id, Kind::kGrantRole, dep, symTable),
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
    static RevokeRole* make(QueryContext* qctx,
                            PlanNode* dep,
                            const std::string* username,
                            const std::string* spaceName,
                            meta::cpp2::RoleType role) {
        return qctx->objPool()->add(
            new RevokeRole(qctx->genId(), dep, username, spaceName, role, qctx->symTable()));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

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
    RevokeRole(int64_t id,
               PlanNode* dep,
               const std::string* username,
               const std::string* spaceName,
               meta::cpp2::RoleType role,
               SymbolTable* symTable)
        : SingleDependencyNode(id, Kind::kRevokeRole, dep, symTable),
          username_(username),
          spaceName_(spaceName),
          role_(role) {}

private:
    const std::string* username_;
    const std::string* spaceName_;
    meta::cpp2::RoleType role_;
};

class ChangePassword final : public SingleDependencyNode {
public:
    static ChangePassword* make(QueryContext* qctx,
                                PlanNode* dep,
                                const std::string* username,
                                const std::string* password,
                                const std::string* newPassword) {
        return qctx->objPool()->add(new ChangePassword(
            qctx->genId(), dep, username, password, newPassword, qctx->symTable()));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

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
    ChangePassword(int64_t id,
                   PlanNode* dep,
                   const std::string* username,
                   const std::string* password,
                   const std::string* newPassword,
                   SymbolTable* symTable)
        : SingleDependencyNode(id, Kind::kChangePassword, dep, symTable),
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
    static ListUserRoles* make(QueryContext* qctx, PlanNode* dep, const std::string* username) {
        return qctx->objPool()->add(
            new ListUserRoles(qctx->genId(), dep, username, qctx->symTable()));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    const std::string* username() const {
        return username_;
    }

private:
    ListUserRoles(int64_t id, PlanNode* dep, const std::string* username, SymbolTable* symTable)
        : SingleDependencyNode(id, Kind::kListUserRoles, dep, symTable),
          username_(username) {}

private:
    const std::string* username_;
};

class ListUsers final : public SingleDependencyNode {
public:
    static ListUsers* make(QueryContext* qctx, PlanNode* dep) {
        return qctx->objPool()->add(new ListUsers(qctx->genId(), dep, qctx->symTable()));
    }

private:
    explicit ListUsers(int64_t id, PlanNode* dep, SymbolTable* symTable)
        : SingleDependencyNode(id, Kind::kListUsers, dep, symTable) {}
};

class ListRoles final : public SingleDependencyNode {
public:
    static ListRoles* make(QueryContext* qctx, PlanNode* dep, GraphSpaceID space) {
        return qctx->objPool()->add(new ListRoles(qctx->genId(), dep, space, qctx->symTable()));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    GraphSpaceID space() const {
        return space_;
    }

private:
    explicit ListRoles(int64_t id, PlanNode* dep, GraphSpaceID space, SymbolTable* symTable)
        : SingleDependencyNode(id, Kind::kListRoles, dep, symTable), space_(space) {}

    GraphSpaceID space_{-1};
};

class ShowParts final : public SingleInputNode {
public:
    static ShowParts* make(QueryContext* qctx,
                           PlanNode* input,
                           GraphSpaceID spaceId,
                           std::vector<PartitionID> partIds) {
        return qctx->objPool()->add(
            new ShowParts(qctx->genId(), input, spaceId, std::move(partIds), qctx->symTable()));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    GraphSpaceID getSpaceId() const {
        return spaceId_;
    }

    const std::vector<PartitionID>& getPartIds() const {
        return partIds_;
    }

private:
    explicit ShowParts(int64_t id,
                       PlanNode* input,
                       GraphSpaceID spaceId,
                       std::vector<PartitionID> partIds,
                       SymbolTable* symTable)
        : SingleInputNode(id, Kind::kShowParts, input, symTable) {
        spaceId_ = spaceId;
        partIds_ = std::move(partIds);
    }

private:
    GraphSpaceID                       spaceId_{-1};
    std::vector<PartitionID>           partIds_;
};

class SubmitJob final : public SingleDependencyNode {
public:
    static SubmitJob* make(QueryContext* qctx,
                           PlanNode* dep,
                           meta::cpp2::AdminJobOp op,
                           const std::vector<std::string>& params) {
        return qctx->objPool()->add(
            new SubmitJob(qctx->genId(), dep, op, params, qctx->symTable()));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

public:
    meta::cpp2::AdminJobOp jobOp() const {
        return op_;
    }

    meta::cpp2::AdminCmd cmd() const {
        return cmd_;
    }

    const std::vector<std::string> &params() const {
        return params_;
    }

private:
    SubmitJob(int64_t id,
              PlanNode* dep,
              meta::cpp2::AdminJobOp op,
              const std::vector<std::string> &params,
              SymbolTable* symTable)
        : SingleDependencyNode(id, Kind::kSubmitJob, dep, symTable),
          op_(op),
          params_(params) {}


private:
    meta::cpp2::AdminJobOp         op_;
    meta::cpp2::AdminCmd           cmd_;
    const std::vector<std::string> params_;
};

class BalanceLeaders final : public SingleDependencyNode {
public:
    static BalanceLeaders* make(QueryContext* qctx, PlanNode* dep) {
        return qctx->objPool()->add(new BalanceLeaders(qctx->genId(), dep, qctx->symTable()));
    }

private:
    explicit BalanceLeaders(int64_t id, PlanNode* dep, SymbolTable* symTable)
        : SingleDependencyNode(id, Kind::kBalanceLeaders, dep, symTable) {}
};

class Balance final : public SingleDependencyNode {
public:
    static Balance* make(QueryContext* qctx, PlanNode* dep, std::vector<HostAddr> deleteHosts) {
        return qctx->objPool()->add(
            new Balance(qctx->genId(), dep, std::move(deleteHosts), qctx->symTable()));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    const std::vector<HostAddr> &deleteHosts() const {
        return deleteHosts_;
    }

private:
    Balance(int64_t id, PlanNode* dep, std::vector<HostAddr> deleteHosts, SymbolTable* symTable)
        : SingleDependencyNode(id, Kind::kBalance, dep, symTable),
          deleteHosts_(std::move(deleteHosts)) {}

    std::vector<HostAddr> deleteHosts_;
};

class StopBalance final : public SingleDependencyNode {
public:
    static StopBalance* make(QueryContext* qctx, PlanNode* dep) {
        return qctx->objPool()->add(new StopBalance(qctx->genId(), dep, qctx->symTable()));
    }

private:
    explicit StopBalance(int64_t id, PlanNode* dep, SymbolTable* symTable)
        : SingleDependencyNode(id, Kind::kStopBalance, dep, symTable) {}
};

class ShowBalance final : public SingleDependencyNode {
public:
    static ShowBalance* make(QueryContext* qctx, PlanNode* dep, int64_t jobId) {
        return qctx->objPool()->add(new ShowBalance(qctx->genId(), dep, jobId, qctx->symTable()));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    int64_t jobId() const {
        return jobId_;
    }

private:
    ShowBalance(int64_t id, PlanNode* dep, int64_t jobId, SymbolTable* symTable)
        : SingleDependencyNode(id, Kind::kShowBalance, dep, symTable), jobId_(jobId) {}

    int64_t jobId_;
};

class ShowCharset final : public SingleInputNode {
public:
    static ShowCharset* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new ShowCharset(qctx->genId(), input, qctx->symTable()));
    }

private:
    explicit ShowCharset(int64_t id, PlanNode* input, SymbolTable* symTable)
        : SingleInputNode(id, Kind::kShowCharset, input, symTable) {}
};

class ShowCollation final : public SingleInputNode {
public:
    static ShowCollation* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new ShowCollation(qctx->genId(), input, qctx->symTable()));
    }

private:
    explicit ShowCollation(int64_t id, PlanNode* input, SymbolTable* symTable)
        : SingleInputNode(id, Kind::kShowCollation, input, symTable) {}
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_ADMIN_H_
