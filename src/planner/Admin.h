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

// TODO: All DDLs, DMLs and DQLs could be used in a single query
// which would make them in a single and big execution plan

class ShowHosts final : public SingleDependencyNode {
    // TODO(shylock) meta/storage/graph enumerate
public:
    static ShowHosts* make(ExecutionPlan* plan, PlanNode* dep) {
        return new ShowHosts(plan, dep);
    }

private:
    explicit ShowHosts(ExecutionPlan* plan, PlanNode* dep)
        : SingleDependencyNode(plan, Kind::kShowHosts, dep) {}
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

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

public:
    const meta::SpaceDesc& getSpaceDesc() const {
        return props_;
    }

    bool getIfNotExists() const {
        return ifNotExists_;
    }

private:
    CreateSpace(ExecutionPlan* plan,
                PlanNode* input,
                meta::SpaceDesc props,
                bool ifNotExists)
        : SingleInputNode(plan, Kind::kCreateSpace, input) {
        props_ = std::move(props);
        ifNotExists_ = ifNotExists;
    }

private:
    meta::SpaceDesc               props_;
    bool                          ifNotExists_;
};

class DropSpace final : public SingleInputNode {
public:
    static DropSpace* make(ExecutionPlan* plan,
                           PlanNode* input,
                           std::string spaceName,
                           bool ifExists) {
        return new DropSpace(plan, input, std::move(spaceName), ifExists);
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

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

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

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

private:
    explicit ShowSpaces(ExecutionPlan* plan, PlanNode* input)
            : SingleInputNode(plan, Kind::kShowSpaces, input) {}
};

class Config final : public SingleInputNode {
public:
};

class ShowCreateSpace final : public SingleInputNode {
public:
    static ShowCreateSpace* make(ExecutionPlan* plan,
                                 PlanNode* input,
                                 std::string spaceName) {
        return new ShowCreateSpace(plan, input, std::move(spaceName));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

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

class CreateSnapshot final : public SingleInputNode {
public:
    static CreateSnapshot* make(ExecutionPlan* plan, PlanNode* input) {
        return new CreateSnapshot(plan, input);
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

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    const std::string& getShapshotName() const {
        return snapshotName_;
    }

private:
    explicit DropSnapshot(ExecutionPlan* plan,
                          PlanNode* input,
                          std::string snapshotName)
        : SingleInputNode(plan, Kind::kDropSnapshot, input) {
        snapshotName_ = std::move(snapshotName);
    }

private:
    std::string           snapshotName_;
};

class ShowSnapshots final : public SingleInputNode {
public:
    static ShowSnapshots* make(ExecutionPlan* plan, PlanNode* input) {
        return new ShowSnapshots(plan, input);
    }

private:
    explicit ShowSnapshots(ExecutionPlan* plan, PlanNode* input)
        : SingleInputNode(plan, Kind::kShowSnapshots, input) {}
};

class Download final : public SingleInputNode {
public:
};

class Ingest final : public SingleInputNode {
public:
};

class ShowParts final : public SingleInputNode {
public:
    static ShowParts* make(ExecutionPlan* plan,
                           PlanNode* input,
                           GraphSpaceID spaceId,
                           std::vector<PartitionID> partIds) {
        return new ShowParts(plan, input, spaceId, std::move(partIds));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    GraphSpaceID getSpaceId() const {
        return spaceId_;
    }

    const std::vector<PartitionID>& getPartIds() const {
        return partIds_;
    }

private:
    explicit ShowParts(ExecutionPlan* plan,
                       PlanNode* input,
                       GraphSpaceID spaceId,
                       std::vector<PartitionID> partIds)
        : SingleInputNode(plan, Kind::kShowParts, input) {
        spaceId_ = spaceId;
        partIds_ = std::move(partIds);
    }

private:
    GraphSpaceID                       spaceId_{-1};
    std::vector<PartitionID>           partIds_;
};

class BalanceLeaders final : public SingleDependencyNode {
public:
    static BalanceLeaders* make(ExecutionPlan* plan, PlanNode* dep) {
        return new BalanceLeaders(plan, dep);
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override {
        return nullptr;
    }

private:
    explicit BalanceLeaders(ExecutionPlan* plan, PlanNode* dep)
        : SingleDependencyNode(plan, Kind::kBalanceLeaders, dep) {}
};

class Balance final : public SingleDependencyNode {
public:
    static Balance* make(ExecutionPlan* plan, PlanNode* dep, std::vector<HostAddr> deleteHosts) {
        return new Balance(plan, dep, std::move(deleteHosts));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override {
        return nullptr;
    }

    const std::vector<HostAddr> &deleteHosts() const {
        return deleteHosts_;
    }

private:
    Balance(ExecutionPlan* plan, PlanNode* dep, std::vector<HostAddr> deleteHosts)
        : SingleDependencyNode(plan, Kind::kBalance, dep), deleteHosts_(std::move(deleteHosts)) {}

    std::vector<HostAddr> deleteHosts_;
};

class StopBalance final : public SingleDependencyNode {
public:
    static StopBalance* make(ExecutionPlan* plan, PlanNode* dep) {
        return new StopBalance(plan, dep);
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override {
        return nullptr;
    }

private:
    explicit StopBalance(ExecutionPlan* plan, PlanNode* dep)
        : SingleDependencyNode(plan, Kind::kStopBalance, dep) {}
};

class ShowBalance final : public SingleDependencyNode {
public:
    static ShowBalance* make(ExecutionPlan* plan, PlanNode* dep, int64_t id) {
        return new ShowBalance(plan, dep, id);
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override {
        return nullptr;
    }

    int64_t id() const {
        return id_;
    }

private:
    ShowBalance(ExecutionPlan* plan, PlanNode* dep, int64_t id)
        : SingleDependencyNode(plan, Kind::kShowBalance, dep), id_(id) {}

    int64_t id_;
};

}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_ADMIN_H_
