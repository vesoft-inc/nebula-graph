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
class Show final : public PlanNode {
 public:
    std::string explain() const override {
        return "Show";
    }
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

class DropSpace final : public PlanNode {
public:
    static DropSpace* make(ExecutionPlan* plan,
                           std::string spaceName,
                           bool ifExists) {
        return new DropSpace(plan, std::move(spaceName), ifExists);
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
              std::string spaceName,
              bool ifExists)
        : PlanNode(plan, Kind::kDropSpace) {
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

class ShowSpaces final : public PlanNode {
public:
    static ShowSpaces* make(ExecutionPlan* plan) {
        return new ShowSpaces(plan);
    }

    std::string explain() const override {
        return "ShowSpaces";
    }

private:
    explicit ShowSpaces(ExecutionPlan* plan)
            : PlanNode(plan, Kind::kShowSpaces) {}
};

class Config final : public PlanNode {
public:
    std::string explain() const override {
        return "Config";
    }
};

class Balance final : public PlanNode {
public:
    std::string explain() const override {
        return "Balance";
    }
};

class CreateSnapshot final : public PlanNode {
public:
    static CreateSnapshot* make(ExecutionPlan* plan) {
        return new CreateSnapshot(plan);
    }

    std::string explain() const override {
        return "CreateSnapshot";
    }

private:
    explicit CreateSnapshot(ExecutionPlan* plan)
        : PlanNode(plan, Kind::kCreateSnapshot) {}
};

class DropSnapshot final : public PlanNode {
public:
    static DropSnapshot* make(ExecutionPlan* plan,
                              std::string snapshotName) {
        return new DropSnapshot(plan, std::move(snapshotName));
    }

    std::string explain() const override {
        return "DropSnapshot";
    }

    const std::string& getShapshotName() const {
        return shapshotName_;
    }

private:
    explicit DropSnapshot(ExecutionPlan* plan, std::string snapshotName)
        : PlanNode(plan, Kind::kDropSnapshot) {
        shapshotName_ = std::move(snapshotName);
    }

private:
    std::string           shapshotName_;
};

class ShowSnapshots final : public PlanNode {
public:
    static ShowSnapshots* make(ExecutionPlan* plan) {
        return new ShowSnapshots(plan);
    }

    std::string explain() const override {
        return "ShowSnapshots";
    }

private:
    explicit ShowSnapshots(ExecutionPlan* plan)
        : PlanNode(plan, Kind::kShowSnapshots) {}
};

class Download final : public PlanNode {
public:
    std::string explain() const override {
        return "Download";
    }
};

class Ingest final : public PlanNode {
public:
    std::string explain() const override {
        return "Ingest";
    }
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_ADMIN_H_
