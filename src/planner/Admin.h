/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_ADMIN_H_
#define PLANNER_ADMIN_H_

#include "planner/PlanNode.h"
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

class CreateSpace final : public PlanNode {
public:
    static CreateSpace* make(ExecutionPlan* plan,
                             meta::SpaceDesc props,
                             bool ifNotExists) {
    return new CreateSpace(plan,
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
                meta::SpaceDesc props,
                bool ifNotExists)
        : PlanNode(plan, Kind::kCreateSpace) {
            props_ = std::move(props);
            ifNotExists_ = ifNotExists;
        }


private:
    meta::SpaceDesc               props_;
    bool                          ifNotExists_;
};

class DropSpace final : public PlanNode {
public:
    std::string explain() const override {
        return "DropSpace";
    }
};

class DescSpace final : public PlanNode {
public:
    static DescSpace* make(ExecutionPlan* plan,
                           std::string spaceName) {
    return new DescSpace(plan, std::move(spaceName));
    }

    std::string explain() const override {
        return "DescSpace";
    }

    const std::string& getSpaceName() const {
        return spaceName_;
    }

private:
    DescSpace(ExecutionPlan* plan,
              std::string spaceName)
        : PlanNode(plan, Kind::kDescSpace) {
            spaceName_ = std::move(spaceName);
        }

private:
    std::string           spaceName_;
};

class Config final : public PlanNode {
public:
    std::string explain() const override {
        return "Config";
    }
};

class CreateSnapshot final : public PlanNode {
public:
    std::string explain() const override {
        return "CreateSnapshot";
    }
};

class DropSnapshot final : public PlanNode {
public:
    std::string explain() const override {
        return "DropSnapshot";
    }
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

class BalanceLeaders final : public PlanNode {
public:
    static BalanceLeaders* make(ExecutionPlan* plan) {
        return new BalanceLeaders(plan);
    }

    std::string explain() const override {
        return "BalanceLeaders";
    }

private:
    explicit BalanceLeaders(ExecutionPlan* plan)
        : PlanNode(plan, Kind::kBalanceLeaders) {}
};

class Balance final : public PlanNode {
public:
    static Balance* make(ExecutionPlan* plan, std::vector<HostAddr> deleteHosts) {
        return new Balance(plan, std::move(deleteHosts));
    }

    std::string explain() const override {
        return "Balance";
    }

    const std::vector<HostAddr> &deleteHosts() const {
        return deleteHosts_;
    }

private:
    Balance(ExecutionPlan* plan, std::vector<HostAddr> deleteHosts)
        : PlanNode(plan, Kind::kBalance), deleteHosts_(std::move(deleteHosts)) {}

    std::vector<HostAddr> deleteHosts_;
};

class StopBalance final : public PlanNode {
public:
    static StopBalance* make(ExecutionPlan* plan) {
        return new StopBalance(plan);
    }

    std::string explain() const override {
        return "StopBalance";
    }

private:
    explicit StopBalance(ExecutionPlan* plan)
        : PlanNode(plan, Kind::kStopBalance) {}
};

class ShowBalance final : public PlanNode {
public:
    static ShowBalance* make(ExecutionPlan* plan, int64_t id) {
        return new ShowBalance(plan, id);
    }

    std::string explain() const override {
        return "ShowBalance";
    }

    int64_t id() const {
        return id_;
    }

private:
    ShowBalance(ExecutionPlan* plan, int64_t id)
        : PlanNode(plan, Kind::kShowBalance), id_(id) {}

    int64_t id_;
};

}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_ADMIN_H_
