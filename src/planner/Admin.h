/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_ADMIN_H_
#define PLANNER_ADMIN_H_

#include "planner/PlanNode.h"
#include "interface/gen-cpp2/meta_types.h"
#include "clients/meta/MetaClient.h"

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
    enum class ShowKind {
        kUnknown,
        kHosts,
    };

    static Show* make(ExecutionPlan* plan,
                      ShowKind kind) {
        return new Show(plan, kind);
    }

    std::string explain() const override {
        return "Show " + toString(showKind_);
    }

    ShowKind showKind() const {
        return showKind_;
    }

private:
    static std::string toString(ShowKind kind) {
        switch (kind) {
        case ShowKind::kUnknown:
            return "Unknown";
        case ShowKind::kHosts:
            return "Hosts";
        // No default so the compiler will warning when lack
        }
        return "Unknown";
    }

    Show(ExecutionPlan* plan, ShowKind kind) : PlanNode(plan, Kind::kShow), showKind_(kind) {}

    ShowKind showKind_{ShowKind::kUnknown};
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

class Balance final : public PlanNode {
public:
    std::string explain() const override {
        return "Balance";
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
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_ADMIN_H_
