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
class Show final : public PlanNode {
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
};

class Balance final : public PlanNode {
};

class CreateSnapshot final : public PlanNode {
};

class DropSnapshot final : public PlanNode {
};

class Download final : public PlanNode {
};

class Ingest final : public PlanNode {
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_ADMIN_H_
