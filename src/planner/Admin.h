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

class CreateSpace final : public CreateNode {
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

private:
    CreateSpace(ExecutionPlan* plan,
                meta::SpaceDesc props,
                bool ifNotExists)
        : CreateNode(plan, Kind::kCreateSpace, ifNotExists), props_(std::move(props)) {}

private:
    meta::SpaceDesc               props_;
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

// User related Node
class CreateUser final : public CreateNode {
public:
    static CreateUser* make(ExecutionPlan* plan,
                            std::string username,
                            std::string password,
                            bool ifNotExists) {
        return new CreateUser(plan,
                            std::move(username),
                            std::move(password),
                            ifNotExists);
    }

    std::string explain() const override {
        return "CreateUser";
    }

    const std::string& username() const {
        return username_;
    }

    const std::string& password() const {
        return password_;
    }

private:
    CreateUser(ExecutionPlan* plan, std::string username, std::string password,  bool ifNotExists)
        : CreateNode(plan, Kind::kCreateUser, ifNotExists),
          username_(std::move(username)),
          password_(std::move(password)) {}

private:
    std::string username_;
    std::string password_;
};

class DropUser final : public DropNode {
public:
    static DropUser* make(ExecutionPlan* plan,
                            std::string username,
                            bool ifNotExists) {
        return new DropUser(plan,
                            std::move(username),
                            ifNotExists);
    }

    std::string explain() const override {
        return "DropUser";
    }

    const std::string& username() const {
        return username_;
    }

private:
    DropUser(ExecutionPlan* plan, std::string username, bool ifNotExists)
        : DropNode(plan, Kind::kDropUser, ifNotExists),
          username_(std::move(username)) {}

private:
    std::string username_;
};

}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_ADMIN_H_
