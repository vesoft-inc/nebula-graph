/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MUTATE_H_
#define PLANNER_MUTATE_H_

#include "interface/gen-cpp2/storage_types.h"
#include "PlanNode.h"

/**
 * All mutate-related nodes would put in this file.
 */
namespace nebula {
namespace graph {
class InsertVertices final : public PlanNode {
public:
    static InsertVertices* make(ExecutionPlan* plan,
                                GraphSpaceID spaceId,
                                std::vector<storage::cpp2::NewVertex> vertices,
                                bool overwritable) {
        return new InsertVertices(plan,
                                  spaceId,
                                  std::move(vertices),
                                  overwritable);
    }

    std::string explain() const override {
        return "InsertVertices";
    }

    GraphSpaceID space() const {
        return space_;
    }

    std::vector<storage::cpp2::NewVertex> getVertices() const {
        return std::move(vertices_);
    }

    bool getOverwritable() const {
        return overwritable_;
    }

private:
    InsertVertices(ExecutionPlan* plan,
                   GraphSpaceID spaceId,
                   std::vector<storage::cpp2::NewVertex> vertices,
                   bool overwritable)
    : PlanNode(plan)
    , kind_(Kind::kInsertVertices)
    , space_(spaceId)
    , vertices_(std::move(vertices))
    , overwritable_(overwritable) {}

private:
    GraphSpaceID                               space_;
    std::vector<storage::cpp2::NewVertex>      vertices_;
    bool                                       overwritable_;
};

class InsertEdges final : public PlanNode {
public:
    static InsertEdges* make(ExecutionPlan* plan,
                             GraphSpaceID spaceId,
                             std::vector<storage::cpp2::NewEdge> edges,
                             bool overwritable) {
        return new InsertEdges(plan,
                               spaceId,
                               std::move(edges),
                               overwritable);
    }

    std::string explain() const override {
        return "InsertEdges";
    }

    GraphSpaceID space() const {
        return space_;
    }

    std::vector<storage::cpp2::NewEdge> getEdges() const {
        return std::move(edges_);
    }

    bool getOverwritable() const {
        return overwritable_;
    }

private:
    InsertEdges(ExecutionPlan* plan,
                GraphSpaceID spaceId,
                std::vector<storage::cpp2::NewEdge> edges,
                bool overwritable)
    : PlanNode(plan)
    , kind_(Kind::kInsertEdges)
    , space_(spaceId)
    , edges_(std::move(edges))
    , overwritable_(overwritable) {}

private:
    GraphSpaceID                               space_;
    std::vector<storage::cpp2::NewEdge>        edges_;
    bool                                       overwritable_;
};

class UpdateVertex final : public PlanNode {
};

class UpdateEdge final : public PlanNode {
};

class DeleteVertex final : public PlanNode {
};

class DeleteEdge final : public PlanNode {
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_MUTATE_H_
