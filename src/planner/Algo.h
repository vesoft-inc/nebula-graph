/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_ALGO_H_
#define PLANNER_ALGO_H_

#include "common/base/Base.h"
#include "context/QueryContext.h"
#include "planner/PlanNode.h"

namespace nebula {
namespace graph {
class ProduceSemiShortestPath : public PlanNode {
};

class BFSShortestPath : public SingleInputNode {
public:
    static BFSShortestPath* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new BFSShortestPath(qctx->genId(), input));
    }

private:
    BFSShortestPath(int64_t id, PlanNode* input)
        : SingleInputNode(id, Kind::kBFSShortest, input) {}
};

class ConjunctPath : public BiInputNode {
public:
    static ConjunctPath* make(QueryContext* qctx, PlanNode* left, PlanNode* right) {
        return qctx->objPool()->add(new ConjunctPath(qctx->genId(), left, right));
    }

private:
    ConjunctPath(int64_t id, PlanNode* left, PlanNode* right)
        : BiInputNode(id, Kind::kConjunctPath, left, right) {}
};

}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_ALGO_H_
