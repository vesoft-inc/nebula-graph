/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef NEBULA_GRAPH_OPTIMIZER_RULES_H_
#define NEBULA_GRAPH_OPTIMIZER_RULES_H_

#include "planner/PlanNode.h"
#include "context/QueryContext.h"
namespace nebula {
namespace graph {
class OptRule {
public:
    // Transform plan based on the plan node
    virtual StatusOr<std::shared_ptr<PlanNode>> transform(std::shared_ptr<PlanNode> node);

    // Check whether this rule will match on the plan node
    virtual bool match(const std::shared_ptr<PlanNode>& node) const;

    std::string name() const {
        return name_;
    }

protected:
    OptRule(const std::string& name, QueryContext* qctx)
    : name_(name)
    , qctx_(qctx) {}

    ~OptRule() {};

    QueryContext* queryContext ();

private:
    std::string             name_;
    QueryContext*           qctx_{nullptr};

};

}   // namespace graph
}   // namespace nebula
#endif  // NEBULA_GRAPH_OPTIMIZER_RULES_H_
