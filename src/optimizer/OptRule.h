/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef OPTIMIZER_OPTRULE_H_
#define OPTIMIZER_OPTRULE_H_

#include <initializer_list>
#include <memory>
#include <string>
#include <vector>

#include "common/base/StatusOr.h"
#include "planner/PlanNode.h"

namespace nebula {
namespace graph {
class QueryContext;
}   // namespace graph

namespace opt {

class OptGroupExpr;
class OptGroup;

struct MatchedResult {
    const OptGroupExpr *node;
    std::vector<MatchedResult> dependencies;
};

class Pattern final {
public:
    static Pattern create(graph::PlanNode::Kind kind, std::initializer_list<Pattern> patterns = {});

    StatusOr<MatchedResult> match(const OptGroupExpr *groupNode) const;

private:
    Pattern() = default;
    StatusOr<MatchedResult> match(const OptGroup *group) const;

    graph::PlanNode::Kind kind_;
    std::vector<Pattern> dependencies_;
};

class OptRule {
public:
    struct TransformResult {
        bool eraseCurr{false};
        bool eraseAll{false};
        std::vector<OptGroupExpr *> newGroupExprs;
    };

    virtual ~OptRule() = default;

    virtual const Pattern &pattern() const = 0;
    virtual bool match(const MatchedResult &matched) const = 0;
    virtual StatusOr<TransformResult> transform(graph::QueryContext *qctx,
                                                const MatchedResult &matched) const = 0;
    virtual std::string toString() const = 0;

protected:
    OptRule() = default;
};

class RuleSet final {
public:
    static RuleSet &DefaultRules();
    static RuleSet &QueryRules();

    RuleSet *addRule(const OptRule *rule);

    void merge(const RuleSet &ruleset);

    const std::vector<const OptRule *> &rules() const {
        return rules_;
    }

private:
    explicit RuleSet(const std::string &name);

    std::string name_;
    std::vector<const OptRule *> rules_;
};

}   // namespace opt
}   // namespace nebula

#endif   // OPTIMIZER_OPTRULE_H_
