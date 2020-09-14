/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef OPTIMIZER_RULE_LIMITPUSHDOWN_H_
#define OPTIMIZER_RULE_LIMITPUSHDOWN_H_

#include <memory>

#include "optimizer/OptRule.h"

namespace nebula {
namespace graph {
class Limit;
class Project;
class GetNeighbors;
}   // namespace graph

namespace opt {

class LimitPushDownRule final : public OptRule {
public:
    static std::unique_ptr<OptRule> kInstance;

    bool match(const OptGroupExpr *groupExpr) const override;
    Status transform(graph::QueryContext *qctx,
                     const OptGroupExpr *groupExpr,
                     TransformResult *result) const override;
    std::string toString() const override;

private:
    LimitPushDownRule();

    graph::Limit *cloneLimit(graph::QueryContext *qctx,
                             const graph::Limit *limit) const;

    graph::Project *cloneProj(graph::QueryContext *qctx,
                              const graph::Project *proj) const;

    graph::GetNeighbors *cloneGetNbrs(graph::QueryContext *qctx,
                                      const graph::GetNeighbors *getNbrs) const;

    std::pair<bool, std::vector<const OptGroupExpr *>>
    findMatchedGroupExpr(const OptGroupExpr *groupExpr) const;
};

}   // namespace opt
}   // namespace nebula

#endif   // OPTIMIZER_RULE_LIMITPUSHDOWN_H_
