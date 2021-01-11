/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef OPTIMIZER_RULE_PROJECTCOLUMNREWRITETOINDEXCOLEXPRRULE_H_
#define OPTIMIZER_RULE_PROJECTCOLUMNREWRITETOINDEXCOLEXPRRULE_H_

#include "optimizer/OptRule.h"

namespace nebula {
namespace opt {
class ProjectColumnRewriteToIndexColExprRule final : public OptRule {
public:
    const Pattern &pattern() const override;

    bool match(const MatchedResult &matched) const override;

    StatusOr<OptRule::TransformResult> transform(graph::QueryContext *qctx,
                                                 const MatchedResult &matched) const override;

    std::string toString() const override;

private:
    ProjectColumnRewriteToIndexColExprRule();

    static std::unique_ptr<OptRule> kInstance;
};
}  // namespace opt
}  // namespace nebula

#endif  // OPTIMIZER_RULE_PROJECTCOLUMNREWRITETOINDEXCOLEXPRRULE_H_
