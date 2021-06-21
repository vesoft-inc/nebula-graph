/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_NGQL_LOOKUPPLANNER_H_
#define PLANNER_NGQL_LOOKUPPLANNER_H_

#include <memory>

#include "planner/Planner.h"

namespace nebula {

class YieldColumns;

namespace graph {

struct LookupContext;
struct AstContext;

class LookupPlanner final : public Planner {
public:
    static std::unique_ptr<Planner> make();
    static bool match(AstContext* astCtx);

    StatusOr<SubPlan> transform(AstContext* astCtx) override;

private:
    YieldColumns* prepareReturnCols(LookupContext* lookupCtx) const;
    void appendColumns(LookupContext* lookupCtx, YieldColumns* columns) const;
};

}   // namespace graph
}   // namespace nebula

#endif   // PLANNER_NGQL_LOOKUPPLANNER_H_
