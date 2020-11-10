/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/UnwindExecutor.h"

#include "context/QueryExpressionContext.h"
#include "parser/Clauses.h"
#include "planner/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> UnwindExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *unwind = asNode<Unwind>(node());
    auto *expr = unwind->expr();

    auto iter = ectx_->getResult(unwind->inputVar()).iter();
    DCHECK(!!iter);
    QueryExpressionContext ctx(ectx_);

    DataSet ds;
    ds.colNames = unwind->colNames();
    if (expr->kind() == Expression::Kind::kLabel) {   // UNWIND var AS r
        const auto *labelExpr = static_cast<const LabelExpression *>(expr);
        auto *vp = rewrite(labelExpr);
        for (; iter->valid(); iter->next()) {
            Value val = vp->eval(ctx(iter.get()));
            std::vector<Value> &vals = extractList(val);
            for (const auto &v : vals) {
                ds.rows.emplace_back(Row({std::move(v)}));
            }
        }
    } else {   // UNWIND [1, 2, 3] AS r
        Value val = expr->eval(ctx);
        for (const auto &v : vals) {
            ds.rows.emplace_back(Row({std::move(v)}));
        }
    }

    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}

Expression *UnwindExecutor::rewrite(const LabelExpression *label) {
    auto *expr = new VariablePropertyExpression(new std::string(), new std::string(*label->name()));
    return expr;
}

std::vector<Value> UnwindExecutor::extractList(Value &val) {
    std::vector<Value> ret;
    if (val.isList()) {
        auto &list = val.getList();
        for (size_t i = 0; i < list.size(); ++i) {
            ret.emplace_back(std::move(list[i]));
        }
    } else {
        if (val != Value::kNullValue) {
            ret.emplace_back(std::move(val));
        }
    }
}

}   // namespace graph
}   // namespace nebula
