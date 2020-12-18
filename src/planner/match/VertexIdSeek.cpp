/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/VertexIdSeek.h"

#include "planner/Logic.h"
#include "planner/match/MatchSolver.h"
#include "util/ExpressionUtils.h"
#include "util/SchemaUtil.h"

namespace nebula {
namespace graph {
bool VertexIdSeek::matchEdge(EdgeContext* edgeCtx) {
    UNUSED(edgeCtx);
    return false;
}

StatusOr<SubPlan> VertexIdSeek::transformEdge(EdgeContext* edgeCtx) {
    UNUSED(edgeCtx);
    return Status::Error("Unimplement for edge pattern.");
}

bool VertexIdSeek::matchNode(NodeContext* nodeCtx) {
    auto& node = *nodeCtx->info;
    auto* matchClauseCtx = nodeCtx->matchClauseCtx;
    if (matchClauseCtx->where == nullptr
            || matchClauseCtx->where->filter == nullptr) {
        return false;
    }

    if (node.label != nullptr) {
        // TODO: Only support all tags for now.
        return false;
    }

    auto vidResult = extractVids(matchClauseCtx->where->filter.get());
    if (!vidResult.ok()) {
        return false;
    }

    nodeCtx->ids = std::move(vidResult).value();
    return true;
}

StatusOr<List> VertexIdSeek::extractVids(const Expression* filter) {
    auto result = reverseEvalVids(filter);
    if (result.in == VidPattern::IN::kIn) {
        return std::move(result.vids);
    }
    return Status::SemanticError("Can't extract valid vertices id from filter.");
}

std::pair<std::string, Expression*> VertexIdSeek::listToAnnoVarVid(QueryContext* qctx,
                                                                    const List& list) {
    auto input = qctx->vctx()->anonVarGen()->getVar();
    DataSet vids({kVid});
    QueryExpressionContext dummy;
    for (auto& v : list.values) {
        vids.emplace_back(Row({std::move(v)}));
    }

    qctx->ectx()->setResult(input, ResultBuilder().value(Value(std::move(vids))).finish());

    auto* src = new VariablePropertyExpression(new std::string(input), new std::string(kVid));
    return std::pair<std::string, Expression*>(input, src);
}

std::pair<std::string, Expression*> VertexIdSeek::constToAnnoVarVid(QueryContext* qctx,
                                                                     const Value& v) {
    auto input = qctx->vctx()->anonVarGen()->getVar();
    DataSet vids({kVid});
    QueryExpressionContext dummy;
    vids.emplace_back(Row({v}));

    qctx->ectx()->setResult(input, ResultBuilder().value(Value(std::move(vids))).finish());

    auto* src = new VariablePropertyExpression(new std::string(input), new std::string(kVid));
    return std::pair<std::string, Expression*>(input, src);
}

StatusOr<SubPlan> VertexIdSeek::transformNode(NodeContext* nodeCtx) {
    SubPlan plan;
    auto* matchClauseCtx = nodeCtx->matchClauseCtx;
    auto* qctx = matchClauseCtx->qctx;

    QueryExpressionContext dummy;
    std::pair<std::string, Expression*> vidsResult = listToAnnoVarVid(qctx, nodeCtx->ids);

    auto* passThrough = PassThroughNode::make(qctx, nullptr);
    passThrough->setColNames({kVid});
    passThrough->setOutputVar(vidsResult.first);
    plan.root = passThrough;
    plan.tail = passThrough;

    nodeCtx->initialExpr = vidsResult.second;
    VLOG(1) << "root: " << plan.root->kind() << " tail: " << plan.tail->kind();
    return plan;
}

// Reverse Eval the id(V) IN [List] or id(V) == vid that suppose result is TRUE
// Keep the constraint :
//    Put any vid in result VidPattern.vids to id(V) make the expression evaluate to TRUE!(Ignore
//    not related expression component e.g. attribute expr, so the filter still need eval
//    when filter final result) and non any other vid make it eval to TRUE!
// TODO(shylock) support different id() call (e.g. id(a) == '...' & id(b) in [..])
/*static*/ VertexIdSeek::VidPattern VertexIdSeek::reverseEvalVids(const Expression *expr) {
    switch (expr->kind()) {
        case Expression::Kind::kRelIn: {
            const auto* inExpr = static_cast<const RelationalExpression*>(expr);
            if (inExpr->left()->kind() != Expression::Kind::kFunctionCall ||
                inExpr->right()->kind() != Expression::Kind::kConstant) {
                    return VidPattern{};
            }
            const auto* fCallExpr = static_cast<const FunctionCallExpression*>(inExpr->left());
            if (*fCallExpr->name() != "id") {
                return VidPattern{};
            }
            const auto* constExpr = static_cast<const ConstantExpression*>(inExpr->right());
            if (constExpr->value().type() != Value::Type::LIST) {
                return VidPattern{};
            }
            return VidPattern{VidPattern::IN::kIn, constExpr->value().getList()};
        }
        case Expression::Kind::kRelEQ: {
            const auto* eqExpr = static_cast<const RelationalExpression*>(expr);
            if (eqExpr->left()->kind() != Expression::Kind::kFunctionCall ||
                eqExpr->right()->kind() != Expression::Kind::kConstant) {
                return VidPattern{};
            }
            const auto* fCallExpr = static_cast<const FunctionCallExpression*>(eqExpr->left());
            if (*fCallExpr->name() != "id") {
                return VidPattern{};
            }
            const auto* constExpr = static_cast<const ConstantExpression*>(eqExpr->right());
            if (!SchemaUtil::isValidVid(constExpr->value())) {
                return VidPattern{};
            }
            return VidPattern{VidPattern::IN::kIn, List({constExpr->value()})};
        }
        case Expression::Kind::kLogicalAnd: {
            const auto *andExpr = static_cast<const LogicalExpression*>(expr);
            std::vector<VidPattern> operandsResult;
            operandsResult.reserve(andExpr->operands().size());
            for (const auto &operand : andExpr->operands()) {
                operandsResult.emplace_back(reverseEvalVids(operand.get()));
            }
            VidPattern inResult{VidPattern::IN::kIn, {}};
            std::unordered_set<Value> notInResult;
            for (auto &operandResult : operandsResult) {
                if (operandResult.in == VidPattern::IN::kNotIn) {
                    notInResult.insert(std::make_move_iterator(operandResult.vids.values.begin()),
                                       std::make_move_iterator(operandResult.vids.values.end()));
                }
            }
            // intersect all in list
            std::vector<VidPattern> inOperandsResult;
            for (auto &result : operandsResult) {
                if (result.in == VidPattern::IN::kIn) {
                    inOperandsResult.emplace_back(std::move(result));
                }
            }
            if (inOperandsResult.size() < 1) {
                // noting
            } else if (inOperandsResult.size() < 2) {
                inResult = inOperandsResult[0];
            } else {
                inResult = intersect(inOperandsResult[0], inOperandsResult[1]);
                for (std::size_t i = 2; i < inOperandsResult.size(); ++i) {
                    inResult = intersect(inResult, inOperandsResult[i]);
                }
            }
            // remove that not in item
            List removeNotIn;
            for (auto &v : inResult.vids.values) {
                if (notInResult.find(v) == notInResult.end()) {
                    removeNotIn.emplace_back(std::move(v));
                }
            }
            inResult.vids = std::move(removeNotIn);
            return inResult;
        }
        case Expression::Kind::kLogicalOr: {
            const auto *andExpr = static_cast<const LogicalExpression*>(expr);
            std::vector<VidPattern> operandsResult;
            operandsResult.reserve(andExpr->operands().size());
            for (const auto &operand : andExpr->operands()) {
                operandsResult.emplace_back(reverseEvalVids(operand.get()));
            }
            VidPattern inResult{VidPattern::IN::kIn, {}};
            for (auto &result : operandsResult) {
                if (result.in == VidPattern::IN::kIn) {
                    inResult.vids.values.insert(inResult.vids.values.end(),
                                                std::make_move_iterator(result.vids.values.begin()),
                                                std::make_move_iterator(result.vids.values.end()));
                }
            }
            return inResult;
        }
        case Expression::Kind::kLogicalXor:
            // TODO
            return VidPattern{};
        case Expression::Kind::kUnaryNot: {
            const auto *notExpr = static_cast<const UnaryExpression*>(expr);
            auto operandResult = reverseEvalVids(notExpr->operand());
            switch (operandResult.in) {
                case VidPattern::IN::kIgnore:
                    break;
                case VidPattern::IN::kIn:
                    operandResult.in = VidPattern::IN::kNotIn;
                    break;
                case VidPattern::IN::kNotIn:
                    operandResult.in = VidPattern::IN::kIn;
                    break;
            }
            return operandResult;
        }
        default:
            return VidPattern{};
    }
}

/*static*/ VertexIdSeek::VidPattern VertexIdSeek::intersect(VidPattern &left, VidPattern &right) {
    DCHECK(left.in == VidPattern::IN::kIn);
    DCHECK(right.in == VidPattern::IN::kIn);
    VidPattern v{VidPattern::IN::kIn, {}};
    std::sort(left.vids.values.begin(), left.vids.values.end());
    std::sort(right.vids.values.begin(), right.vids.values.end());
    std::set_intersection(left.vids.values.begin(), left.vids.values.end(),
                          right.vids.values.begin(), right.vids.values.end(),
                          std::back_inserter(v.vids.values));
    return v;
}

}  // namespace graph
}  // namespace nebula
