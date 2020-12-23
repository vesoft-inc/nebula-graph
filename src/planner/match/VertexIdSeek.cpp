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
bool VertexIdSeek::matchEdge(EdgeContext *edgeCtx) {
    UNUSED(edgeCtx);
    return false;
}

StatusOr<SubPlan> VertexIdSeek::transformEdge(EdgeContext *edgeCtx) {
    UNUSED(edgeCtx);
    return Status::Error("Unimplemented for edge pattern.");
}

bool VertexIdSeek::matchNode(NodeContext *nodeCtx) {
    auto &node = *nodeCtx->info;
    auto *matchClauseCtx = nodeCtx->matchClauseCtx;
    if (matchClauseCtx->where == nullptr || matchClauseCtx->where->filter == nullptr) {
        return false;
    }

    if (node.label != nullptr) {
        // TODO: Only support all tags for now.
        return false;
    }

    if (node.alias == nullptr) {
        // require one named node
        return false;
    }

    auto vidResult = reverseEvalVids(matchClauseCtx->where->filter.get());
    if (vidResult.spec != VidPattern::Special::kInUsed) {
        return false;
    }
    for (auto &nodeVid : vidResult.nodes) {
        if (nodeVid.second.kind == VidPattern::Vids::Kind::kIn) {
            if (nodeVid.first == *node.alias) {
                nodeCtx->ids = std::move(nodeVid.second.vids);
                return true;
            }
        }
    }

    return false;
}

std::pair<std::string, Expression *> VertexIdSeek::listToAnnoVarVid(QueryContext *qctx,
                                                                    const List &list) {
    auto input = qctx->vctx()->anonVarGen()->getVar();
    DataSet vids({kVid});
    QueryExpressionContext dummy;
    for (auto &v : list.values) {
        vids.emplace_back(Row({std::move(v)}));
    }

    qctx->ectx()->setResult(input, ResultBuilder().value(Value(std::move(vids))).finish());

    auto *src = new VariablePropertyExpression(new std::string(input), new std::string(kVid));
    return std::pair<std::string, Expression *>(input, src);
}

std::pair<std::string, Expression *> VertexIdSeek::constToAnnoVarVid(QueryContext *qctx,
                                                                     const Value &v) {
    auto input = qctx->vctx()->anonVarGen()->getVar();
    DataSet vids({kVid});
    QueryExpressionContext dummy;
    vids.emplace_back(Row({v}));

    qctx->ectx()->setResult(input, ResultBuilder().value(Value(std::move(vids))).finish());

    auto *src = new VariablePropertyExpression(new std::string(input), new std::string(kVid));
    return std::pair<std::string, Expression *>(input, src);
}

StatusOr<SubPlan> VertexIdSeek::transformNode(NodeContext *nodeCtx) {
    SubPlan plan;
    auto *matchClauseCtx = nodeCtx->matchClauseCtx;
    auto *qctx = matchClauseCtx->qctx;

    QueryExpressionContext dummy;
    std::pair<std::string, Expression *> vidsResult = listToAnnoVarVid(qctx, nodeCtx->ids);

    auto *passThrough = PassThroughNode::make(qctx, nullptr);
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
/*static*/ VertexIdSeek::VidPattern VertexIdSeek::reverseEvalVids(const Expression *expr) {
    switch (expr->kind()) {
        case Expression::Kind::kRelIn: {
            const auto *inExpr = static_cast<const RelationalExpression *>(expr);
            if (inExpr->left()->kind() == Expression::Kind::kLabelAttribute) {
                const auto *labelExpr =
                    static_cast<const LabelAttributeExpression *>(inExpr->left());
                const auto &label = labelExpr->left()->toString();
                return VidPattern{VidPattern::Special::kInUsed,
                                  {{label, {VidPattern::Vids::Kind::kOtherSource, {}}}}};
            }
            if (inExpr->left()->kind() != Expression::Kind::kFunctionCall ||
                inExpr->right()->kind() != Expression::Kind::kConstant) {
                return VidPattern{};
            }
            const auto *fCallExpr = static_cast<const FunctionCallExpression *>(inExpr->left());
            if (*fCallExpr->name() != "id" && fCallExpr->args()->numArgs() != 1 &&
                fCallExpr->args()->args().front()->kind() != Expression::Kind::kLabel) {
                return VidPattern{};
            }
            const auto *constExpr = static_cast<const ConstantExpression *>(inExpr->right());
            if (constExpr->value().type() != Value::Type::LIST) {
                return VidPattern{};
            }
            return VidPattern{VidPattern::Special::kInUsed,
                              {{fCallExpr->args()->args().front()->toString(),
                                {VidPattern::Vids::Kind::kIn, constExpr->value().getList()}}}};
        }
        case Expression::Kind::kRelEQ: {
            const auto *eqExpr = static_cast<const RelationalExpression *>(expr);
            if (eqExpr->left()->kind() == Expression::Kind::kLabelAttribute) {
                const auto *labelExpr =
                    static_cast<const LabelAttributeExpression *>(eqExpr->left());
                const auto &label = labelExpr->left()->toString();
                return VidPattern{VidPattern::Special::kInUsed,
                                  {{label, {VidPattern::Vids::Kind::kOtherSource, {}}}}};
            }
            if (eqExpr->left()->kind() != Expression::Kind::kFunctionCall ||
                eqExpr->right()->kind() != Expression::Kind::kConstant) {
                return VidPattern{};
            }
            const auto *fCallExpr = static_cast<const FunctionCallExpression *>(eqExpr->left());
            if (*fCallExpr->name() != "id" && fCallExpr->args()->numArgs() != 1 &&
                fCallExpr->args()->args().front()->kind() != Expression::Kind::kLabel) {
                return VidPattern{};
            }
            const auto *constExpr = static_cast<const ConstantExpression *>(eqExpr->right());
            if (!SchemaUtil::isValidVid(constExpr->value())) {
                return VidPattern{};
            }
            return VidPattern{VidPattern::Special::kInUsed,
                              {{fCallExpr->args()->args().front()->toString(),
                                {VidPattern::Vids::Kind::kIn, List({constExpr->value()})}}}};
        }
        case Expression::Kind::kLogicalAnd: {
            const auto *andExpr = static_cast<const LogicalExpression *>(expr);
            std::vector<VidPattern> operandsResult;
            operandsResult.reserve(andExpr->operands().size());
            for (const auto &operand : andExpr->operands()) {
                operandsResult.emplace_back(reverseEvalVids(operand.get()));
            }
            VidPattern inResult{VidPattern::Special::kInUsed, {}};
            VidPattern notInResult{VidPattern::Special::kInUsed, {}};
            for (auto &operandResult : operandsResult) {
                if (operandResult.spec == VidPattern::Special::kInUsed) {
                    for (auto &node : operandResult.nodes) {
                        if (node.second.kind == VidPattern::Vids::Kind::kNotIn) {
                            notInResult.nodes[node.first].vids.values.insert(
                                notInResult.nodes[node.first].vids.values.end(),
                                std::make_move_iterator(node.second.vids.values.begin()),
                                std::make_move_iterator(node.second.vids.values.end()));
                        }
                    }
                }
            }
            // intersect all in list
            std::vector<std::pair<std::string, VidPattern::Vids>> inOperandsResult;
            for (auto &operandResult : operandsResult) {
                if (operandResult.spec == VidPattern::Special::kInUsed) {
                    for (auto &node : operandResult.nodes) {
                        if (node.second.kind == VidPattern::Vids::Kind::kIn) {
                            inOperandsResult.emplace_back(std::move(node));
                        }
                    }
                }
            }
            if (inOperandsResult.size() < 1) {
                // noting
            } else if (inOperandsResult.size() < 2) {
                inResult.nodes.emplace(std::move(inOperandsResult.front()));
            } else {
                inResult =
                    intersect(std::move(inOperandsResult[0]), std::move(inOperandsResult[1]));
                for (std::size_t i = 2; i < inOperandsResult.size(); ++i) {
                    inResult = intersect(std::move(inResult), std::move(inOperandsResult[i]));
                }
            }
            // remove that not in item
            for (auto &node : inResult.nodes) {
                auto find = notInResult.nodes.find(node.first);
                if (find != notInResult.nodes.end()) {
                    List removeNotIn;
                    for (auto &v : node.second.vids.values) {
                        if (std::find(find->second.vids.values.begin(),
                                      find->second.vids.values.end(),
                                      v) == find->second.vids.values.end()) {
                            removeNotIn.emplace_back(std::move(v));
                        }
                    }
                    node.second.vids = std::move(removeNotIn);
                }
            }
            return inResult;
        }
        case Expression::Kind::kLogicalOr: {
            const auto *andExpr = static_cast<const LogicalExpression *>(expr);
            std::vector<VidPattern> operandsResult;
            operandsResult.reserve(andExpr->operands().size());
            for (const auto &operand : andExpr->operands()) {
                operandsResult.emplace_back(reverseEvalVids(operand.get()));
            }
            VidPattern inResult{VidPattern::Special::kInUsed, {}};
            for (auto &result : operandsResult) {
                if (result.spec == VidPattern::Special::kInUsed) {
                    for (auto &node : result.nodes) {
                        // Can't deduce with outher source (e.g. PropertiesIndex)
                        switch (node.second.kind) {
                            case VidPattern::Vids::Kind::kOtherSource:
                                return VidPattern{};
                            case VidPattern::Vids::Kind::kIn: {
                                inResult.nodes[node.first].kind = VidPattern::Vids::Kind::kIn;
                                inResult.nodes[node.first].vids.values.insert(
                                    inResult.nodes[node.first].vids.values.end(),
                                    std::make_move_iterator(node.second.vids.values.begin()),
                                    std::make_move_iterator(node.second.vids.values.end()));
                            }
                            case VidPattern::Vids::Kind::kNotIn:
                                // nothing
                                break;
                        }
                    }
                }
            }
            return inResult;
        }
        case Expression::Kind::kLogicalXor:
            // TODO
            return VidPattern{};
        case Expression::Kind::kUnaryNot: {
            const auto *notExpr = static_cast<const UnaryExpression *>(expr);
            auto operandResult = reverseEvalVids(notExpr->operand());
            if (operandResult.spec == VidPattern::Special::kInUsed) {
                for (auto &node : operandResult.nodes) {
                    switch (node.second.kind) {
                        case VidPattern::Vids::Kind::kOtherSource:
                            break;
                        case VidPattern::Vids::Kind::kIn:
                            node.second.kind = VidPattern::Vids::Kind::kNotIn;
                            break;
                        case VidPattern::Vids::Kind::kNotIn:
                            node.second.kind = VidPattern::Vids::Kind::kIn;
                            break;
                    }
                }
            }
            return operandResult;
        }
        case Expression::Kind::kLabelAttribute: {
            const auto *labelExpr = static_cast<const LabelAttributeExpression *>(expr);
            return VidPattern{
                VidPattern::Special::kInUsed,
                {{labelExpr->left()->toString(), {VidPattern::Vids::Kind::kOtherSource, {}}}}};
        }
        default:
            return VidPattern{};
    }
}

/*static*/ VertexIdSeek::VidPattern VertexIdSeek::intersect(VidPattern &&left, VidPattern &&right) {
    DCHECK(left.spec == VidPattern::Special::kInUsed);
    DCHECK(right.spec == VidPattern::Special::kInUsed);
    VidPattern v{VidPattern::Special::kInUsed, {std::move(left.nodes)}};
    for (auto &node : right.nodes) {
        DCHECK(node.second.kind == VidPattern::Vids::Kind::kIn);
        auto find = v.nodes.find(node.first);
        if (find == v.nodes.end()) {
            v.nodes.emplace(std::move(*find));
        } else {
            std::sort(find->second.vids.values.begin(), find->second.vids.values.end());
            std::sort(node.second.vids.values.begin(), node.second.vids.values.end());
            std::vector<Value> intersection;
            std::set_intersection(find->second.vids.values.begin(),
                                  find->second.vids.values.end(),
                                  node.second.vids.values.begin(),
                                  node.second.vids.values.end(),
                                  std::back_inserter(intersection));
            find->second.vids.values = std::move(intersection);
        }
    }
    return v;
}

/*static*/ VertexIdSeek::VidPattern VertexIdSeek::intersect(
    VidPattern &&left,
    std::pair<std::string, VidPattern::Vids> &&right) {
    auto find = left.nodes.find(right.first);
    if (find == left.nodes.end()) {
        left.nodes.emplace(std::move(right));
    } else {
        std::sort(find->second.vids.values.begin(), find->second.vids.values.end());
        std::sort(right.second.vids.values.begin(), right.second.vids.values.end());
        std::vector<Value> values;
        std::set_intersection(find->second.vids.values.begin(),
                              find->second.vids.values.end(),
                              right.second.vids.values.begin(),
                              right.second.vids.values.end(),
                              std::back_inserter(values));
        find->second.vids.values = std::move(values);
    }
    return std::move(left);
}

/*static*/ VertexIdSeek::VidPattern VertexIdSeek::intersect(
    std::pair<std::string, VidPattern::Vids> &&left,
    std::pair<std::string, VidPattern::Vids> &&right) {
    VidPattern v{VidPattern::Special::kInUsed, {}};
    if (left.first != right.first) {
        v.nodes.emplace(std::move(left));
        v.nodes.emplace(std::move(right));
    } else {
        std::sort(left.second.vids.values.begin(), left.second.vids.values.end());
        std::sort(right.second.vids.values.begin(), right.second.vids.values.end());
        std::vector<Value> values;
        std::set_intersection(left.second.vids.values.begin(),
                              left.second.vids.values.end(),
                              right.second.vids.values.begin(),
                              right.second.vids.values.end(),
                              std::back_inserter(values));
        v.nodes[left.first].kind = VidPattern::Vids::Kind::kIn;
        v.nodes[left.first].vids.values.insert(v.nodes[left.first].vids.values.end(),
                                               std::make_move_iterator(values.begin()),
                                               std::make_move_iterator(values.end()));
    }
    return v;
}

}   // namespace graph
}   // namespace nebula
