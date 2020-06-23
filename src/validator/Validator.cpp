/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/Validator.h"

#include "parser/Sentence.h"
#include "planner/Query.h"
#include "validator/GoValidator.h"
#include "validator/PipeValidator.h"
#include "validator/ReportError.h"
#include "validator/SequentialValidator.h"
#include "validator/AssignmentValidator.h"
#include "validator/SetValidator.h"
#include "validator/UseValidator.h"
#include "validator/GetSubgraphValidator.h"
#include "validator/AdminValidator.h"
#include "validator/MaintainValidator.h"
#include "validator/MutateValidator.h"

namespace nebula {
namespace graph {
std::unique_ptr<Validator> Validator::makeValidator(Sentence* sentence, QueryContext* context) {
    CHECK_NOTNULL(sentence);
    CHECK_NOTNULL(context);
    auto kind = sentence->kind();
    switch (kind) {
        case Sentence::Kind::kSequential:
            return std::make_unique<SequentialValidator>(sentence, context);
        case Sentence::Kind::kGo:
            return std::make_unique<GoValidator>(sentence, context);
        case Sentence::Kind::kPipe:
            return std::make_unique<PipeValidator>(sentence, context);
        case Sentence::Kind::kAssignment:
            return std::make_unique<AssignmentValidator>(sentence, context);
        case Sentence::Kind::kSet:
            return std::make_unique<SetValidator>(sentence, context);
        case Sentence::Kind::kUse:
            return std::make_unique<UseValidator>(sentence, context);
        case Sentence::Kind::kGetSubgraph:
            return std::make_unique<GetSubgraphValidator>(sentence, context);
        case Sentence::Kind::kCreateSpace:
            return std::make_unique<CreateSpaceValidator>(sentence, context);
        case Sentence::Kind::kCreateTag:
            return std::make_unique<CreateTagValidator>(sentence, context);
        case Sentence::Kind::kCreateEdge:
            return std::make_unique<CreateEdgeValidator>(sentence, context);
        case Sentence::Kind::kDescribeSpace:
            return std::make_unique<DescSpaceValidator>(sentence, context);
        case Sentence::Kind::kDescribeTag:
            return std::make_unique<DescTagValidator>(sentence, context);
        case Sentence::Kind::kDescribeEdge:
            return std::make_unique<DescEdgeValidator>(sentence, context);
        case Sentence::Kind::kInsertVertices:
            return std::make_unique<InsertVerticesValidator>(sentence, context);
        case Sentence::Kind::kInsertEdges:
            return std::make_unique<InsertEdgesValidator>(sentence, context);
        default:
            return std::make_unique<ReportError>(sentence, context);
    }
}

Status Validator::appendPlan(PlanNode* node, PlanNode* appended) {
    switch (node->kind()) {
        case PlanNode::Kind::kFilter:
        case PlanNode::Kind::kProject:
        case PlanNode::Kind::kSort:
        case PlanNode::Kind::kLimit:
        case PlanNode::Kind::kAggregate:
        case PlanNode::Kind::kSelect:
        case PlanNode::Kind::kLoop:
        case PlanNode::Kind::kSwitchSpace:
        case PlanNode::Kind::kCreateSpace:
        case PlanNode::Kind::kCreateTag:
        case PlanNode::Kind::kCreateEdge:
        case PlanNode::Kind::kDescSpace:
        case PlanNode::Kind::kDescTag:
        case PlanNode::Kind::kDescEdge:
        case PlanNode::Kind::kInsertVertices:
        case PlanNode::Kind::kInsertEdges: {
            static_cast<SingleInputNode*>(node)->setInput(appended);
            break;
        }
        default: {
            return Status::Error("%s not support to append an input.",
                                 PlanNode::toString(node->kind()));
        }
    }
    return Status::OK();
}

Status Validator::validate() {
    Status status;

    if (!vctx_) {
        VLOG(1) << "Validate context was not given.";
        return Status::Error("Validate context was not given.");
    }

    if (!sentence_) {
        VLOG(1) << "Sentence was not given";
        return Status::Error("Sentence was not given");
    }

    if (!noSpaceRequired_ && !spaceChosen()) {
        VLOG(1) << "Space was not chosen.";
        status = Status::Error("Space was not chosen.");
        return status;
    }

    space_ = vctx_->whichSpace();

    status = validateImpl();
    if (!status.ok()) {
        return status;
    }

    status = toPlan();
    if (!status.ok()) {
        return status;
    }

    return Status::OK();
}

bool Validator::spaceChosen() {
    return vctx_->spaceChosen();
}

std::vector<std::string> Validator::deduceColNames(const YieldColumns* cols) const {
    std::vector<std::string> colNames;
    for (auto col : cols->columns()) {
        colNames.emplace_back(deduceColName(col));
    }

    return colNames;
}

std::string Validator::deduceColName(const YieldColumn* col) const {
    if (col->alias() != nullptr) {
        return *col->alias();
    } else {
        switch (col->expr()->kind()) {
            case Expression::Kind::kInputProperty: {
                auto expr = static_cast<InputPropertyExpression*>(col->expr());
                return *expr->sym();
            }
            default: {
                return col->expr()->toString();
            }
        }
    }
}

#define DETECT_BIEXPR_TYPE(OP)                                           \
    auto biExpr = static_cast<const BinaryExpression*>(expr);            \
    auto left = deduceExprType(biExpr->left());                          \
    if (!left.ok()) {                                                    \
        return left;                                                     \
    }                                                                    \
                                                                         \
    auto right = deduceExprType(biExpr->right());                        \
    if (!right.ok()) {                                                   \
        return right;                                                    \
    }                                                                    \
                                                                         \
    auto detectVal =                                                     \
        kValues.at(left.value()) OP kValues.at(right.value());           \
    if (isBadNull(detectVal)) {                                          \
        std::stringstream ss;                                            \
        ss << "`" << expr->toString() << "' is not a valid expression, " \
           << "can not apply `" << #OP << "' to `" << left.value()       \
           << "' and `" << right.value() << "'.";                        \
        return Status::Error(ss.str());                                  \
    }                                                                    \
    return detectVal.type();

#define DETECT_UNARYEXPR_TYPE(OP)                                           \
    auto unaryExpr = static_cast<const UnaryExpression*>(expr);             \
    auto status = deduceExprType(unaryExpr->operand());                     \
    if (!status.ok()) {                                                     \
        return status.status();                                             \
    }                                                                       \
                                                                            \
    auto detectVal = OP kValues.at(status.value());                         \
    if (isBadNull(detectVal)) {                                             \
        std::stringstream ss;                                               \
        ss << "`" << expr->toString() << "' is not a valid expression, "    \
           << "can not apply `" << #OP << "' to " << status.value() << "."; \
        return Status::Error(ss.str());                                     \
    }                                                                       \
    return detectVal.type();

bool Validator::isBadNull(const Value& val) const {
    if (!val.isNull()) {
        return false;
    }
    auto null = val.getNull();
    return null == NullType::NaN
        || null == NullType::BAD_DATA
        || null == NullType::BAD_TYPE
        || null == NullType::ERR_OVERFLOW
        || null == NullType::UNKNOWN_PROP
        || null == NullType::DIV_BY_ZERO;
}

StatusOr<Value::Type> Validator::deduceExprType(const Expression* expr) const {
    static const std::unordered_map<Value::Type, Value> kValues = {
        {Value::Type::__EMPTY__, Value()},
        {Value::Type::NULLVALUE, Value(NullType::__NULL__)},
        {Value::Type::BOOL, Value(true)},
        {Value::Type::INT, Value(1)},
        {Value::Type::FLOAT, Value(1.0)},
        {Value::Type::STRING, Value("a")},
        {Value::Type::DATE, Value(Date())},
        {Value::Type::DATETIME, Value(DateTime())},
        {Value::Type::VERTEX, Value(Vertex())},
        {Value::Type::EDGE, Value(Edge())},
        {Value::Type::PATH, Value(Path())},
        {Value::Type::LIST, Value(List())},
        {Value::Type::MAP, Value(Map())},
        {Value::Type::SET, Value(Set())},
        {Value::Type::DATASET, Value(DataSet())},
    };
    switch (expr->kind()) {
        case Expression::Kind::kAdd: {
            DETECT_BIEXPR_TYPE(+)
        }
        case Expression::Kind::kMinus: {
            DETECT_BIEXPR_TYPE(-)
        }
        case Expression::Kind::kMultiply: {
            DETECT_BIEXPR_TYPE(*)
        }
        case Expression::Kind::kDivision: {
            DETECT_BIEXPR_TYPE(/)
        }
        case Expression::Kind::kMod: {
            DETECT_BIEXPR_TYPE(%)
        }
        case Expression::Kind::kRelEQ:
        case Expression::Kind::kRelNE:
        case Expression::Kind::kRelLT:
        case Expression::Kind::kRelLE:
        case Expression::Kind::kRelGT:
        case Expression::Kind::kRelGE: {
            // TODO: Should deduce.
            return Value::Type::BOOL;
        }
        case Expression::Kind::kLogicalAnd: {
            DETECT_BIEXPR_TYPE(&&)
        }
        case Expression::Kind::kLogicalXor:
        case Expression::Kind::kLogicalOr: {
            DETECT_BIEXPR_TYPE(||)
        }
        case Expression::Kind::kRelIn: {
            auto biExpr = static_cast<const BinaryExpression*>(expr);
            auto left = deduceExprType(biExpr->left());
            if (!left.ok()) {
                return left;
            }

            auto right = deduceExprType(biExpr->right());
            if (!right.ok()) {
                return right;
            }

            if (right.value() != Value::Type::LIST) {
                std::stringstream ss;
                ss << "`" << expr->toString() << "' is not a valid expression, "
                    << "expected `LIST' but `" << right.value() << "' was given.";
                return Status::Error(ss.str());
            }
            return Value::Type::BOOL;
        }
        case Expression::Kind::kUnaryPlus: {
            auto unaryExpr = static_cast<const UnaryExpression*>(expr);
            auto status = deduceExprType(unaryExpr->operand());
            if (!status.ok()) {
                return status.status();
            }

            return status.value();
        }
        case Expression::Kind::kUnaryNegate: {
            DETECT_UNARYEXPR_TYPE(-)
        }
        case Expression::Kind::kUnaryNot: {
            DETECT_UNARYEXPR_TYPE(!)
        }
        case Expression::Kind::kUnaryIncr: {
            auto unaryExpr = static_cast<const UnaryExpression*>(expr);
            auto status = deduceExprType(unaryExpr->operand());
            if (!status.ok()) {
                return status.status();
            }

            auto detectVal = kValues.at(status.value()) + 1;
            if (isBadNull(detectVal)) {
                std::stringstream ss;
                ss << "`" << expr->toString() << "' is not a valid expression, "
                    << "can not apply `++' to " << status.value() << ".";
                return Status::Error(ss.str());
            }
            return detectVal.type();
        }
        case Expression::Kind::kUnaryDecr: {
            auto unaryExpr = static_cast<const UnaryExpression*>(expr);
            auto status = deduceExprType(unaryExpr->operand());
            if (!status.ok()) {
                return status.status();
            }

            auto detectVal = kValues.at(status.value()) - 1;
            if (isBadNull(detectVal)) {
                std::stringstream ss;
                ss << "`" << expr->toString() << "' is not a valid expression, "
                    << "can not apply `--' to " << status.value() << ".";
                return Status::Error(ss.str());
            }
            return detectVal.type();
        }
        case Expression::Kind::kFunctionCall: {
            // TODO
            return Status::Error("Not support function yet.");
        }
        case Expression::Kind::kTypeCasting: {
            auto castExpr = static_cast<const TypeCastingExpression*>(expr);
            auto status = deduceExprType(castExpr->operand());
            if (!status.ok()) {
                return status.status();
            }
            // TODO
            return Status::Error("Not support type casting yet.");
        }
        case Expression::Kind::kDstProperty:
        case Expression::Kind::kSrcProperty: {
            /*
            auto* tagPropExpr = static_cast<const SymbolPropertyExpression*>(expr);
            auto* tag = tagPropExpr->sym();
            auto tagId = qctx_->schemaMng()->toTagID(space_.id, *tag);
            if (!tagId.ok()) {
                return tagId.status();
            }
            auto schema = qctx_->schemaMng()->getTagSchema(space_.id, tagId.value());
            if (!schema) {
                return Status::Error("`%s', not found tag `%s'.",
                        expr->toString().c_str(), tag->c_str());
            }
            auto* prop = tagPropExpr->prop();
            auto* field = schema->field(*prop);
            if (field == nullptr) {
                return Status::Error("`%s', not found the property `%s'.",
                        expr->toString().c_str(), prop->c_str());
            }
            return field->type();
            */
            return Status::Error("TODO");
        }
        case Expression::Kind::kEdgeProperty: {
            /*
            auto* edgePropExpr = static_cast<const SymbolPropertyExpression*>(expr);
            auto* edge = edgePropExpr->sym();
            auto edgeType = qctx_->schemaMng()->toEdgeType(space_.id, *edge);
            if (!edgeType.ok()) {
                return edgeType.status();
            }
            auto schema = qctx_->schemaMng()->getEdgeSchema(space_.id, edgeType.value());
            if (!schema) {
                return Status::Error("`%s', not found edge `%s'.",
                        expr->toString().c_str(), edge->c_str());
            }
            auto* prop = edgePropExpr->prop();
            auto* field = schema->field(*prop);
            if (field == nullptr) {
                return Status::Error("`%s', not found the property `%s'.",
                        expr->toString().c_str(), prop->c_str());
            }
            return field->type();
            */
            return Status::Error("TODO");
        }
        case Expression::Kind::kVarProperty: {
            auto* varPropExpr = static_cast<const SymbolPropertyExpression*>(expr);
            auto* var = varPropExpr->sym();
            if (!vctx_->existVar(*var)) {
                return Status::Error("`%s', not exist variable `%s'",
                        expr->toString().c_str(), var->c_str());
            }
            auto* prop = varPropExpr->prop();
            auto cols = vctx_->getVar(*var);
            auto found = std::find_if(cols.begin(), cols.end(), [&prop] (auto& col) {
                return *prop == col.first;
            });
            if (found == cols.end()) {
                return Status::Error("`%s', not exist prop `%s'",
                        expr->toString().c_str(), prop->c_str());
            }
            return found->second;
        }
        case Expression::Kind::kInputProperty: {
            auto* inputPropExpr = static_cast<const SymbolPropertyExpression*>(expr);
            auto* prop = inputPropExpr->prop();
            auto found = std::find_if(inputs_.begin(), inputs_.end(), [&prop] (auto& col) {
                return *prop == col.first;
            });
            if (found == inputs_.end()) {
                return Status::Error("`%s', not exist prop `%s'",
                        expr->toString().c_str(), prop->c_str());
            }
            return found->second;
        }
        case Expression::Kind::kSymProperty: {
            return Status::Error("SymbolPropertyExpression can not be instantiated.");
        }

        case Expression::Kind::kConstant: {
            ExpressionContextImpl ctx(nullptr, nullptr);
            auto* mutableExpr = const_cast<Expression*>(expr);
            return mutableExpr->eval(ctx).type();
        }
        case Expression::Kind::kEdgeSrc: {
            return Value::Type::STRING;
        }
        case Expression::Kind::kEdgeType: {
            return Value::Type::INT;
        }
        case Expression::Kind::kEdgeRank: {
            return Value::Type::INT;
        }
        case Expression::Kind::kEdgeDst: {
            return Value::Type::STRING;
        }
        case Expression::Kind::kUUID: {
            return Value::Type::STRING;
        }
        case Expression::Kind::kVar: {
            // TODO: not only dataset
            return Value::Type::DATASET;
        }
        case Expression::Kind::kVersionedVar: {
            // TODO: not only dataset
            return Value::Type::DATASET;
        }
    }
    return Status::Error("Unkown expression kind: %ld", static_cast<int64_t>(expr->kind()));
}
}  // namespace graph
}  // namespace nebula
