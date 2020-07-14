/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/GoValidator.h"

#include "common/base/Base.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/expression/VariableExpression.h"

#include "parser/TraverseSentences.h"

namespace nebula {
namespace graph {
Status GoValidator::validateImpl() {
    Status status;
    auto* goSentence = static_cast<GoSentence*>(sentence_);
    do {
        status = validateStep(goSentence->stepClause());
        if (!status.ok()) {
            break;
        }

        status = validateFrom(goSentence->fromClause());
        if (!status.ok()) {
            break;
        }

        status = validateOver(goSentence->overClause());
        if (!status.ok()) {
            break;
        }

        status = validateWhere(goSentence->whereClause());
        if (!status.ok()) {
            break;
        }

        status = validateYield(goSentence->yieldClause());
        if (!status.ok()) {
            break;
        }

        if (!inputProps_.empty() && fromType_ != kPipe) {
            status = Status::Error("$- must be referred in FROM "
                                   "before used in WHERE or YIELD");
            break;
        }

        if (!varProps_.empty() && fromType_ != kVariable) {
            status = Status::Error("A variable must be referred in FROM "
                                   "before used in WHERE or YIELD");
            break;
        }

        if ((!inputProps_.empty() && !varProps_.empty())
                || varProps_.size() > 1) {
            status = Status::Error(
                    "Only support single input in a go sentence.");
            break;
        }

        if (!dstTagProps_.empty()) {
            // TODO: inplement get vertex props.
            status = Status::Error("Not support get dst yet.");
            break;
        }

        if (isOverAll_) {
            // TODO: implement over all.
            status = Status::Error("Not support over all yet.");
            break;
        }

        if (!inputProps_.empty()) {
            // TODO: inplement get input props.
            status = Status::Error("Not support input prop yet.");
            break;
        }
    } while (false);

    return status;
}

Status GoValidator::validateStep(const StepClause* step) {
    if (step == nullptr) {
        return Status::Error("Step clause nullptr.");
    }
    auto steps = step->steps();
    if (steps <= 0) {
        return Status::Error("Only accpet positive number steps.");
    }
    steps_ = steps;
    return Status::OK();
}

Status GoValidator::validateFrom(const FromClause* from) {
    if (from == nullptr) {
        return Status::Error("From clause nullptr.");
    }
    if (from->isRef()) {
        auto* src = from->ref();
        if (src->kind() != Expression::Kind::kInputProperty
                && src->kind() != Expression::Kind::kVarProperty) {
            return Status::Error(
                    "`%s', Only input and variable expression is acceptable"
                    " when starts are evaluated at runtime.", src->toString().c_str());
        } else {
            fromType_ = src->kind() == Expression::Kind::kInputProperty ? kPipe : kVariable;
            auto type = deduceExprType(src);
            if (!type.ok()) {
                return type.status();
            }
            if (type.value() != Value::Type::STRING) {
                std::stringstream ss;
                ss << "`" << src->toString() << "', the srcs should be type of string, "
                   << "but was`" << type.value() << "'";
                return Status::Error(ss.str());
            }
            src_ = src;
        }
    } else {
        auto vidList = from->vidList();
        QueryExpressionContext ctx(qctx_->ectx(), nullptr);
        for (auto* expr : vidList) {
            if (!evaluableExpr(expr)) {
                return Status::Error("`%s' is not an evaluable expression.",
                        expr->toString().c_str());
            }
            auto vid = expr->eval(ctx);
            if (!vid.isStr()) {
                return Status::Error("Vid should be a string.");
            }
            starts_.emplace_back(std::move(vid));
        }
    }
    return Status::OK();
}

Status GoValidator::validateOver(const OverClause* over) {
    if (over == nullptr) {
        return Status::Error("Over clause nullptr.");
    }

    direction_ = over->direction();
    if (over->isOverAll()) {
        isOverAll_ = true;
        return Status::OK();
    }
    auto edges = over->edges();
    auto* schemaMng = qctx_->schemaMng();
    for (auto* edge : edges) {
        auto edgeName = *edge->edge();
        auto edgeType = schemaMng->toEdgeType(space_.id, edgeName);
        if (!edgeType.ok()) {
            return Status::Error("%s not found in space [%s].",
                    edgeName.c_str(), space_.name.c_str());
        }
        edgeTypes_.emplace_back(edgeType.value());
    }
    return Status::OK();
}

Status GoValidator::validateWhere(const WhereClause* where) {
    if (where == nullptr) {
        return Status::OK();
    }

    filter_ = where->filter();

    auto typeStatus = deduceExprType(filter_);
    if (!typeStatus.ok()) {
        return typeStatus.status();
    }

    auto type = typeStatus.value();
    if (type != Value::Type::BOOL && type != Value::Type::NULLVALUE) {
        std::stringstream ss;
        ss << "`" << filter_->toString() << "', Filter only accpet bool/null value, "
           << "but was `" << type << "'";
        return Status::Error(ss.str());
    }

    auto status = deduceProps(filter_);
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
}

Status GoValidator::validateYield(const YieldClause* yield) {
    if (yield == nullptr) {
        return Status::Error("Yield clause nullptr.");
    }

    distinct_ = yield->isDistinct();
    auto cols = yield->columns();
    for (auto col : cols) {
        auto colName = deduceColName(col);
        colNames_.emplace_back(colName);

        auto typeStatus = deduceExprType(col->expr());
        NG_RETURN_IF_ERROR(typeStatus);
        auto type = typeStatus.value();
        outputs_.emplace_back(colName, type);

        NG_RETURN_IF_ERROR(deduceProps(col->expr()));
    }
    for (auto& e : edgeProps_) {
        auto found = std::find(edgeTypes_.begin(), edgeTypes_.end(), e.first);
        if (found == edgeTypes_.end()) {
            return Status::Error("Edges should be declared first in over clause.");
        }
    }
    yields_ = yield->yields();
    return Status::OK();
}

Status GoValidator::toPlan() {
    if (steps_ > 1) {
        return buildNStepsPlan();
    } else {
        return buildOneStepPlan();
    }
}

Status GoValidator::buildNStepsPlan() {
    auto* plan = qctx_->plan();
    auto& space = vctx_->whichSpace();

    // loop -> project -> gn1 -> bodyStart
    auto* bodyStart = StartNode::make(plan);

    std::string input;
    PlanNode* projectStartVid = nullptr;
    if (!starts_.empty() && src_ == nullptr) {
        input = buildInput();
    } else {
        projectStartVid = buildRuntimeInput();
        input = projectStartVid->varName();
    }

    auto* gn1 = GetNeighbors::make(plan, bodyStart, space.id);
    gn1->setSrc(src_);
    gn1->setEdgeProps(buildNStepLoopEdgeProps());
    gn1->setInputVar(input);

    auto* columns = new YieldColumns();
    auto* column = new YieldColumn(
        new EdgePropertyExpression(new std::string("*"), new std::string(kDst)),
        new std::string(kVid));
    columns->addColumn(column);

    auto* project = Project::make(plan, gn1, plan->saveObject(columns));
    project->setInputVar(gn1->varName());
    project->setColNames(deduceColNames(columns));

    auto* loop = Loop::make(plan, projectStartVid, project,
                            buildNStepLoopCondition(steps_ - 1));

    auto* gn2 = GetNeighbors::make(plan, loop, space_.id);
    gn2->setSrc(src_);
    gn2->setEdgeTypes(buildEdgeTypes());
    gn2->setVertexProps(buildSrcVertexProps());
    gn2->setEdgeProps(buildEdgeProps());
    gn2->setInputVar(project->varName());


    PlanNode *projectInput = gn2;
    if (filter_ != nullptr) {
        Filter* filterNode = Filter::make(plan, gn2, filter_);
        filterNode->setInputVar(gn2->varName());
        projectInput = filterNode;
    }

    auto* project2 = Project::make(plan, projectInput, yields_);
    project2->setInputVar(projectInput->varName());
    project2->setColNames(std::vector<std::string>(colNames_));

    if (distinct_) {
        Dedup* dedupNode = Dedup::make(plan, project2);
        dedupNode->setInputVar(project2->varName());
        dedupNode->setColNames(std::move(colNames_));
        root_ = dedupNode;
    } else {
        root_ = project2;
    }

    tail_ = projectStartVid == nullptr ? loop : projectStartVid;
    VLOG(1) << "root: " << root_->kind() << " tail: " << tail_->kind();
    return Status::OK();
}

Status GoValidator::buildOneStepPlan() {
    auto* plan = qctx_->plan();

    std::string input;
    if (!starts_.empty() && src_ == nullptr) {
        input = buildInput();
    }

    auto* gn1 = GetNeighbors::make(plan, nullptr, space_.id);
    gn1->setSrc(src_);
    gn1->setEdgeTypes(buildEdgeTypes());
    gn1->setVertexProps(buildSrcVertexProps());
    gn1->setEdgeProps(buildEdgeProps());
    gn1->setInputVar(input);

    if (!dstTagProps_.empty()) {
        // TODO: inplement get vertex props.
        return Status::Error("Not support get dst yet.");
    }

    PlanNode *projectInput = gn1;
    if (filter_ != nullptr) {
        Filter *filterNode = Filter::make(plan, gn1, filter_);
        filterNode->setInputVar(gn1->varName());
        projectInput = filterNode;
    }

    auto* project = Project::make(plan, projectInput, yields_);
    project->setInputVar(projectInput->varName());
    project->setColNames(std::vector<std::string>(colNames_));

    if (distinct_) {
        Dedup *dedupNode = Dedup::make(plan, project);
        dedupNode->setInputVar(project->varName());
        dedupNode->setColNames(std::move(colNames_));
        root_ = dedupNode;
    } else {
        root_ = project;
    }

    tail_ = gn1;
    VLOG(1) << "root: " << root_->kind() << " tail: " << tail_->kind();
    return Status::OK();
}

std::string GoValidator::buildInput() {
    auto input = vctx_->varGen()->getVar();
    DataSet ds;
    ds.colNames.emplace_back(kVid);
    for (auto& vid : starts_) {
        Row row;
        row.values.emplace_back(vid);
        ds.rows.emplace_back(std::move(row));
    }
    qctx_->ectx()->setResult(input, ResultBuilder().value(Value(std::move(ds))).finish());

    auto* vids = new VariablePropertyExpression(
                    new std::string(input),
                    new std::string(kVid));
    qctx_->plan()->saveObject(vids);
    src_ = vids;
    return input;
}

PlanNode* GoValidator::buildRuntimeInput() {
    auto* columns = new YieldColumns();
    auto encode = src_->encode();
    auto decode = Expression::decode(encode);
    auto* column = new YieldColumn(decode.release(), new std::string(kVid));
    columns->addColumn(column);
    auto plan = qctx_->plan();
    auto* project = Project::make(plan, nullptr, plan->saveObject(columns));
    project->setColNames({ kVid });
    newSrc_ = std::make_unique<InputPropertyExpression>(new std::string(kVid));
    src_ = newSrc_.get();
    return project;
}


std::vector<EdgeType> GoValidator::buildEdgeTypes() {
    std::vector<EdgeType> edgeTypes(edgeTypes_.size());
    if (direction_ == storage::cpp2::EdgeDirection::IN_EDGE) {
        std::transform(edgeTypes_.begin(), edgeTypes_.end(), edgeTypes.begin(), [] (auto& type) {
            return -type;
        });
    } else if (direction_ == storage::cpp2::EdgeDirection::BOTH) {
        edgeTypes = edgeTypes_;
        std::transform(edgeTypes_.begin(), edgeTypes_.end(), edgeTypes.begin(), [] (auto& type) {
            return -type;
        });
    } else {
        edgeTypes = edgeTypes_;
    }
    return edgeTypes;
}

GetNeighbors::VertexProps GoValidator::buildSrcVertexProps() {
    GetNeighbors::VertexProps vertexProps;
    if (!srcTagProps_.empty()) {
        vertexProps = std::make_unique<std::vector<storage::cpp2::VertexProp>>(
            srcTagProps_.size());
        std::transform(srcTagProps_.begin(), srcTagProps_.end(),
                       vertexProps->begin(), [](auto& tag) {
                           storage::cpp2::VertexProp vp;
                           vp.tag = tag.first;
                           vp.props = std::move(tag.second);
                           return vp;
                       });
    }
    return vertexProps;
}

GetNeighbors::VertexProps GoValidator::buildDstVertexProps() {
    GetNeighbors::VertexProps vertexProps;
    if (!dstTagProps_.empty()) {
        vertexProps = std::make_unique<std::vector<storage::cpp2::VertexProp>>(
            dstTagProps_.size());
        std::transform(dstTagProps_.begin(), dstTagProps_.end(),
                       vertexProps->begin(), [](auto& tag) {
                           storage::cpp2::VertexProp vp;
                           vp.tag = tag.first;
                           vp.props = std::move(tag.second);
                           return vp;
                       });
    }
    return vertexProps;
}

GetNeighbors::EdgeProps GoValidator::buildEdgeProps() {
    GetNeighbors::EdgeProps edgeProps;
    if (!edgeProps_.empty()) {
        edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>(edgeProps_.size());
        std::transform(edgeProps_.begin(), edgeProps_.end(), edgeProps->begin(), [] (auto& edge) {
            storage::cpp2::EdgeProp ep;
            ep.type = edge.first;
            ep.props = std::move(edge.second);
            return ep;
        });
    }
    return edgeProps;
}

GetNeighbors::EdgeProps GoValidator::buildNStepLoopEdgeProps() {
    GetNeighbors::EdgeProps edgeProps;
    if (!edgeProps_.empty()) {
        edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>(edgeProps_.size());
        std::transform(edgeProps_.begin(), edgeProps_.end(), edgeProps->begin(), [] (auto& edge) {
            storage::cpp2::EdgeProp ep;
            ep.type = edge.first;
            ep.props = std::vector<std::string>({kDst});
            return ep;
        });
    }
    return edgeProps;
}

Expression* GoValidator::buildNStepLoopCondition(int64_t steps) const {
    // ++counter{0} <- steps
    auto counter = vctx_->varGen()->getVar();
    qctx_->ectx()->setValue(counter, 0);
    auto* condition = new RelationalExpression(
        Expression::Kind::kRelLE,
        new UnaryExpression(
            Expression::Kind::kUnaryIncr,
            new VersionedVariableExpression(new std::string(counter),
                                            new ConstantExpression(0))),
        new ConstantExpression(static_cast<int32_t>(steps)));
    qctx_->plan()->saveObject(condition);
    return condition;
}

}  // namespace graph
}  // namespace nebula
