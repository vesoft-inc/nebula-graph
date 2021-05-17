/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "planner/ngql/PathPlanner.h"
#include "validator/Validator.h"
#include "planner/plan/Logic.h"

namespace nebula {
namespace graph {

bool PathPlanner::match(AstContext* astCtx) {
    return astCtx->sentence->kind() == Sentence::Kind::kFindPath;
}

GetNeighbors::EdgeProps PathPlanner::buildEdgeProps(bool reverse) {
    auto edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
    switch (pathCtx_->over.direction) {
        case storage::cpp2::EdgeDirection::IN_EDGE: {
            doBuildEdgeProps(edgeProps, reverse, true);
            break;
        }
        case storage::cpp2::EdgeDirection::OUT_EDGE: {
            doBuildEdgeProps(edgeProps, reverse, false);
            break;
        }
        case storage::cpp2::EdgeDirection::BOTH: {
            doBuildEdgeProps(edgeProps, reverse, true);
            doBuildEdgeProps(edgeProps, reverse, false);
            break;
        }
    }
    return edgeProps;
}

void PathPlanner::doBuildEdgeProps(GetNeighbors::EdgeProps& edgeProps,
                                   bool reverse,
                                   bool isInEdge) {
    for (const auto& e : pathCtx_->over.edgeTypes) {
        stroage::cpp2::EdgeProp ep;
        if (reverse == isInEdge) {
            ep.set_type(e);
        } else {
            ep.set_type(-e);
        }
        ep.set_props({kDst, kType, kRank});
        edgeProps->emplace_back(std::move(ep));
    }
}

void PathPlanner::buildStart(Starts& starts, std::string& vidsVar, bool reverse) {
    if (!starts.vids.empty() && starts.originalSrc == nullptr) {
        buildConstantInput(starts, vidsVar);
    } else {
        if (reverse) {
            pathCtx_->runtimeToDedup = buildRuntimeInput(starts, pathCtx_->runtimeToProject);
            vidsVar = pathCtx_->runtimeToDedup->outputVar();
        } else {
            pathCtx_->runtimeFromDedup = buildRuntimeInput(starts, pathCtx_->runtimeFromProject);
            vidsVar = pathCtx_->runtimeFromDedup->outputVar();
        }
    }
}

// loopSteps{0} <= (steps / 2 + steps % 2) && size(pathVar) == 0
Expression* PathPlanner::singlePairLoopCondition(uint32_t steps, const std:string& pathVar) {
    auto loopSteps = pathCtx_->qctx->vctx()->anonVarGen()->getVar();
    pathCtx_->qctx->ectx()->setValue(loopSteps, 0);
    auto stepLimit = ExpressionUtils::stepCondition(loopSteps, (steps / 2 + steps % 2));
    auto zero = ExpressionUtils::zeroCondition(pathVar)
    return ExpressionUtils::And(stepLimit.release(), zero.release());
}

// loopSteps{0} <= (steps / 2 + steps % 2)
Expression* PathPlanner::allPairLoopCondition(uint32_t steps) {
    auto loopSteps = pathCtx_->qctx->vctx()->anonVarGen()->getVar();
    pathCtx_->qctx->ectx()->setValue(loopSteps, 0);
    return ExpressionUtils::stepCondition(loopSteps, (steps / 2 + steps % 2)).release();
}

// loopSteps{0} <= (steps / 2 + steps % 2) && size(pathVar) != 0
Expression* PathPlanner::multiPairLoopCondition(uint32_t steps, const std:string& pathVar) {
    auto loopSteps = pathCtx_->qctx->vctx()->anonVarGen()->getVar();
    pathCtx_->qctx->ectx()->setValue(loopSteps, 0);
    auto stepLimit = ExpressionUtils::stepCondition(loopSteps, (steps / 2 + steps % 2));
    auto neZero = ExpressionUtils::neZeroCondition(pathVar)
    return ExpressionUtils::And(stepLimit.release(), neZero.release());
}

SubPlan buildRuntimeVidPlan() {
    SubPlan subPlan;
    const auto& from = pathCtx_->from;
    const auto& to = pathCtx_->to;
    if (!from.vids.empty() && from.originalSrc == nullptr) {
        if (!to.vids.empty() && to.originalSrc == nullptr) {
            return subPlan;
        }
        subPlan.tail = pathCtx_->runtimeToProject;
        subPlan.root = pathCtx_->runtimeToDedup;
    } else {
        if (!to.vids.empty() && to.originalSrc == nullptr) {
            subPlan.tail = pathCtx_->runtimeFromProject;
            subPlan.root = pathCtx_->runtimeFromDedup;
        } else {
            auto* toProject = static_cast<SingleInputNode*>(pathCtx_->runtimeToProject);
            toProject->dependsOn(pathCtx_->runtimeFromDedup);
            // set <to>'s input
            auto& inputName = to.fromType == kPipe ? pathCtx_->inputVarName : to.userDefinedVarName;
            toProject->setInputVar(inputName);
            subPlan.tail = pathCtx_->runtimeFromProject;
            subPlan.root = pathCtx_->runtimeToDedup;
        }
    }
    return subPlan;
}

PlanNode* PathPlanner::allPairStartVidDataSet(PlanNode* dep, const std::string& inputVar) {
    // col 0 is vid
    auto* vid = new YieldColumn(new ColumnExpression(0), new std::string(kVid));
    // col 1 is list<path(only contain src)>
    auto* pathExpr = new PathBuildExpression();
    pathExpr->add(new ColumnExpression(0));

    auto* exprList = new ExpressionList();
    exprList->add(pathExpr);
    auto* listExpr = new ListExpression(exprList);
    auto* path = new YieldColumn(listExpr, new std::string("path"));

    auto* columns = pathCtx_->qctx->objPool()->add(new YieldColumns());
    columns->addColumn(vid);
    columns->addColumn(path);

    auto* project = Project::make(pathCtx_->qctx, dep, columns);
    project->setColNames({kVid, "path"});
    project->setInputVar(inputVar);
    project->setOutputVar(inputVar);

    return project;
}

PlanNode* PathPlanner::multiPairStartVidDataSet(PlanNode* dep, const std::string& inputVar) {
    // col 0 is dst
    auto* dst = new YieldColumn(new ColumnExpression(0), new std::string(kDst));
    // col 1 is src
    auto* src = new YieldColumn(new ColumnExpression(1), new std::string(kSrc));
    // col 2 is cost
    auto* cost = new YieldColumn(new ConstantExpression(0), new std::string("cost"));
    // col 3 is list<path(only contain dst)>
    auto* pathExpr = new PathBuildExpression();
    pathExpr->add(new ColumnExpression(0));

    auto* exprList = new ExpressionList();
    exprList->add(pathExpr);
    auto* listExpr = new ListExpression(exprList);
    auto* path = new YieldColumn(listExpr, new std::string("paths"));

    auto* columns = pathCtx_->qctx->objPool()->add(new YieldColumns());
    columns->addColumn(dst);
    columns->addColumn(src);
    columns->addColumn(cost)
    columns->addColumn(path);

    auto* project = Project::make(pathCtx_->qctx, dep, columns);
    project->setColNames({kDst, kSrc, "cost", "path"});
    project->setInputVar(inputVar);
    project->setOutputVar(inputVar);

    return project;
}

SubPlan PathPlanner::allPairLoopDepPlan() {
    SubPlan subPlan = buildRuntimeVidPlan();
    subPlan.root = allPairStartVidDataSet(subPlan.root, pathCtx_->fromVidsVar);
    subPlan.root = allPairStartVidDataSet(subPlan.root, pathCtx_->toVidsVar);
    return subPlan;
}

SubPlan PathPlanner::multiPairLoopDepPlan() {
    SubPlan subPlan = buildRuntimeVidPlan();
    subPlan.root = multiPairStartVidDataSet(subPlan.root, pathCtx_->fromVidsVar);
    subPlan.root = multiPairStartVidDataSet(subPlan.root, pathCtx_->toVidsVar);

    /*
    *  Create the Cartesian product of the start point set and the end point set.
    *  When a pair of paths (start->end) is found,
    *  delete it from the DataSet of cartesianProduct->outputVar().
    *  When the DataSet of cartesianProduct->outputVar() is empty
    *  terminate the execution early
    */
    auto* cartesianProduct = CartesianProduct::make(pathCtx_->qctx, subPlan.root);
    NG_RETURN_IF_ERROR(cartesianProduct->addVar(pathCtx_->fromVidsVar));
    NG_RETURN_IF_ERROR(cartesianProduct->addVar(pathCtx_->toVidsVar));
    subPlan.root = cartesianProduct;
    return subPlan;
}

PlanNode* PathPlanner::singlePairPath(PlanNode* dep, bool reverse) {
    const auto& vidsVar = reverse ? pathCtx_->toVidsVar : pathCtx_->fromVidsVar;
    auto qctx = pathCtx_->qctx;
    auto* src = qctx->objPool()->add(new ColumnExpression(0));

    auto* gn = GetNeighbors::make(qctx, dep, pathCtx_->space.id);
    gn->setSrc(src);
    gn->setEdgeProps(buildEdgeProps(reverse));
    gn->setInputVar(vidsVar);
    gn->setDedup();

    auto* path = BFSShortestPath::make(qctx, gn);
    path->setOutputVar(vidsVar);
    path->setColNames({kVid, "edge"});

    // build first dataset
    DataSet ds;
    ds.colNames = {kVid, "edge"};
    Row row;
    row.values.emplace_back(vidsVar.vids.front());
    row.values.emplace_back(Value::kEmpty);
    ds.rows.emplace_back(std::move(row));
    qctx->ectx()->setResult(vidsVar, ResultBuilder().value(Value(std::move(ds))).finish());
    return path;
}

SubPlan PathPlanner::singlePairPlan(PlanNode* dep) {
    auto* forwardPath = singlePairPath(dep, false);
    auto* backwardPath = singlePairPath(dep, true);
    auto qctx = pathCtx_->qctx;

    auto* conjunct = ConjunctPath::make(
        qctx, forwardPath, backwardPath, ConjunctPath::PathKind::kBiBFS, pathCtx_->steps.steps);
    conjunct->setColNames({"path"});

    auto* loopCondition =
        qctx->objPool()->add(singlePairLoopCondition(pathCtx_->steps.steps, conjunct->outputVar()));
    auto* loop = Loop::make(qctx, nullptr, conjunct, loopCondition);
    auto* dc = DataCollect::make(qctx, DataCollect::DCKind::kBFSShortest);
    dc->setInputVar({conjunct->outputVar()});
    dc->addDep(loop);
    dc->setColNames({"path"});

    SubPlan subPlan;
    subPlan.root = dc;
    subPlan.tail = loop;
    return subPlan;
}

PlanNode* PathPlanner::allPairPath(PlanNode* dep, bool reverse) {
    const auto& vidsVar = reverse ? pathCtx_->toVidsVar : pathCtx_->fromVidsVar;
    auto qctx = pathCtx_->qctx;
    auto* src = qctx->objPool()->add(new ColumnExpression(0));

    auto* gn = GetNeighbors::make(qctx, dep, pathCtx_->space.id);
    gn->setSrc(src);
    gn->setEdgeProps(buildEdgeProps(reverse));
    gn->setInputVar(vidsVar);
    gn->setDedup();

    auto* path = ProduceAllPaths::make(qctx, gn);
    path->setOutputVar(vidsVar);
    path->setColNames({kVid, "path"});
    return path;
}

SubPlan PathPlanner::allPairPlan(PlanNode* dep) {
    auto* forwardPath = allPairPath(dep, false);
    auto* backwardPath = allPairPath(dep, true);
    auto qctx = pathCtx_->qctx;

    auto* conjunct = ConjunctPath::make(
        qctx, forwardPath, backwardPath, ConjunctPath::PathKind::kAllPaths, pathCtx_->steps.steps);
    conjunct->setNoLoop(pathCtx_->noLoop);
    conjunct->setColNames({"path"});

    SubPlan loopDepPlan = allPairLoopDepPlan();
    auto* loopCondition = qctx->objPool()->add(allPairLoopCondition(pathCtx_->steps.steps));
    auto* loop = Loop::make(qctx, loopDepPlan.root, conjunct, loopCondition);

    auto* dc = DataCollect::make(qctx, DataCollect::DCKind::kAllPaths);
    dc->addDep(loop);
    dc->setInputVar({conjunct->outputVar()});
    dc->setColNames({"path"});

    SubPlan subPlan;
    subPlan.root = dc;
    subPlan.tail = loopDepPlan.tail;
    return subPlan;
}

PlanNode* PathPlanner::multiPairPath(PlanNode* dep, bool reverse) {
    const auto& vidsVar = reverse ? pathCtx_->toVidsVar : pathCtx_->fromVidsVar;
    auto qctx = pathCtx_->qctx;
    auto* src = qctx->objPool()->add(new ColumnExpression(0));

    auto* gn = GetNeighbors::make(qctx, dep, pathCtx_->space.id);
    gn->setSrc(src);
    gn->setEdgeProps(buildEdgeProps(reverse));
    gn->setInputVar(vidsVar);
    gn->setDedup();

    auto* path = ProduceSemiShortestPath::make(qctx, gn);
    path->setOutputVar(vidsVar);
    path->setColNames({kDst, kSrc, "cost", "paths"});

    return path;
}

SubPlan PathPlanner::multiPairPlan(PlanNode* dep) {
    auto* forwardPath = multiPairPath(dep, false);
    auto* backwardPath = multiPairPath(dep, true);
    auto qctx = pathCtx_->qctx;

    auto* conjunct = ConjunctPath::make(
        qctx, forwardPath, backwardPath, ConjunctPath::PathKind::kFloyd, pathCtx_->steps.steps);
    conjunct->setColNames({"_path", "cost"});

    SubPlan loopDepPlan = multiPairLoopDepPlan();
    // loopDepPlan.root is cartesianProduct
    const auto& endConditionVar = loopDepPlan.root->outputVar();
    conjunct->setConditionalVar(endConditionVar);
    auto* loopCondition =
        qctx->objPool()->add(multiPairLoopCondition(pathCtx_->steps.steps, endConditionVar));
    auto* loop = Loop::make(qctx, loopDepPlan.root, conjunct, loopCondition);

    auto* dc = DataCollect::make(qctx, DataCollect::DCKind::kAllPaths);
    dc->addDep(loop);
    dc->setInputVar({conjunct->outputVar()});
    dc->setColNames({"path"});

    SubPlan subPlan;
    subPlan.root = dc;
    subPlan.tail = loopDepPlan.tail;
    return subPlan;
}

StatusOr<SubPlan> PathPlanner::transform(AstContext* astCtx) {
    pathCtx_ = static_cast<PathContext *>(astCtx);

    buildStart(pathCtx_->from, pathCtx_->fromVidsVar, false);
    buildStart(pathCtx_->to, pathCtx_->toVidsVar, true);

    auto* startNode = StartNode::make(pathCtx_->qctx);
    auto* pt = PassThroughNode::make(pathCtx_->qctx, startNode);

    SubPlan subPlan;
    do {
        if (!pathCtx_->isShortest || pathCtx_->noLoop) {
            subPlan = allPairPlan(pt);
            break;
        }
        if (pathCtx_->from.vids.size() == 1 && pathCtx_->to.vids.size() == 1) {
            subPlan = singlePairPlan(pt);
            break;
        }
        subPlan = multiPairPlan(pt);
    } while (0);
    // get path's property
    return subPlan;
}

}  // namespace graph
}  // namespace nebula
