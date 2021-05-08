/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "planner/planners/PathPlanner.h"
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

void PathPlanner::buildStart(Starts& starts, std::string& startVidsVar, bool reverse) {
    if (!starts.vids.empty() && starts.originalSrc == nullptr) {
        buildConstantInput(starts, startVidsVar);
    } else {
        if (reverse) {
            pathCtx_->toDedupStartVid = buildRuntimeInput(starts, pathCtx_->toProjectStartVid);
            startVidsVar = pathCtx_->toDedupStartVid->outputVar();
        } else {
            pathCtx_->fromDedupStartVid = buildRuntimeInput(starts, pathCtx_->fromProjectStartVid);
            startVidsVar = pathCtx_->fromDedupStartVid->outputVar();
        }
    }
}

// loopSteps{0} <= (steps / 2 + steps % 2) && size(pathVar) == 0
Expression* PathPlanner::singlePairLoopCondition(uint32_t steps, const std:string& pathVar) {
    auto loopSteps = pathCtx_->qctx->vctx()->anonVarGen()->getVar();
    pathCtx_->qctx->ectx()->setValue(loopSteps, 0);
    auto* stepLimit = ExpressionUtils::stepCondition(loopSteps, (steps / 2 + steps % 2));
    auto* zero = ExpressionUtils::zeroCondition(pathVar)
    return ExpressionUtils::And(stepLimit, zero);
}

// loopSteps{0} <= (steps / 2 + steps % 2)
Expression* PathPlanner::allPairLoopCondition(uint32_t steps) {
    auto loopSteps = pathCtx_->qctx->vctx()->anonVarGen()->getVar();
    pathCtx_->qctx->ectx()->setValue(loopSteps, 0);
    return ExpressionUtils::stepCondition(loopSteps, (steps / 2 + steps % 2));
}

// loopSteps{0} <= (steps / 2 + steps % 2) && size(pathVar) != 0
Expression* PathPlanner::multiPairLoopCondition(uint32_t steps, const std:string& pathVar) {
    auto loopSteps = pathCtx_->qctx->vctx()->anonVarGen()->getVar();
    pathCtx_->qctx->ectx()->setValue(loopSteps, 0);
    auto* stepLimit = ExpressionUtils::stepCondition(loopSteps, (steps / 2 + steps % 2));
    auto* neZero = ExpressionUtils::neZeroCondition(pathVar)
    return ExpressionUtils::And(stepLimit, neZero);
}

SubPlan buildRuntimeVidPlan() {
    SubPlan subPlan;
    if (!pathCtx_->from.vids.empty() && pathCtx_->from.originalSrc == nullptr) {
        if (!pathCtx_->to.vids.empty() && pathCtx_->to.originalSrc == nullptr) {
            return subPlan;
        }
        subPlan.tail = pathCtx_->toProjectStartVid;
        subPlan.root = pathCtx_->toDedupStartVid;
    } else {
        if (!pathCtx_->to.vids.empty() && pathCtx_->to.originalSrc == nullpltr) {
            subPlan.tail = pathCtx_->fromProjectStartVid;
            subPlan.root = pathCtx_->fromDedupStartVid;
        } else {
            auto* toProject = static_cast<SingleInputNode*>(pathCtx_->toProjectStartVid);
            toProject->dependsOn(pathCtx_->fromDedupStartVid);
            // TODO
            // auto inputName = pathCtx_->to.fromType == kPikp ?
            // toProject->setInputVar(inputVarName); //todo
            subPlan.tail = pathCtx_->fromProjectStartVid;
            subPlan.root = pathCtx_->toDedupStartVid;
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
    subPlan.root = allPairStartVidDataSet(subPlan.root, pathCtx_->fromStartVid);
    subPlan.root = allPairStartVidDataSet(subPlan.root, pathCtx_->toStartVid);
    return subPlan;
}

SubPlan PathPlanner::multiPairLoopDepPlan() {
    SubPlan subPlan = buildRuntimeVidPlan();
    subPlan.root = multiPairStartVidDataSet(subPlan.root, pathCtx_->fromStartVid);
    subPlan.root = multiPairStartVidDataSet(subPlan.root, pathCtx_->toStartVid);

    /*
    *  Create the Cartesian product of the start point set and the end point set.
    *  When a pair of paths (start->end) is found,
    *  delete it from the DataSet of cartesianProduct->outputVar().
    *  When the DataSet of cartesianProduct->outputVar() is empty
    *  terminate the execution early
    */
    auto* cartesianProduct = CartesianProduct::make(pathCtx_->qctx, subPlan.root);
    NG_RETURN_IF_ERROR(cartesianProduct->addVar(pathCtx_->fromStartVidsVar));
    NG_RETURN_IF_ERROR(cartesianProduct->addVar(pathCtx_->toStartVidsVar));
    subPlan.root = cartesianProduct;
    return subPlan;
}

PlanNode* PathPlanner::singlePairPath(PlanNode* dep, bool reverse) {
    const auto& startVidsVar = reverse ? pathCtx_->toStartVidsVar : pathCtx_->fromStartVidsVar;
    const auto* src = reverse ? pathCtx_->to.src : pathCtx_->from.src;

    auto* gn = GetNeighbors::make(pathCtx_->qctx, dep, pathCtx_->space.id);
    gn->setSrc(src);
    gn->setEdgeProps(buildEdgeProps(reverse));
    gn->setInputVar(startVidsVar);
    gn->setDedup();

    auto* path = BFSShortestPath::make(pathCtx_->qctx, gn);
    path->setOutputVar(startVidsVar);
    path->setColNames({kVid, "edge"});

    // build original value
    DataSet ds;
    ds.colNames = {kVid, "edge"};
    Row row;
    row.values.emplace_back();
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
    const auto& startVidsVar = reverse ? pathCtx_->toStartVidsVar : pathCtx_->fromStartVidsVar;
    const auto* src = reverse ? pathCtx_->to.src : pathCtx_->from.src;
    auto* gn = GetNeighbors::make(pathCtx_->qctx, dep, pathCtx_->space.id);
    gn->setSrc(src);
    gn->setEdgeProps(buildEdgeProps(reverse));
    gn->setInputVar(startVidsVar);
    gn->setDedup();

    auto* path = ProduceAllPaths::make(pathCtx_->qctx, gn);
    path->setOutputVar(startVidsVar);
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
    const auto& startVidsVar = reverse ? pathCtx_->toStartVidsVar : pathCtx_->fromStartVidsVar;
    const auto* src = reverse ? pathCtx_->to.src : pathCtx_->from.src;
    auto* gn = GetNeighbors::make(pathCtx_->qctx, dep, pathCtx_->space.id);
    gn->setSrc(src);
    gn->setEdgeProps(buildEdgeProps(reverse));
    gn->setInputVar(startVidsVar);
    gn->setDedup();

    auto* path = ProduceSemiShortestPath::make(pathCtx_->qctx, gn);
    path->setColNames({kDst, kSrc, "cost", "paths"});
    return path;
}

SubPlan PathPlanner::multiPairPlan(PlanNode* dep) {
    auto* forwardPath = multiPairPath(dep, false);
    auto* backwardPath = multiPairPath(dep, true);

    auto qctx = pathCtx_->qctx;
    auto* conjunct = ConjunctPath::make(
        qctx, forwardPath, backwardPath, ConjunctPath::PathKind::kFloyd, pathCtx_->steps.steps);

    SubPlan loopDepPlan = multiPairLoopDepPlan();
    auto* loopCondition = qctx->objPool()->add(multiPairLoopCondition(pathCtx_->steps.steps));
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

    buildStart(pathCtx_->from, pathCtx_->fromStartVidsVar, false);
    buildStart(pathCtx_->to, pathCtx_->toStartVidsVar, true);

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
