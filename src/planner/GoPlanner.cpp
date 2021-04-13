/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/GoPlanner.h"

#include "validator/Validator.h"
#include "planner/Logic.h"

namespace nebula {
namespace graph {
bool GoPlanner::match(AstContext* astCtx) {
    return astCtx->sentence->kind() == Sentence::Kind::kGo;
}

StatusOr<SubPlan> GoPlanner::transform(AstContext* astCtx) {
    goCtx_ = static_cast<GoAstContext*>(astCtx);
    SubPlan subPlan;
    if (!goCtx_->steps.isMToN() && goCtx_->steps.steps() == 0) {
        auto* passThrough = PassThroughNode::make(goCtx_->qctx, nullptr);
        passThrough->setColNames(std::move(goCtx_->colNames));
        subPlan.tail = passThrough;
        subPlan.root = passThrough;
        return subPlan;
    }

    std::string startVidsVar;
    PlanNode* dedupStartVid = nullptr;
    if (!goCtx_->from.vids.empty() && goCtx_->from.originalSrc == nullptr) {
        buildConstantInput(goCtx_->from, startVidsVar);
    } else {
        dedupStartVid = buildRuntimeInput(goCtx_->from, goCtx_->projectStartVid);
        startVidsVar = dedupStartVid->outputVar();
    }

    PlanNode* getNeighbors = nullptr;
    PlanNode* dependencyForProjectResult = nullptr;
    PlanNode* projectSrcEdgeProps = nullptr;

    bool needJoinInput = !goCtx_->exprProps.inputProps().empty() ||
                            !goCtx_->exprProps.varProps().empty() ||
                            goCtx_->from.fromType != FromType::kInstantExpr;
    bool needJoinDst = !goCtx_->exprProps.dstTagProps().empty();
    if (!goCtx_->steps.isMToN() && goCtx_->steps.steps() == 1) {
        auto* gn = GetNeighbors::make(goCtx_->qctx, dedupStartVid, goCtx_->space.id);
        gn->setSrc(goCtx_->from.src);
        gn->setVertexProps(buildSrcVertexProps());
        gn->setEdgeProps(buildEdgeProps());
        gn->setInputVar(startVidsVar);
        VLOG(1) << gn->outputVar();

        getNeighbors = gn;
        dependencyForProjectResult = gn;

        if (needJoinInput || needJoinDst) {
            projectSrcEdgeProps =
                buildProjectSrcEdgePropsForGN(gn->outputVar(), gn, needJoinInput, needJoinDst);
        }
    } else {
        auto* gn = GetVarStepsNeighbors::make(goCtx_->qctx, dedupStartVid, goCtx_->space.id);
        gn->setSrc(goCtx_->from.src);
        gn->setVertexProps(buildSrcVertexProps());
        gn->setEdgeDst(buildEdgeDst());
        gn->setEdgeProps(buildEdgeProps());
        gn->setInputVar(startVidsVar);
        gn->setSteps(goCtx_->steps);
        VLOG(1) << gn->outputVar();

        getNeighbors = gn;
        dependencyForProjectResult = gn;

        if (needJoinInput || needJoinDst) {
            if (needJoinInput && goCtx_->steps.isMToN()) {
                gn->setSteps(StepClause(goCtx_->steps.nSteps()));
            }
            if (goCtx_->steps.isMToN()) {
                gn->setUnion();
            }
            projectSrcEdgeProps =
                buildTraceProjectForGN(gn->outputVar(), gn, needJoinInput, needJoinDst);
        }
    }

    // Join the dst props if $$.tag.prop was declared.
    PlanNode* joinDstProps = nullptr;
    if (needJoinDst && projectSrcEdgeProps != nullptr) {
        joinDstProps = buildJoinDstProps(projectSrcEdgeProps);
    }
    if (joinDstProps != nullptr) {
        dependencyForProjectResult = joinDstProps;
    }

    PlanNode* joinInput = nullptr;
    if (needJoinInput && (projectSrcEdgeProps != nullptr || joinDstProps != nullptr)) {
        joinInput = buildJoinPipeOrVariableInput(
            nullptr, joinDstProps == nullptr ? projectSrcEdgeProps : joinDstProps);
    }
    if (joinInput != nullptr) {
        dependencyForProjectResult = joinInput;
    }

    if (goCtx_->filter != nullptr) {
        auto* filterNode = Filter::make(goCtx_->qctx, dependencyForProjectResult,
                    goCtx_->newFilter != nullptr ? goCtx_->newFilter : goCtx_->filter);
        filterNode->setInputVar(dependencyForProjectResult->outputVar());
        filterNode->setColNames(dependencyForProjectResult->colNames());
        dependencyForProjectResult = filterNode;
    }
    auto* projectResult =
        Project::make(goCtx_->qctx, dependencyForProjectResult,
        goCtx_->newYieldCols != nullptr ? goCtx_->newYieldCols : goCtx_->yields);
    projectResult->setInputVar(dependencyForProjectResult->outputVar());
    projectResult->setColNames(std::vector<std::string>(goCtx_->colNames));
    if (goCtx_->distinct) {
        Dedup* dedupNode = Dedup::make(goCtx_->qctx, projectResult);
        dedupNode->setInputVar(projectResult->outputVar());
        dedupNode->setColNames(std::move(goCtx_->colNames));
        subPlan.root = dedupNode;
    } else {
        subPlan.root = projectResult;
    }

    if (goCtx_->projectStartVid != nullptr) {
        subPlan.tail = goCtx_->projectStartVid;
    } else {
        subPlan.tail = getNeighbors;
    }
    return subPlan;
}

PlanNode* GoPlanner::buildTraceProjectForGN(std::string gnVar,
                                            PlanNode* dependency,
                                            bool needJoinInput,
                                            bool needJoinDst) {
    DCHECK(dependency != nullptr);
    DCHECK(dependency->kind() == PlanNode::Kind::kGetVarStepsNeighbors);

    // Get all _dst to a single column which used for join the dst vertices.
    if (needJoinDst) {
        goCtx_->joinDstVidColName = goCtx_->qctx->vctx()->anonColGen()->getCol();
        auto* dstVidCol =
            new YieldColumn(new EdgePropertyExpression(new std::string("*"), new std::string(kDst)),
                            new std::string(goCtx_->joinDstVidColName));
        goCtx_->srcAndEdgePropCols->addColumn(dstVidCol);
    }

    PlanNode* pro = nullptr;
    if (needJoinInput) {
        auto* project = TraceProject::make(goCtx_->qctx, dependency, goCtx_->srcAndEdgePropCols);
        project->setInputVar(gnVar);
        auto colNames = Validator::deduceColNames(goCtx_->srcAndEdgePropCols);
        colNames.emplace_back(kVid);
        project->setColNames(std::move(colNames));
        if (goCtx_->steps.isMToN()) {
            project->setMToN();
        }
        pro = project;
    } else {
        auto* project = Project::make(goCtx_->qctx, dependency, goCtx_->srcAndEdgePropCols);
        project->setInputVar(gnVar);
        project->setColNames(Validator::deduceColNames(goCtx_->srcAndEdgePropCols));
        pro = project;
    }
    VLOG(1) << pro->outputVar();

    return pro;
}

PlanNode* GoPlanner::buildProjectSrcEdgePropsForGN(std::string gnVar,
                                                   PlanNode* dependency,
                                                   bool needJoinInput,
                                                   bool needJoinDst) {
    DCHECK(dependency != nullptr);

    // Get _vid for join if $-/$var were declared.
    if (needJoinInput) {
        auto* srcVidCol = new YieldColumn(
            new VariablePropertyExpression(new std::string(gnVar), new std::string(kVid)),
            new std::string(kVid));
        goCtx_->srcAndEdgePropCols->addColumn(srcVidCol);
    }

    // Get all _dst to a single column.
    if (needJoinDst) {
        goCtx_->joinDstVidColName = goCtx_->qctx->vctx()->anonColGen()->getCol();
        auto* dstVidCol =
            new YieldColumn(new EdgePropertyExpression(new std::string("*"), new std::string(kDst)),
                            new std::string(goCtx_->joinDstVidColName));
        goCtx_->srcAndEdgePropCols->addColumn(dstVidCol);
    }

    auto* project = Project::make(goCtx_->qctx, dependency, goCtx_->srcAndEdgePropCols);
    project->setInputVar(gnVar);
    project->setColNames(Validator::deduceColNames(goCtx_->srcAndEdgePropCols));
    VLOG(1) << project->outputVar();

    return project;
}

PlanNode* GoPlanner::buildJoinDstProps(PlanNode* projectSrcDstProps) {
    DCHECK(goCtx_->dstPropCols != nullptr);
    DCHECK(projectSrcDstProps != nullptr);

    auto objPool = goCtx_->qctx->objPool();

    auto* vids = objPool->makeAndAdd<VariablePropertyExpression>(
        new std::string(projectSrcDstProps->outputVar()),
        new std::string(goCtx_->joinDstVidColName));
    auto* getDstVertices = GetVertices::make(
        goCtx_->qctx, projectSrcDstProps, goCtx_->space.id, vids, buildDstVertexProps(), {});
    getDstVertices->setInputVar(projectSrcDstProps->outputVar());
    getDstVertices->setDedup();

    auto vidColName = goCtx_->qctx->vctx()->anonColGen()->getCol();
    auto* vidCol = new YieldColumn(
        new VariablePropertyExpression(new std::string(getDstVertices->outputVar()),
                                    new std::string(kVid)),
        new std::string(vidColName));
    goCtx_->dstPropCols->addColumn(vidCol);
    auto* project = Project::make(goCtx_->qctx, getDstVertices, goCtx_->dstPropCols);
    project->setInputVar(getDstVertices->outputVar());
    project->setColNames(Validator::deduceColNames(goCtx_->dstPropCols));

    auto* joinHashKey = objPool->makeAndAdd<VariablePropertyExpression>(
        new std::string(projectSrcDstProps->outputVar()),
        new std::string(goCtx_->joinDstVidColName));
    auto* probeKey = objPool->makeAndAdd<VariablePropertyExpression>(
        new std::string(project->outputVar()), new std::string(vidColName));
    auto joinDst = LeftJoin::make(goCtx_->qctx, project,
            {projectSrcDstProps->outputVar(), ExecutionContext::kLatestVersion},
            {project->outputVar(), ExecutionContext::kLatestVersion},
            {joinHashKey}, {probeKey});
    VLOG(1) << joinDst->outputVar() << " hash key: " << joinHashKey->toString()
        << " probe key: " << probeKey->toString();
    std::vector<std::string> colNames = projectSrcDstProps->colNames();
    for (auto& col : project->colNames()) {
        colNames.emplace_back(col);
    }
    joinDst->setColNames(std::move(colNames));

    return joinDst;
}

PlanNode* GoPlanner::buildJoinPipeOrVariableInput(PlanNode* projectFromJoin,
                                                    PlanNode* dependencyForJoinInput) {
    UNUSED(projectFromJoin);
    auto* pool = goCtx_->qctx->objPool();

    DCHECK(dependencyForJoinInput != nullptr);
    auto* joinHashKey = pool->add(new VariablePropertyExpression(
        new std::string(dependencyForJoinInput->outputVar()), new std::string(kVid)));
    std::string varName =
        goCtx_->from.fromType == kPipe ? goCtx_->inputVarName : goCtx_->from.userDefinedVarName;
    auto* joinInput =
        LeftJoin::make(goCtx_->qctx, dependencyForJoinInput,
                        {dependencyForJoinInput->outputVar(),
                        ExecutionContext::kLatestVersion},
                        {varName,
                        ExecutionContext::kLatestVersion},
                        {joinHashKey}, {goCtx_->from.originalSrc});
    std::vector<std::string> colNames = dependencyForJoinInput->colNames();
    auto* varPtr = goCtx_->qctx->symTable()->getVar(varName);
    DCHECK(varPtr != nullptr);
    for (auto& col : varPtr->colNames) {
        colNames.emplace_back(col);
    }
    joinInput->setColNames(std::move(colNames));
    VLOG(1) << joinInput->outputVar();

    return joinInput;
}

GetNeighbors::VertexProps GoPlanner::buildSrcVertexProps() {
    GetNeighbors::VertexProps vertexProps;
    if (!goCtx_->exprProps.srcTagProps().empty()) {
        vertexProps = std::make_unique<std::vector<storage::cpp2::VertexProp>>(
            goCtx_->exprProps.srcTagProps().size());
        std::transform(goCtx_->exprProps.srcTagProps().begin(),
                       goCtx_->exprProps.srcTagProps().end(),
                       vertexProps->begin(),
                       [](auto& tag) {
                           storage::cpp2::VertexProp vp;
                           vp.set_tag(tag.first);
                           std::vector<std::string> props(tag.second.begin(), tag.second.end());
                           vp.set_props(std::move(props));
                           return vp;
                       });
    }
    return vertexProps;
}

std::vector<storage::cpp2::VertexProp> GoPlanner::buildDstVertexProps() {
    std::vector<storage::cpp2::VertexProp> vertexProps(goCtx_->exprProps.dstTagProps().size());
    if (!goCtx_->exprProps.dstTagProps().empty()) {
        std::transform(goCtx_->exprProps.dstTagProps().begin(),
                       goCtx_->exprProps.dstTagProps().end(),
                       vertexProps.begin(),
                       [](auto& tag) {
                           storage::cpp2::VertexProp vp;
                           vp.set_tag(tag.first);
                           std::vector<std::string> props(tag.second.begin(), tag.second.end());
                           vp.set_props(std::move(props));
                           return vp;
                       });
    }
    return vertexProps;
}

GetNeighbors::EdgeProps GoPlanner::buildEdgeProps() {
    GetNeighbors::EdgeProps edgeProps;
    bool onlyInputPropsOrConstant = goCtx_->exprProps.srcTagProps().empty() &&
                                    goCtx_->exprProps.dstTagProps().empty() &&
                                    goCtx_->exprProps.edgeProps().empty();
    if (!goCtx_->exprProps.edgeProps().empty()) {
        if (goCtx_->over.direction == storage::cpp2::EdgeDirection::IN_EDGE) {
            edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
            buildEdgeProps(edgeProps, true);
        } else if (goCtx_->over.direction == storage::cpp2::EdgeDirection::BOTH) {
            edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
            buildEdgeProps(edgeProps, false);
            buildEdgeProps(edgeProps, true);
        } else {
            edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
            buildEdgeProps(edgeProps, false);
        }
    } else if (!goCtx_->exprProps.dstTagProps().empty() || onlyInputPropsOrConstant) {
        return buildEdgeDst();
    }

    return edgeProps;
}

void GoPlanner::buildEdgeProps(GetNeighbors::EdgeProps& edgeProps, bool isInEdge) {
    edgeProps->reserve(goCtx_->over.edgeTypes.size());
    bool needJoin = !goCtx_->exprProps.dstTagProps().empty();
    for (auto& e : goCtx_->over.edgeTypes) {
        storage::cpp2::EdgeProp ep;
        if (isInEdge) {
            ep.set_type(-e);
        } else {
            ep.set_type(e);
        }

        const auto& propsFound = goCtx_->exprProps.edgeProps().find(e);
        if (propsFound == goCtx_->exprProps.edgeProps().end()) {
            ep.set_props({kDst});
        } else {
            std::vector<std::string> props(propsFound->second.begin(), propsFound->second.end());
            if (needJoin && propsFound->second.find(kDst) == propsFound->second.end()) {
                props.emplace_back(kDst);
            }
            ep.set_props(std::move(props));
        }
        edgeProps->emplace_back(std::move(ep));
    }
}

GetNeighbors::EdgeProps GoPlanner::buildEdgeDst() {
    GetNeighbors::EdgeProps edgeProps;
    bool onlyInputPropsOrConstant = goCtx_->exprProps.srcTagProps().empty() &&
                                    goCtx_->exprProps.dstTagProps().empty() &&
                                    goCtx_->exprProps.edgeProps().empty();
    if (!goCtx_->exprProps.edgeProps().empty() || !goCtx_->exprProps.dstTagProps().empty() ||
        onlyInputPropsOrConstant) {
        if (goCtx_->over.direction == storage::cpp2::EdgeDirection::IN_EDGE) {
            edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>(
                goCtx_->over.edgeTypes.size());
            std::transform(goCtx_->over.edgeTypes.begin(),
                           goCtx_->over.edgeTypes.end(),
                           edgeProps->begin(),
                           [](auto& type) {
                               storage::cpp2::EdgeProp ep;
                               ep.set_type(-type);
                               ep.set_props({kDst});
                               return ep;
                           });
        } else if (goCtx_->over.direction == storage::cpp2::EdgeDirection::BOTH) {
            edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>(
                goCtx_->over.edgeTypes.size() * 2);
            std::transform(goCtx_->over.edgeTypes.begin(),
                           goCtx_->over.edgeTypes.end(),
                           edgeProps->begin(),
                           [](auto& type) {
                               storage::cpp2::EdgeProp ep;
                               ep.set_type(type);
                               ep.set_props({kDst});
                               return ep;
                           });
            std::transform(goCtx_->over.edgeTypes.begin(), goCtx_->over.edgeTypes.end(),
                           edgeProps->begin() + goCtx_->over.edgeTypes.size(),
                           [](auto& type) {
                               storage::cpp2::EdgeProp ep;
                               ep.set_type(-type);
                               ep.set_props({kDst});
                               return ep;
                           });
        } else {
            edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>(
                goCtx_->over.edgeTypes.size());
            std::transform(goCtx_->over.edgeTypes.begin(),
                           goCtx_->over.edgeTypes.end(),
                           edgeProps->begin(),
                           [](auto& type) {
                               storage::cpp2::EdgeProp ep;
                               ep.set_type(type);
                               ep.set_props({kDst});
                               return ep;
                           });
        }
    }
    return edgeProps;
}

void GoPlanner::buildConstantInput(Starts& starts, std::string& startVidsVar) {
    startVidsVar = goCtx_->qctx->vctx()->anonVarGen()->getVar();
    DataSet ds;
    ds.colNames.emplace_back(kVid);
    for (auto& vid : starts.vids) {
        Row row;
        row.values.emplace_back(vid);
        ds.rows.emplace_back(std::move(row));
    }
    goCtx_->qctx->ectx()->setResult(startVidsVar,
                                      ResultBuilder().value(Value(std::move(ds))).finish());

    starts.src =
        new VariablePropertyExpression(new std::string(startVidsVar), new std::string(kVid));
    goCtx_->qctx->objPool()->add(starts.src);
}

PlanNode* GoPlanner::buildRuntimeInput(Starts& starts, PlanNode*& projectStartVid) {
    auto pool = goCtx_->qctx->objPool();
    auto* columns = pool->add(new YieldColumns());
    auto* column = new YieldColumn(starts.originalSrc->clone().release(), new std::string(kVid));
    columns->addColumn(column);

    auto* project = Project::make(goCtx_->qctx, nullptr, columns);
    if (starts.fromType == kVariable) {
        project->setInputVar(starts.userDefinedVarName);
    }
    project->setColNames({kVid});
    VLOG(1) << project->outputVar() << " input: " << project->inputVar();
    starts.src = pool->add(new InputPropertyExpression(new std::string(kVid)));

    auto* dedupVids = Dedup::make(goCtx_->qctx, project);
    dedupVids->setInputVar(project->outputVar());
    dedupVids->setColNames(project->colNames());

    projectStartVid = project;
    return dedupVids;
}
}  // namespace graph
}  // namespace nebula
