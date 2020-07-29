/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/Executor.h"

#include <folly/String.h>
#include <folly/executors/InlineExecutor.h>

#include "context/ExecutionContext.h"
#include "context/QueryContext.h"
#include "exec/ExecutionError.h"
#include "exec/admin/SnapshotExecutor.h"
#include "exec/admin/SpaceExecutor.h"
#include "exec/admin/SwitchSpaceExecutor.h"
#include "exec/logic/LoopExecutor.h"
#include "exec/logic/MultiOutputsExecutor.h"
#include "exec/logic/SelectExecutor.h"
#include "exec/logic/StartExecutor.h"
#include "exec/maintain/EdgeExecutor.h"
#include "exec/maintain/TagExecutor.h"
#include "exec/mutate/InsertEdgesExecutor.h"
#include "exec/mutate/InsertVerticesExecutor.h"
#include "exec/query/AggregateExecutor.h"
#include "exec/query/DataCollectExecutor.h"
#include "exec/query/DedupExecutor.h"
#include "exec/query/FilterExecutor.h"
#include "exec/query/GetEdgesExecutor.h"
#include "exec/query/GetNeighborsExecutor.h"
#include "exec/query/GetVerticesExecutor.h"
#include "exec/query/IntersectExecutor.h"
#include "exec/query/LimitExecutor.h"
#include "exec/query/MinusExecutor.h"
#include "exec/query/ProjectExecutor.h"
#include "exec/query/ReadIndexExecutor.h"
#include "exec/query/SortExecutor.h"
#include "exec/query/UnionExecutor.h"
#include "exec/query/DataJoinExecutor.h"
#include "exec/query/LookupExecutor.h"
#include "planner/Admin.h"
#include "planner/Maintain.h"
#include "planner/Mutate.h"
#include "planner/PlanNode.h"
#include "planner/Query.h"
#include "util/ObjectPool.h"

using folly::stringPrintf;

namespace nebula {
namespace graph {

// static
Executor *Executor::makeExecutor(const PlanNode *node, QueryContext *qctx) {
    std::unordered_map<int64_t, Executor *> visited;
    return makeExecutor(node, qctx, &visited);
}

// static
Executor *Executor::makeExecutor(const PlanNode *node,
                                 QueryContext *qctx,
                                 std::unordered_map<int64_t, Executor *> *visited) {
    auto iter = visited->find(node->id());
    if (iter != visited->end()) {
        return iter->second;
    }

    Executor *exec = nullptr;

    switch (node->kind()) {
        case PlanNode::Kind::kMultiOutputs: {
            auto mout = asNode<MultiOutputsNode>(node);
            auto dep = makeExecutor(mout->dep(), qctx, visited);
            exec = new MultiOutputsExecutor(mout, qctx);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kAggregate: {
            auto agg = asNode<Aggregate>(node);
            auto dep = makeExecutor(agg->dep(), qctx, visited);
            exec = new AggregateExecutor(agg, qctx);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kSort: {
            auto sort = asNode<Sort>(node);
            auto dep = makeExecutor(sort->dep(), qctx, visited);
            exec = new SortExecutor(sort, qctx);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kFilter: {
            auto filter = asNode<Filter>(node);
            auto dep = makeExecutor(filter->dep(), qctx, visited);
            exec = new FilterExecutor(filter, qctx);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kGetEdges: {
            auto ge = asNode<GetEdges>(node);
            auto dep = makeExecutor(ge->dep(), qctx, visited);
            exec = new GetEdgesExecutor(ge, qctx);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kGetVertices: {
            auto gv = asNode<GetVertices>(node);
            auto dep = makeExecutor(gv->dep(), qctx, visited);
            exec = new GetVerticesExecutor(gv, qctx);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kGetNeighbors: {
            auto gn = asNode<GetNeighbors>(node);
            auto dep = makeExecutor(gn->dep(), qctx, visited);
            exec = new GetNeighborsExecutor(gn, qctx);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kLimit: {
            auto limit = asNode<Limit>(node);
            auto dep = makeExecutor(limit->dep(), qctx, visited);
            exec = new LimitExecutor(limit, qctx);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kProject: {
            auto project = asNode<Project>(node);
            auto dep = makeExecutor(project->dep(), qctx, visited);
            exec = new ProjectExecutor(project, qctx);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kReadIndex: {
            auto readIndex = asNode<ReadIndex>(node);
            auto dep = makeExecutor(readIndex->dep(), qctx, visited);
            exec = new ReadIndexExecutor(readIndex, qctx);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kLookup: {
            auto lookup = asNode<Lookup>(node);
            auto dep = makeExecutor(lookup->dep(), qctx, visited);
            exec = new LookupExecutor(lookup, qctx);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kStart: {
            exec = new StartExecutor(node, qctx);
            break;
        }
        case PlanNode::Kind::kUnion: {
            auto uni = asNode<Union>(node);
            auto left = makeExecutor(uni->left(), qctx, visited);
            auto right = makeExecutor(uni->right(), qctx, visited);
            exec = new UnionExecutor(uni, qctx);
            exec->addDependent(left)->addDependent(right);
            break;
        }
        case PlanNode::Kind::kIntersect: {
            auto intersect = asNode<Intersect>(node);
            auto left = makeExecutor(intersect->left(), qctx, visited);
            auto right = makeExecutor(intersect->right(), qctx, visited);
            exec = new IntersectExecutor(intersect, qctx);
            exec->addDependent(left)->addDependent(right);
            break;
        }
        case PlanNode::Kind::kMinus: {
            auto minus = asNode<Minus>(node);
            auto left = makeExecutor(minus->left(), qctx, visited);
            auto right = makeExecutor(minus->right(), qctx, visited);
            exec = new MinusExecutor(minus, qctx);
            exec->addDependent(left)->addDependent(right);
            break;
        }
        case PlanNode::Kind::kLoop: {
            auto loop = asNode<Loop>(node);
            auto dep = makeExecutor(loop->dep(), qctx, visited);
            auto body = makeExecutor(loop->body(), qctx, visited);
            exec = new LoopExecutor(loop, qctx, body);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kSelect: {
            auto select = asNode<Select>(node);
            auto dep = makeExecutor(select->dep(), qctx, visited);
            auto then = makeExecutor(select->then(), qctx, visited);
            auto els = makeExecutor(select->otherwise(), qctx, visited);
            exec = new SelectExecutor(select, qctx, then, els);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kDedup: {
            auto dedup = asNode<Dedup>(node);
            auto dep = makeExecutor(dedup->dep(), qctx, visited);
            exec = new DedupExecutor(dedup, qctx);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kSwitchSpace: {
            auto switchSpace = asNode<SwitchSpace>(node);
            auto dep = makeExecutor(switchSpace->dep(), qctx, visited);
            exec = new SwitchSpaceExecutor(switchSpace, qctx);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kCreateSpace: {
            auto createSpace = asNode<CreateSpace>(node);
            auto dep = makeExecutor(createSpace->dep(), qctx, visited);
            exec = new CreateSpaceExecutor(createSpace, qctx);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kDescSpace: {
            auto descSpace = asNode<DescSpace>(node);
            auto dep = makeExecutor(descSpace->dep(), qctx, visited);
            exec = new DescSpaceExecutor(descSpace, qctx);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kShowSpaces: {
            auto showSpaces = asNode<ShowSpaces>(node);
            auto input = makeExecutor(showSpaces->dep(), qctx, visited);
            exec = new ShowSpacesExecutor(showSpaces, qctx);
            exec->addDependent(input);
            break;
        }
        case PlanNode::Kind::kDropSpace: {
            auto dropSpace = asNode<DropSpace>(node);
            auto input = makeExecutor(dropSpace->dep(), qctx, visited);
            exec = new DropSpaceExecutor(dropSpace, qctx);
            exec->addDependent(input);
            break;
        }
        case PlanNode::Kind::kShowCreateSpace: {
            auto showCreateSpace = asNode<ShowCreateSpace>(node);
            auto input = makeExecutor(showCreateSpace->dep(), qctx, visited);
            exec = new ShowCreateSpaceExecutor(showCreateSpace, qctx);
            exec->addDependent(input);
            break;
        }
        case PlanNode::Kind::kCreateTag: {
            auto createTag = asNode<CreateTag>(node);
            auto dep = makeExecutor(createTag->dep(), qctx, visited);
            exec = new CreateTagExecutor(createTag, qctx);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kDescTag: {
            auto descTag = asNode<DescTag>(node);
            auto dep = makeExecutor(descTag->dep(), qctx, visited);
            exec = new DescTagExecutor(descTag, qctx);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kAlterTag: {
            auto alterTag = asNode<AlterTag>(node);
            auto dep = makeExecutor(alterTag->dep(), qctx, visited);
            exec = new AlterTagExecutor(alterTag, qctx);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kCreateEdge: {
            auto createEdge = asNode<CreateEdge>(node);
            auto dep = makeExecutor(createEdge->dep(), qctx, visited);
            exec = new CreateEdgeExecutor(createEdge, qctx);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kDescEdge: {
            auto descEdge = asNode<DescEdge>(node);
            auto dep = makeExecutor(descEdge->dep(), qctx, visited);
            exec = new DescEdgeExecutor(descEdge, qctx);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kAlterEdge: {
            auto alterEdge = asNode<AlterEdge>(node);
            auto dep = makeExecutor(alterEdge->dep(), qctx, visited);
            exec = new AlterEdgeExecutor(alterEdge, qctx);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kShowTags: {
            auto showTags = asNode<ShowTags>(node);
            auto input = makeExecutor(showTags->dep(), qctx, visited);
            exec = new ShowTagsExecutor(showTags, qctx);
            exec->addDependent(input);
            break;
        }
        case PlanNode::Kind::kShowEdges: {
            auto showEdges = asNode<ShowEdges>(node);
            auto input = makeExecutor(showEdges->dep(), qctx, visited);
            exec = new ShowEdgesExecutor(showEdges, qctx);
            exec->addDependent(input);
            break;
        }
        case PlanNode::Kind::kDropTag: {
            auto dropTag = asNode<DropTag>(node);
            auto input = makeExecutor(dropTag->dep(), qctx, visited);
            exec = new DropTagExecutor(dropTag, qctx);
            exec->addDependent(input);
            break;
        }
        case PlanNode::Kind::kDropEdge: {
            auto dropEdge = asNode<DropEdge>(node);
            auto input = makeExecutor(dropEdge->dep(), qctx, visited);
            exec = new DropEdgeExecutor(dropEdge, qctx);
            exec->addDependent(input);
            break;
        }
        case PlanNode::Kind::kShowCreateTag: {
            auto showCreateTag = asNode<ShowCreateTag>(node);
            auto input = makeExecutor(showCreateTag->dep(), qctx, visited);
            exec = new ShowCreateTagExecutor(showCreateTag, qctx);
            exec->addDependent(input);
            break;
        }
        case PlanNode::Kind::kShowCreateEdge: {
            auto showCreateEdge = asNode<ShowCreateEdge>(node);
            auto input = makeExecutor(showCreateEdge->dep(), qctx, visited);
            exec = new ShowCreateEdgeExecutor(showCreateEdge, qctx);
            exec->addDependent(input);
            break;
        }
        case PlanNode::Kind::kInsertVertices: {
            auto insertV = asNode<InsertVertices>(node);
            auto dep = makeExecutor(insertV->dep(), qctx, visited);
            exec = new InsertVerticesExecutor(insertV, qctx);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kInsertEdges: {
            auto insertE = asNode<InsertEdges>(node);
            auto dep = makeExecutor(insertE->dep(), qctx, visited);
            exec = new InsertEdgesExecutor(insertE, qctx);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kDataCollect: {
            auto dc = asNode<DataCollect>(node);
            auto dep = makeExecutor(dc->dep(), qctx, visited);
            exec = new DataCollectExecutor(dc, qctx);
            exec->addDependent(dep);
            break;
        }
        case PlanNode::Kind::kCreateSnapshot: {
            auto createSnapshot = asNode<CreateSnapshot>(node);
            auto input = makeExecutor(createSnapshot->dep(), qctx, visited);
            exec = new CreateSnapshotExecutor(createSnapshot, qctx);
            exec->addDependent(input);
            break;
        }
        case PlanNode::Kind::kDropSnapshot: {
            auto dropSnapshot = asNode<DropSnapshot>(node);
            auto input = makeExecutor(dropSnapshot->dep(), qctx, visited);
            exec = new DropSnapshotExecutor(dropSnapshot, qctx);
            exec->addDependent(input);
            break;
        }
        case PlanNode::Kind::kShowSnapshots: {
            auto showSnapshots = asNode<ShowSnapshots>(node);
            auto input = makeExecutor(showSnapshots->dep(), qctx, visited);
            exec = new ShowSnapshotsExecutor(showSnapshots, qctx);
            exec->addDependent(input);
            break;
        }
        case PlanNode::Kind::kDataJoin: {
            auto dataJoin = asNode<DataJoin>(node);
            auto input = makeExecutor(dataJoin->dep(), qctx, visited);
            exec = new DataJoinExecutor(dataJoin, qctx);
            exec->addDependent(input);
            break;
        }
        case PlanNode::Kind::kUnknown:
        default:
            LOG(FATAL) << "Unknown plan node kind " << static_cast<int32_t>(node->kind());
            break;
    }

    DCHECK(!!exec);

    visited->insert({node->id(), exec});
    return qctx->objPool()->add(exec);
}

Executor::Executor(const std::string &name, const PlanNode *node, QueryContext *qctx)
    : id_(DCHECK_NOTNULL(node)->id()),
      name_(name),
      node_(DCHECK_NOTNULL(node)),
      qctx_(DCHECK_NOTNULL(qctx)),
      ectx_(DCHECK_NOTNULL(qctx->ectx())) {
    // Initialize the position in ExecutionContext for each executor before execution plan
    // starting to run. This will avoid lock something for thread safety in real execution
    if (!ectx_->exist(node->varName())) {
        ectx_->initVar(node->varName());
    }
}

folly::Future<Status> Executor::start(Status status) const {
    return folly::makeFuture(std::move(status)).via(runner());
}

folly::Future<Status> Executor::error(Status status) const {
    return folly::makeFuture<Status>(ExecutionError(std::move(status))).via(runner());
}

Status Executor::finish(Result &&result) {
    ectx_->setResult(node()->varName(), std::move(result));
    return Status::OK();
}

void Executor::dumpLog() const {
    VLOG(4) << name() << "(" << id() << ")";
}

folly::Executor *Executor::runner() const {
    if (!qctx() || !qctx()->rctx() || !qctx()->rctx()->runner()) {
        // This is just for test
        return &folly::InlineExecutor::instance();
    }
    return qctx()->rctx()->runner();
}

}   // namespace graph
}   // namespace nebula
