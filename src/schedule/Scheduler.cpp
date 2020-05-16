/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "schedule/Scheduler.h"

#include "exec/ExecutionError.h"
#include "exec/Executor.h"
#include "exec/query/LoopExecutor.h"
#include "exec/query/MultiOutputsExecutor.h"
#include "exec/query/SelectExecutor.h"
#include "planner/PlanNode.h"
#include "service/ExecutionContext.h"

namespace nebula {
namespace graph {

Scheduler::Task::Task(const Executor *e) : planId(e->node()->id()) {
    DCHECK_NOTNULL(e);
}

Scheduler::Scheduler(ExecutionContext *ectx) : ectx_(ectx) {
    DCHECK_NOTNULL(ectx_);
}

folly::Future<Status> Scheduler::schedule(Executor *executor) {
    switch (executor->node()->kind()) {
        case PlanNode::Kind::kSelector: {
            auto sel = static_cast<SelectExecutor *>(executor);
            return schedule(sel->input())
                .then(task(sel,
                           [sel, this](Status status) {
                               if (!status.ok()) return error(std::move(status));
                               return sel->execute();
                           }))
                .then(task(sel, [sel, this](Status status) {
                    if (!status.ok()) return error(std::move(status));

                    auto val = ectx_->getValue(sel->node()->varName());
                    auto cond = val.moveBool();
                    return schedule(cond ? sel->thenBody() : sel->elseBody());
                }));
        }
        case PlanNode::Kind::kLoop: {
            auto loop = static_cast<LoopExecutor *>(executor);
            return schedule(loop->input()).then(task(loop, [loop, this](Status status) {
                if (!status.ok()) return error(std::move(status));
                return iterate(loop);
            }));
        }
        case PlanNode::Kind::kMultiOutputs: {
            // TODO(yee)
            return executor->execute();
        }
        default: {
            switch (executor->numInputs()) {
                case 0: {
                    // StartExecutor
                    return executor->execute();
                }
                case 1: {
                    auto single = static_cast<SingleInputExecutor *>(executor);
                    return schedule(single->input())
                        .then(task(single, [single, this](Status status) {
                            if (!status.ok()) return error(std::move(status));
                            return single->execute();
                        }));
                }
                default: {
                    auto multiple = static_cast<MultiInputsExecutor *>(executor);
                    std::vector<folly::Future<Status>> futures;
                    for (auto input : multiple->inputs()) {
                        futures.emplace_back(schedule(input));
                    }
                    return folly::collect(futures).then(
                        task(multiple, [multiple, this](std::vector<Status> stats) {
                            for (auto s : stats) {
                                if (!s.ok()) return error(std::move(s));
                            }
                            return multiple->execute();
                        }));
                }
            }
        }
    }
}

folly::Future<Status> Scheduler::error(Status status) const {
    return folly::makeFuture<Status>(ExecutionError(std::move(status)));
}

folly::Future<Status> Scheduler::iterate(LoopExecutor *loop) {
    return loop->execute().then(task(loop, [loop, this](Status status) {
        if (!status.ok()) return error(std::move(status));

        auto val = ectx_->getValue(loop->node()->varName());
        auto cond = val.moveBool();
        if (!cond) return folly::makeFuture(Status::OK());
        return schedule(loop->loopBody()).then(task(loop, [loop, this](Status s) {
            if (!s.ok()) return error(std::move(s));
            return iterate(loop);
        }));
    }));
}

}   // namespace graph
}   // namespace nebula
