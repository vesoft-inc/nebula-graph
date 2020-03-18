/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "scheduler/Scheduler.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/task_queue/UnboundedBlockingQueue.h>
#include <vector>

#include "exec/ExecutionContext.h"
#include "exec/Executor.h"
#include "exec/FilterExecutor.h"
#include "exec/LoopExecutor.h"
#include "planner/Query.h"
#include "scheduler/PlanFragment.h"

namespace nebula {
namespace graph {

Scheduler::Scheduler(uint32_t numThreads) {
    auto taskQueue =
        std::make_unique<folly::UnboundedBlockingQueue<folly::CPUThreadPoolExecutor::CPUTask>>();
    threadPool_ = std::make_unique<folly::CPUThreadPoolExecutor>(
        numThreads,
        std::move(taskQueue),
        std::make_shared<folly::NamedThreadFactory>("graph-scheduler"));
}

void Scheduler::schedulePlanFragment(PlanFragment* root, ExecutionContext* ectx) {
    // Handle plan fragment dependencies firstly
    for (auto& dep : root->depends()) {
        schedulePlanFragment(dep, ectx);
    }

    auto exec = root->convertPlan(ectx);
    addTask([=]() {
        auto status = exec->execute();
        if (!status.ok()) {
            LOG(ERROR) << status.toString();
        }
    });
}

void Scheduler::addTask(std::function<void()> task) {
    threadPool_->add(task);
}

// static
folly::Future<Status> exec(Executor* node, ExecutionContext* ectx) {
    switch (node->node()->kind()) {
        case PlanNode::Kind::kSort:
            DCHECK(node->depends().empty());
            return folly::makeFuture();
        case PlanNode::Kind::kLoop: {
            auto* loop = static_cast<LoopExecutor*>(node);
            const std::vector<Executor*> deps = loop->depends();
            DCHECK_EQ(deps.size(), 2U);
            auto* firstOp = deps[1];
            auto gen = [=]() { return exec(firstOp, ectx); };
            loop->setLoopBody(std::move(gen));
            return exec(deps[0], ectx).then([=](Status s) {
                if (!s.ok()) return s;
                return loop->execute();
            });
        }
        case PlanNode::Kind::kFilter:
            auto* filter = static_cast<FilterExecutor*>(node);
            auto deps = filter->depends();
            DCHECK_EQ(deps.size(), 1);
            return exec(deps[0], ectx).then([=](Status s) {
                if (!s.ok()) return s;
                return filter->execute();
            });
        case PlanNode::Kind::kSort:
            break;
        case PlanNode::Kind::kUnion:
            auto* unionNode = static_cast<UnionExecutor*>(node);
            std::vector<folly::Future<Status>> futures;
            for (auto& child : unionNode->children()) {
                futures.push_back(exec(child.get(), ectx));
            }
            return folly::collect(futures).then([=](std::vector<Status> ss) {
                for (auto& s : ss) {
                    if (!s.ok()) {
                        return folly::makeFuture(s);
                    }
                }
                return unionNode->execute(ectx);
            });
        default:
            break;
    }
}

}   // namespace graph
}   // namespace nebula
