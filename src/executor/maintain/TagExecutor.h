/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_MAINTAIN_TAGEXECUTOR_H_
#define EXECUTOR_MAINTAIN_TAGEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class CreateTagExecutor final : public Executor {
public:
    CreateTagExecutor(const PlanNode *node, QueryContext *ectx)
        : Executor("CreateTagExecutor", node, ectx) {}

    folly::Future<GraphStatus> execute() override;
};

class DescTagExecutor final : public Executor {
public:
    DescTagExecutor(const PlanNode *node, QueryContext *ectx)
            : Executor("DescTagExecutor", node, ectx) {}

    folly::Future<GraphStatus> execute() override;

private:
    folly::Future<GraphStatus> descTag();
};

class DropTagExecutor final : public Executor {
public:
    DropTagExecutor(const PlanNode *node, QueryContext *ectx)
            : Executor("DropTagExecutor", node, ectx) {}

    folly::Future<GraphStatus> execute() override;
};

class ShowTagsExecutor final : public Executor {
public:
    ShowTagsExecutor(const PlanNode *node, QueryContext *ectx)
            : Executor("ShowTagsExecutor", node, ectx) {}

    folly::Future<GraphStatus> execute() override;
};

class ShowCreateTagExecutor final : public Executor {
public:
    ShowCreateTagExecutor(const PlanNode *node, QueryContext *ectx)
            : Executor("ShowTagsExecutor", node, ectx) {}

    folly::Future<GraphStatus> execute() override;
};

class AlterTagExecutor final : public Executor {
public:
    AlterTagExecutor(const PlanNode *node, QueryContext *qctx)
            : Executor("AlterTagExecutor", node, qctx) {}

    folly::Future<GraphStatus> execute() override;
};
}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_MAINTAIN_TAGEXECUTOR_H_
