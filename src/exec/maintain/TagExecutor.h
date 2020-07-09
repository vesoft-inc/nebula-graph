/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_MAINTAIN_TAGEXECUTOR_H_
#define EXEC_MAINTAIN_TAGEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class CreateTagExecutor final : public Executor {
public:
    CreateTagExecutor(const PlanNode *node, QueryContext *ectx)
        : Executor("CreateTagExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};

class DescTagExecutor final : public Executor {
public:
    DescTagExecutor(const PlanNode *node, QueryContext *ectx)
            : Executor("DescTagExecutor", node, ectx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> descTag();
};

class DropTagExecutor final : public Executor {
public:
    DropTagExecutor(const PlanNode *node, QueryContext *ectx)
            : Executor("DropTagExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};

class ShowTagsExecutor final : public Executor {
public:
    ShowTagsExecutor(const PlanNode *node, QueryContext *ectx)
            : Executor("ShowTagsExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};

class ShowCreateTagExecutor final : public Executor {
public:
    ShowCreateTagExecutor(const PlanNode *node, QueryContext *ectx)
            : Executor("ShowTagsExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_MAINTAIN_TAGEXECUTOR_H_
