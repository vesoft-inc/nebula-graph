/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_SETEXECUTOR_H_
#define EXEC_QUERY_SETEXECUTOR_H_

#include <memory>

#include "exec/Executor.h"

namespace nebula {

class Iterator;

namespace graph {

class SetExecutor : public Executor {
protected:
    SetExecutor(const std::string &name, const PlanNode *node, QueryContext *qctx)
        : Executor(name, node, qctx) {}

    Status validateInputDataSets();
    std::unique_ptr<Iterator> getLeftInputDataIter() const;
    std::unique_ptr<Iterator> getRightInputDataIter() const;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_SETEXECUTOR_H_
