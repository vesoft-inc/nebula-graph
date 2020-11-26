/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef _EXEC_QUERY_GET_PROP_EXECUTOR_H_
#define _EXEC_QUERY_GET_PROP_EXECUTOR_H_

#include "executor/StorageAccessExecutor.h"
#include "common/clients/storage/StorageClientBase.h"
#include "service/GraphFlags.h"

using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetPropResponse;

namespace nebula {
namespace graph {

class GetPropExecutor : public StorageAccessExecutor {
protected:
    GetPropExecutor(const std::string &name, const PlanNode *node, QueryContext *qctx)
        : StorageAccessExecutor(name, node, qctx) {}

    Status handleResp(StorageRpcResponse<GetPropResponse> &&rpcResp,
                      const std::vector<std::string> &colNames);

    Status handlePathVertices(StorageRpcResponse<GetPropResponse> &&rpcResp,
                              const std::vector<std::string> &colNames);
};

}   // namespace graph
}   // namespace nebula

#endif  // _EXEC_QUERY_GET_PROP_EXECUTOR_H_
