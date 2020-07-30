/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "exec/StorageExecutor.h"
#include "common/clients/storage/StorageClientBase.h"

namespace nebula {
namespace graph {

class GetPropExecutor : public StorageExecutor {
protected:
    GetPropExecutor(const std::string &name, const PlanNode *node, QueryContext *qctx)
        : StorageExecutor(name, node, qctx) {}

    Status handleResp(storage::StorageRpcResponse<storage::cpp2::GetPropResponse> &&rpcResp) {
        auto result = handleCompleteness(rpcResp, false);
        NG_RETURN_IF_ERROR(result);
        auto state = std::move(result).value();
        // Ok, merge DataSets to one
        nebula::DataSet v;
        for (auto &resp : rpcResp.responses()) {
            if (resp.__isset.props) {
                if (UNLIKELY(!v.append(std::move(*resp.get_props())))) {
                    // it's impossible according to the interface
                    LOG(WARNING) << "Heterogeneous props dataset";
                    state = Result::State::kPartialSuccess;
                }
            } else {
                state = Result::State::kPartialSuccess;
            }
        }
        for (auto &colName : v.colNames) {
            std::replace(colName.begin(), colName.end(), ':', '.');
        }
        VLOG(1) << "Resp: " << v;
        return finish(ResultBuilder()
                      .value(std::move(v))
                      .iter(Iterator::Kind::kSequential)
                      .state(state)
                      .finish());
    }
};

}   // namespace graph
}   // namespace nebula
