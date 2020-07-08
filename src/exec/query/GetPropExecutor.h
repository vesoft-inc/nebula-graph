/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class GetPropExecutor : public Executor {
protected:
    GetPropExecutor(const std::string &name, const PlanNode *node, QueryContext *qctx)
        : Executor(name, node, qctx) {}

    Status handleResp(storage::StorageRpcResponse<storage::cpp2::GetPropResponse> &&rpcResp) {
        auto result = handleCompleteness(rpcResp);
        if (!result.ok()) {
            return std::move(result).status();
        }
        auto state = std::move(result).value();
        // Ok, merge DataSets to one
        nebula::DataSet v;
        if (!rpcResp.responses().empty()) {
            if (rpcResp.responses().front().__isset.props) {
                v = std::move(*rpcResp.responses().front().get_props());
            }
        }
        if (rpcResp.responses().size() > 1) {
            for (std::size_t i = 1; i < rpcResp.responses().size(); ++i) {
                auto resp = rpcResp.responses()[i];
                if (resp.__isset.props) {
                    if (UNLIKELY(!v.append(std::move(*resp.get_props())))) {
                        // it's impossible according to the interface
                        LOG(WARNING) << "Heterogeneous props dataset";
                        state.setState(StateDesc::State::kPartialSuccess);
                    }
                }
            }
        }
        for (auto &colName : v.colNames) {
            for (auto &c : colName) {
                if (c == ':') {
                    c = '.';
                }
            }
        }
        return finish(ExecResult::buildSequential(std::move(v), std::move(state)));
    }
};

}   // namespace graph
}   // namespace nebula
