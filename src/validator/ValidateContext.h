/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_VALIDATECONTEXT_H_
#define VALIDATOR_VALIDATECONTEXT_H_

#include "common/meta/SchemaManager.h"
#include "service/ClientSession.h"
#include "common/datatypes/Value.h"
#include "common/charset/Charset.h"
#include "planner/ExecutionPlan.h"
#include "util/AnnoVarGenerator.h"

namespace nebula {
namespace graph {

using ColDef = std::pair<std::string, Value::Type>;
using ColsDef = std::vector<ColDef>;

struct SpaceDescription {
    std::string name;
    GraphSpaceID id;
};

class ValidateContext final {
public:
    ValidateContext() {
        varGen_ = std::make_unique<AnnoVarGenerator>();
    }

    void switchToSpace(std::string spaceName, GraphSpaceID spaceId) {
        SpaceDescription space;
        space.name = std::move(spaceName);
        space.id = spaceId;
        spaces_.emplace_back(std::move(space));
    }

    void registerVariable(std::string var, ColsDef cols) {
        vars_.emplace(std::move(var), std::move(cols));
    }

    void setPlan(ExecutionPlan* plan) {
        plan_ = plan;
    }

    void setSession(ClientSession* session) {
        session_ = session;
    }

    void setQueryContext(QueryContext* qctx) {
        qctx_ = qctx;
    }

    bool spaceChosen() const {
        return !spaces_.empty();
    }

    const SpaceDescription& whichSpace() const {
        return spaces_.back();
    }

    meta::SchemaManager* schemaMng() const {
        return qctx_->schemaManager();
    }

    ExecutionPlan* plan() const {
        return plan_;
    }

    CharsetInfo* getCharsetInfo() {
        return qctx_->getCharsetInfo();
    }

    ClientSession* session() const {
        return session_;
    }

    QueryContext* qctx() const {
        return qctx_;
    }

    AnnoVarGenerator* varGen() const {
        return varGen_.get();
    }

private:
    QueryContext*                                       qctx_{nullptr};
    ClientSession*                                      session_{nullptr};
    // spaces_ is the trace of space switch
    std::vector<SpaceDescription>                       spaces_;
    // vars_ saves all named variable
    std::unordered_map<std::string, ColsDef>            vars_;
    ExecutionPlan*                                      plan_{nullptr};
    std::unique_ptr<AnnoVarGenerator>                   varGen_;
};

}  // namespace graph
}  // namespace nebula
#endif
