/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_VALIDATECONTEXT_H_
#define VALIDATOR_VALIDATECONTEXT_H_

#include "meta/SchemaManager.h"
#include "service/ClientSession.h"

namespace nebula {
namespace graph {
class ValidateContext final {
public:
    void switchToSpace(std::string spaceName, GraphSpaceID spaceId) {
        spaces_.emplace_back(std::make_pair(std::move(spaceName), spaceId));
    }

    void registerVariable(std::string var) {
        vars_.emplace_back(std::move(var));
    }

    bool spaceChosen() const {
        return !spaces_.empty();
    }

    const std::pair<std::string, GraphSpaceID>& whichSpace() const {
        return spaces_.back();
    }

    meta::SchemaManager* schemaMng() {
        return schemaMng_;
    }

private:
    meta::SchemaManager*                                schemaMng_;
    ClientSession*                                      session_;
    std::vector<std::pair<std::string, GraphSpaceID>>   spaces_;
    std::vector<std::string>                            vars_;
};
}  // namespace graph
}  // namespace nebula
#endif
