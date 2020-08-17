/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef PARSER_SENTENCE_H_
#define PARSER_SENTENCE_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "common/expression/SymbolPropertyExpression.h"
#include "common/expression/ArithmeticExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/RelationalExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/UnaryExpression.h"
#include "common/expression/UUIDExpression.h"
#include "common/expression/LabelExpression.h"

namespace nebula {

class Sentence {
public:
    virtual ~Sentence() {}
    virtual std::string toString() const = 0;

    enum class Kind : uint32_t {
        kUnknown,
        kExplain,
        kSequential,
        kGo,
        kSet,
        kPipe,
        kUse,
        kMatch,
        kAssignment,
        kCreateTag,
        kAlterTag,
        kCreateEdge,
        kAlterEdge,
        kDescribeTag,
        kDescribeEdge,
        kCreateTagIndex,
        kCreateEdgeIndex,
        kDropTagIndex,
        kDropEdgeIndex,
        kDescribeTagIndex,
        kDescribeEdgeIndex,
        kRebuildTagIndex,
        kRebuildEdgeIndex,
        kDropTag,
        kDropEdge,
        kInsertVertices,
        kUpdateVertex,
        kInsertEdges,
        kUpdateEdge,
        kShowHosts,
        kShowSpaces,
        kShowParts,
        kShowTags,
        kShowEdges,
        kShowTagIndexes,
        kShowEdgeIndexes,
        kShowUsers,
        kShowRoles,
        kShowCreateSpace,
        kShowCreateTag,
        kShowCreateEdge,
        kShowCreateTagIndex,
        kShowCreateEdgeIndex,
        kShowTagIndexStatus,
        kShowEdgeIndexStatus,
        kShowSnapshots,
        kShowCharset,
        kShowCollation,
        kDeleteVertices,
        kDeleteEdges,
        kLookup,
        kCreateSpace,
        kDropSpace,
        kDescribeSpace,
        kYield,
        kCreateUser,
        kDropUser,
        kAlterUser,
        kGrant,
        kRevoke,
        kChangePassword,
        kDownload,
        kIngest,
        kOrderBy,
        kConfig,
        kFetchVertices,
        kFetchEdges,
        kBalance,
        kFindPath,
        kLimit,
        kGroupBy,
        kReturn,
        kCreateSnapshot,
        kDropSnapshot,
        kAdminJob,
        kGetSubgraph,
    };

    Kind kind() const {
        return kind_;
    }

protected:
    Sentence() = default;
    explicit Sentence(Kind kind) : kind_(kind) {}

    Kind                kind_{Kind::kUnknown};
};

class CreateSentence : public Sentence {
public:
    explicit CreateSentence(bool ifNotExist) : ifNotExist_{ifNotExist} {}
    virtual ~CreateSentence() {}

    bool isIfNotExist() {
        return ifNotExist_;
    }

private:
    bool ifNotExist_{false};
};

class DropSentence : public Sentence {
public:
    explicit  DropSentence(bool ifExists) : ifExists_{ifExists} {}
    virtual ~DropSentence() = default;

    bool isIfExists() {
        return ifExists_;
    }
private:
    bool ifExists_{false};
};

inline std::ostream& operator<<(std::ostream &os, Sentence::Kind kind) {
    return os << static_cast<uint32_t>(kind);
}

}   // namespace nebula

#endif  // PARSER_SENTENCE_H_
