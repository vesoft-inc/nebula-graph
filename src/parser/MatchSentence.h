/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PARSER_MATCHSENTENCE_H_
#define PARSER_MATCHSENTENCE_H_

#include "common/base/Base.h"
#include "common/expression/ContainerExpression.h"
#include "parser/Sentence.h"
#include "parser/Clauses.h"

namespace nebula {

class MatchEdgeProp final {
public:
    MatchEdgeProp(std::string *alias, std::string *edge, Expression *props = nullptr) {
        alias_.reset(alias);
        edge_.reset(edge);
        props_.reset(static_cast<MapExpression*>(props));
    }

    auto get() && {
        return std::make_tuple(std::move(alias_), std::move(edge_), std::move(props_));
    }

private:
    std::unique_ptr<std::string>                        alias_;
    std::unique_ptr<std::string>                        edge_;
    std::unique_ptr<MapExpression>                      props_;
};

enum class MatchDirection : uint8_t {
    kOut,
    kIn,
    kBoth,
};

class MatchEdge final {
public:
    MatchEdge(MatchEdgeProp *prop, MatchDirection direction) {
        if (prop != nullptr) {
            auto tuple = std::move(*prop).get();
            alias_ = std::move(std::get<0>(tuple));
            edge_ = std::move(std::get<1>(tuple));
            props_ = std::move(std::get<2>(tuple));
            delete prop;
        }
        direction_ = direction;
    }

private:
    MatchDirection                                  direction_;
    std::unique_ptr<std::string>                    alias_;
    std::unique_ptr<std::string>                    edge_;
    std::unique_ptr<MapExpression>                  props_;
};


class MatchNode final {
public:
    MatchNode(std::string *alias,
              std::string *label = nullptr,
              Expression *props = nullptr) {
        alias_.reset(alias);
        label_.reset(label);
        props_.reset(static_cast<MapExpression*>(props));
    }

private:
    std::unique_ptr<std::string>                    alias_;
    std::unique_ptr<std::string>                    label_;
    std::unique_ptr<MapExpression>                  props_;
};


class MatchPath final {
public:
    explicit MatchPath(MatchNode *first) {
        first_.reset(first);
    }

    void add(MatchEdge *edge, MatchNode *node) {
        edges_.emplace_back(edge);
        nodes_.emplace_back(node);
    }

private:
    std::unique_ptr<MatchNode>                      first_;
    std::vector<std::unique_ptr<MatchEdge>>         edges_;
    std::vector<std::unique_ptr<MatchNode>>         nodes_;
};


class MatchReturn final {
public:
    MatchReturn() {
        isAll_ = true;
    }

    explicit MatchReturn(YieldColumns *columns) {
        columns_.reset(columns);
    }

private:
    std::unique_ptr<YieldColumns>                   columns_;
    bool                                            isAll_{false};
};


class MatchSentence final : public Sentence {
public:
    MatchSentence(MatchPath *path, WhereClause *filter, MatchReturn *ret) {
        path_.reset(path);
        filter_.reset(filter);
        return_.reset(ret);
    }

    std::string toString() const override {
        return "MATCH";
    }

private:
    std::unique_ptr<MatchPath>                  path_;
    std::unique_ptr<WhereClause>                filter_;
    std::unique_ptr<MatchReturn>                return_;
};

}   // namespace nebula

#endif  // PARSER_MATCHSENTENCE_H_
