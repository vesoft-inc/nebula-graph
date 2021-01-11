/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PARSER_MATCHSENTENCE_H_
#define PARSER_MATCHSENTENCE_H_

#include "common/base/Base.h"
#include "common/expression/ContainerExpression.h"
#include "common/expression/SubscriptExpression.h"
#include "parser/Sentence.h"
#include "parser/TraverseSentences.h"
#include "parser/Clauses.h"

namespace nebula {

class MatchEdgeTypeList final {
public:
    MatchEdgeTypeList() = default;

    void add(std::string *item) {
        items_.emplace_back(item);
    }

    auto items() && {
        return std::move(items_);
    }

private:
    std::vector<std::unique_ptr<std::string>>           items_;
};


class MatchStepRange final {
public:
    explicit MatchStepRange(int64_t min, int64_t max = std::numeric_limits<int64_t>::max()) {
        min_ = min;
        max_ = max;
    }

    auto min() const {
        return min_;
    }

    auto max() const {
        return max_;
    }

private:
    int64_t         min_{1};
    int64_t         max_{1};
};


class MatchEdgeProp final {
public:
    MatchEdgeProp(std::string *alias,
                  MatchEdgeTypeList *types,
                  MatchStepRange *range,
                  Expression *props = nullptr) {
        alias_.reset(alias);
        range_.reset(range);
        props_.reset(static_cast<MapExpression*>(props));
        if (types != nullptr) {
            types_ = std::move(*types).items();
            delete types;
        }
    }

    auto get() && {
        return std::make_tuple(std::move(alias_),
                               std::move(types_),
                               std::move(range_),
                               std::move(props_));
    }

private:
    std::unique_ptr<std::string>                        alias_;
    std::vector<std::unique_ptr<std::string>>           types_;
    std::unique_ptr<MapExpression>                      props_;
    std::unique_ptr<MatchStepRange>                     range_;
};


class MatchEdge final {
public:
    using Direction = nebula::storage::cpp2::EdgeDirection;
    MatchEdge(MatchEdgeProp *prop, Direction direction) {
        if (prop != nullptr) {
            auto tuple = std::move(*prop).get();
            alias_ = std::move(std::get<0>(tuple));
            types_ = std::move(std::get<1>(tuple));
            range_ = std::move(std::get<2>(tuple));
            props_ = std::move(std::get<3>(tuple));
            delete prop;
        }
        direction_ = direction;
    }

    auto direction() const {
        return direction_;
    }

    const std::string* alias() const {
        return alias_.get();
    }

    auto& types() const {
        return types_;
    }

    const MapExpression* props() const {
        return props_.get();
    }

    auto* range() const {
        return range_.get();
    }

    std::string toString() const;

private:
    Direction                                       direction_;
    std::unique_ptr<std::string>                    alias_;
    std::vector<std::unique_ptr<std::string>>       types_;
    std::unique_ptr<MatchStepRange>                 range_;
    std::unique_ptr<MapExpression>                  props_;
};

class MatchNodeLabel final {
public:
    explicit MatchNodeLabel(std::string *label, Expression *props = nullptr) :
        label_(label), props_(static_cast<MapExpression*>(props)) {
            DCHECK(props == nullptr || props->kind() == Expression::Kind::kMap);
    }

    const std::string* label() const {
        return label_.get();
    }

    const MapExpression* props() const {
        return props_.get();
    }

    MapExpression* props() {
        return props_.get();
    }

    std::string toString() const {
        std::stringstream ss;
        ss << ":" << *label_;
        if (props_ != nullptr) {
            ss << props_->toString();
        }
        return ss.str();
    }

private:
    std::unique_ptr<std::string>                    label_;
    std::unique_ptr<MapExpression>                  props_;
};

class MatchNodeLabelList final {
public:
    void add(MatchNodeLabel *label) {
        labels_.emplace_back(label);
    }

    const auto& labels() const {
        return labels_;
    }

    std::string toString() const {
        std::stringstream ss;
        for (const auto &label : labels_) {
            ss << label->toString();
        }
        return ss.str();
    }

private:
    std::vector<std::unique_ptr<MatchNodeLabel>> labels_;
};

class MatchNode final {
public:
    MatchNode(std::string *alias,
              MatchNodeLabelList *labels,
              Expression *props = nullptr) {
        alias_.reset(alias);
        labels_.reset(labels);
        props_.reset(static_cast<MapExpression*>(props));
    }

    const std::string* alias() const {
        return alias_.get();
    }

    const auto* labels() const {
        return labels_.get();
    }

    const MapExpression* props() const {
        return props_.get();
    }

    std::string toString() const;

private:
    std::unique_ptr<std::string>                    alias_;
    std::unique_ptr<MatchNodeLabelList>             labels_;
    std::unique_ptr<MapExpression>                  props_;
};


class MatchPath final {
public:
    explicit MatchPath(MatchNode *node) {
        nodes_.emplace_back(node);
    }

    void add(MatchEdge *edge, MatchNode *node) {
        edges_.emplace_back(edge);
        nodes_.emplace_back(node);
    }

    void setAlias(std::string *alias) {
        alias_.reset(alias);
    }

    const std::string* alias() const {
        return alias_.get();
    }

    const auto& nodes() const {
        return nodes_;
    }

    const auto& edges() const {
        return edges_;
    }

    size_t steps() const {
        return edges_.size();
    }

    const MatchNode* node(size_t i) const {
        return nodes_[i].get();
    }

    const MatchEdge* edge(size_t i) const {
        return edges_[i].get();
    }

    std::string toString() const;

private:
    std::unique_ptr<std::string>                    alias_;
    std::vector<std::unique_ptr<MatchNode>>         nodes_;
    std::vector<std::unique_ptr<MatchEdge>>         edges_;
};


class MatchReturn final {
public:
    explicit MatchReturn(YieldColumns *columns = nullptr,
                         OrderFactors *orderFactors = nullptr,
                         Expression *skip = nullptr,
                         Expression *limit = nullptr,
                         bool distinct = false) {
        columns_.reset(columns);
        orderFactors_.reset(orderFactors);
        skip_.reset(skip);
        limit_.reset(limit);
        isDistinct_ = distinct;
        if (columns_ == nullptr) {
            isAll_ = true;
        }
    }

    const YieldColumns* columns() const {
        return columns_.get();
    }

    void setColumns(YieldColumns *columns) {
        columns_.reset(columns);
    }

    bool isAll() const {
        return isAll_;
    }

    bool isDistinct() const {
        return isDistinct_;
    }

    const Expression* skip() const {
        return skip_.get();
    }

    const Expression* limit() const {
        return limit_.get();
    }

    OrderFactors* orderFactors() {
        return orderFactors_.get();
    }

    const OrderFactors* orderFactors() const {
        return orderFactors_.get();
    }

    std::string toString() const;

private:
    std::unique_ptr<YieldColumns>                   columns_;
    bool                                            isAll_{false};
    bool                                            isDistinct_{false};
    std::unique_ptr<OrderFactors>                   orderFactors_;
    std::unique_ptr<Expression>                     skip_;
    std::unique_ptr<Expression>                     limit_;
};


class ReadingClause {
public:
    enum class Kind: uint8_t {
        kMatch,
        kUnwind,
        kWith,
    };
    explicit ReadingClause(Kind kind) {
        kind_ = kind;
    }
    virtual ~ReadingClause() = default;

    auto kind() const {
        return kind_;
    }

    bool isMatch() const {
        return kind() == Kind::kMatch;
    }

    bool isUnwind() const {
        return kind() == Kind::kUnwind;
    }

    bool isWith() const {
        return kind() == Kind::kWith;
    }

    virtual std::string toString() const = 0;

private:
    Kind            kind_;
};


class MatchClause final : public ReadingClause {
public:
    MatchClause(MatchPath *path, WhereClause *where, bool optional)
        : ReadingClause(Kind::kMatch) {
        path_.reset(path);
        where_.reset(where);
        isOptional_ = optional;
    }

    MatchPath* path() {
        return path_.get();
    }

    const MatchPath* path() const {
        return path_.get();
    }

    WhereClause* where() {
        return where_.get();
    }

    const WhereClause* where() const {
        return where_.get();
    }

    bool isOptional() const {
        return isOptional_;
    }

    std::string toString() const override;

private:
    bool                                isOptional_{false};
    std::unique_ptr<MatchPath>          path_;
    std::unique_ptr<WhereClause>        where_;
};


class UnwindClause final : public ReadingClause {
public:
    UnwindClause(Expression *expr, std::string *alias)
        : ReadingClause(Kind::kUnwind) {
        expr_.reset(expr);
        alias_.reset(alias);
    }

    Expression* expr() {
        return expr_.get();
    }

    const Expression* expr() const {
        return expr_.get();
    }

    std::string* alias() {
        return alias_.get();
    }

    const std::string* alias() const {
        return alias_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<Expression>         expr_;
    std::unique_ptr<std::string>        alias_;
};


class WithClause final : public ReadingClause {
public:
    explicit WithClause(YieldColumns *cols,
                       OrderFactors *orderFactors = nullptr,
                       Expression *skip = nullptr,
                       Expression *limit = nullptr,
                       WhereClause *where = nullptr,
                       bool distinct = false)
        : ReadingClause(Kind::kWith) {
        columns_.reset(cols);
        orderFactors_.reset(orderFactors);
        skip_.reset(skip);
        limit_.reset(limit);
        where_.reset(where);
        isDistinct_ = distinct;
    }

    YieldColumns* columns() {
        return columns_.get();
    }

    const YieldColumns* columns() const {
        return columns_.get();
    }

    OrderFactors* orderFactors() {
        return orderFactors_.get();
    }

    const OrderFactors* orderFactors() const {
        return orderFactors_.get();
    }

    Expression* skip() {
        return skip_.get();
    }

    const Expression* skip() const {
        return skip_.get();
    }

    Expression* limit() {
        return limit_.get();
    }

    const Expression* limit() const {
        return limit_.get();
    }

    WhereClause* where() {
        return where_.get();
    }

    const WhereClause* where() const {
        return where_.get();
    }

    bool isDistinct() const {
        return isDistinct_;
    }

    std::string toString() const override;

private:
    std::unique_ptr<YieldColumns>       columns_;
    std::unique_ptr<OrderFactors>       orderFactors_;
    std::unique_ptr<Expression>         skip_;
    std::unique_ptr<Expression>         limit_;
    std::unique_ptr<WhereClause>        where_;
    bool                                isDistinct_;
};


class MatchClauseList final {
public:
    void add(ReadingClause *clause) {
        clauses_.emplace_back(clause);
    }

    void add(MatchClauseList *list) {
        DCHECK(list != nullptr);
        for (auto &clause : list->clauses_) {
            clauses_.emplace_back(std::move(clause));
        }
        delete list;
    }

    auto clauses() && {
        return std::move(clauses_);
    }

private:
    std::vector<std::unique_ptr<ReadingClause>>     clauses_;
};


class MatchSentence final : public Sentence {
public:
    MatchSentence(MatchClauseList *clauses, MatchReturn *ret)
        : Sentence(Kind::kMatch) {
        clauses_ = std::move(*clauses).clauses();
        delete clauses;
        return_.reset(ret);
    }

    auto& clauses() {
        return clauses_;
    }

    const auto& clauses() const {
        return clauses_;
    }

    const MatchReturn* ret() const {
        return return_.get();
    }

    MatchReturn* ret() {
        return return_.get();
    }

    std::string toString() const override;

private:
    std::vector<std::unique_ptr<ReadingClause>>     clauses_;
    std::unique_ptr<MatchReturn>                    return_;
};

}   // namespace nebula

#endif  // PARSER_MATCHSENTENCE_H_
