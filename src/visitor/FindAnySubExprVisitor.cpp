/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/FindAnySubExprVisitor.h"

namespace nebula {
namespace graph {

FindAnySubExprVisitor::FindAnySubExprVisitor(std::unordered_set<Expression*> &subExprs)
    : subExprs_(subExprs) {
    DCHECK(!subExprs_.empty());
}

void FindAnySubExprVisitor::visit(TypeCastingExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<TypeCastingExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
    expr->operand()->accept(this);
}

void FindAnySubExprVisitor::visit(UnaryExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<UnaryExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
    expr->operand()->accept(this);
}

void FindAnySubExprVisitor::visit(FunctionCallExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<FunctionCallExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
    for (const auto &arg : expr->args()->args()) {
        arg->accept(this);
        if (found_) return;
    }
}

void FindAnySubExprVisitor::visit(AggregateExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<AggregateExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
    expr->arg()->accept(this);
}

void FindAnySubExprVisitor::visit(ListExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<ListExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
    for (const auto &item : expr->items()) {
        item->accept(this);
        if (found_) return;
    }
}

void FindAnySubExprVisitor::visit(SetExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<SetExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
    for (const auto &item : expr->items()) {
        item->accept(this);
        if (found_) return;
    }
}

void FindAnySubExprVisitor::visit(MapExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<MapExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
    for (const auto &pair : expr->items()) {
        pair.second->accept(this);
        if (found_) return;
    }
}

void FindAnySubExprVisitor::visit(CaseExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<CaseExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
    if (expr->hasCondition()) {
        expr->condition()->accept(this);
        if (found_) return;
    }
    if (expr->hasDefault()) {
        expr->defaultResult()->accept(this);
        if (found_) return;
    }
    for (const auto &whenThen : expr->cases()) {
        whenThen.when->accept(this);
        if (found_) return;
        whenThen.then->accept(this);
        if (found_) return;
    }
}

void FindAnySubExprVisitor::visit(ConstantExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<ConstantExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
}

void FindAnySubExprVisitor::visit(EdgePropertyExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<EdgePropertyExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
}

void FindAnySubExprVisitor::visit(TagPropertyExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<TagPropertyExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
}

void FindAnySubExprVisitor::visit(InputPropertyExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<InputPropertyExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
}

void FindAnySubExprVisitor::visit(VariablePropertyExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<VariablePropertyExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
}

void FindAnySubExprVisitor::visit(SourcePropertyExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<SourcePropertyExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
}

void FindAnySubExprVisitor::visit(DestPropertyExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<DestPropertyExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
}

void FindAnySubExprVisitor::visit(EdgeSrcIdExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<EdgeSrcIdExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
}

void FindAnySubExprVisitor::visit(EdgeTypeExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<EdgeTypeExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
}

void FindAnySubExprVisitor::visit(EdgeRankExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<EdgeRankExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
}

void FindAnySubExprVisitor::visit(EdgeDstIdExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<EdgeDstIdExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
}

void FindAnySubExprVisitor::visit(UUIDExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<UUIDExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
}

void FindAnySubExprVisitor::visit(VariableExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<VariableExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
}

void FindAnySubExprVisitor::visit(VersionedVariableExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<VersionedVariableExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
}

void FindAnySubExprVisitor::visit(LabelExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<LabelExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
}

void FindAnySubExprVisitor::visit(VertexExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<VertexExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
}

void FindAnySubExprVisitor::visit(EdgeExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<EdgeExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
}

void FindAnySubExprVisitor::visit(ColumnExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<ColumnExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
}

void FindAnySubExprVisitor::visitBinaryExpr(BinaryExpression *expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<BinaryExpression*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
    expr->left()->accept(this);
    if (found_) return;
    expr->right()->accept(this);
}

void FindAnySubExprVisitor::checkExprKind(const Expression *expr, const Expression *sub_expr) {
    if (expr == sub_expr) {
        found_ = true;
        continue_ = false;
    } else if (expr == nullptr || sub_expr == nullptr) {
        continue_ = false;
    } else if (expr->kind() != sub_expr->kind()) {
       continue_ = false;
    }
}

}   // namespace graph
}   // namespace nebula
