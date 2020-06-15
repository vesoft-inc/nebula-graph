/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/test/ValidatorTest.h"
#include "planner/Admin.h"

namespace nebula {
namespace graph {


class ACLValidatorTest : public ValidatorTest {
public:
    void SetUp() override {
        ValidatorTest::SetUp();

        qCtx_ = buildContext();
    }

protected:
    void getPlan(const std::string &query, const ExecutionPlan* &plan) const {
        auto result = GQLParser().parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        auto validateResult = validator.validate();
        ASSERT_TRUE(validateResult.ok()) << validateResult;
        plan = qCtx_->plan();
        ASSERT_NE(plan, nullptr);
    }

    std::unique_ptr<QueryContext> qCtx_;
};

TEST_F(ACLValidatorTest, Simple) {
    constexpr char user[] = "shylock";
    constexpr char password[] = "123456";
    constexpr char newPassword[] = "654321";
    constexpr char roleTypeName[] = "ADMIN";
    constexpr char space[] = "test";
    // create user
    {
        const ExecutionPlan *plan = nullptr;
        getPlan(folly::stringPrintf("CREATE USER %s", user), plan);

        auto root = plan->root();
        ASSERT_EQ(root->kind(), PlanNode::Kind::kCreateUser);
        auto createUser = static_cast<const CreateUser*>(root);
        ASSERT_FALSE(createUser->ifNotExist());
        ASSERT_EQ(createUser->username(), user);
        ASSERT_EQ(createUser->password(), "");
    }
    {  // if not exists
        const ExecutionPlan *plan = nullptr;
        getPlan(folly::stringPrintf("CREATE USER IF NOT EXISTS %s", user), plan);

        auto root = plan->root();
        ASSERT_EQ(root->kind(), PlanNode::Kind::kCreateUser);
        auto createUser = static_cast<const CreateUser*>(root);
        ASSERT_TRUE(createUser->ifNotExist());
        ASSERT_EQ(createUser->username(), user);
        ASSERT_EQ(createUser->password(), "");
    }
    {  // with password
        const ExecutionPlan *plan = nullptr;
        getPlan(folly::stringPrintf("CREATE USER %s WITH PASSWORD \"%s\"",
                                     user,
                                     password),
                plan);

        auto root = plan->root();
        ASSERT_EQ(root->kind(), PlanNode::Kind::kCreateUser);
        auto createUser = static_cast<const CreateUser*>(root);
        ASSERT_FALSE(createUser->ifNotExist());
        ASSERT_EQ(createUser->username(), user);
        ASSERT_EQ(createUser->password(), password);
    }

    // drop user
    {
        const ExecutionPlan *plan = nullptr;
        getPlan(folly::stringPrintf("DROP USER %s",
                                    user),
                plan);

        auto root = plan->root();
        ASSERT_EQ(root->kind(), PlanNode::Kind::kDropUser);
        auto dropUser = static_cast<const DropUser*>(root);
        ASSERT_FALSE(dropUser->ifExist());
        ASSERT_EQ(dropUser->username(), user);
    }
    {  // if exits
        const ExecutionPlan *plan = nullptr;
        getPlan(folly::stringPrintf("DROP USER IF EXISTS %s",
                                    user),
                plan);

        auto root = plan->root();
        ASSERT_EQ(root->kind(), PlanNode::Kind::kDropUser);
        auto dropUser = static_cast<const DropUser*>(root);
        ASSERT_TRUE(dropUser->ifExist());
        ASSERT_EQ(dropUser->username(), user);
    }

    // update user
    {
        const ExecutionPlan *plan = nullptr;
        getPlan(folly::stringPrintf("ALTER USER %s WITH PASSWORD \"%s\"",
                                    user, password),
                plan);

        auto root = plan->root();
        ASSERT_EQ(root->kind(), PlanNode::Kind::kUpdateUser);
        auto updateUser = static_cast<const UpdateUser*>(root);
        ASSERT_EQ(updateUser->username(), user);
        ASSERT_EQ(updateUser->password(), password);
    }

    // change password
    {
        const ExecutionPlan *plan = nullptr;
        getPlan(folly::stringPrintf("CHANGE PASSWORD %s FROM \"%s\" TO \"%s\"",
                                    user, password, newPassword),
                plan);

        auto root = plan->root();
        ASSERT_EQ(root->kind(), PlanNode::Kind::kChangePassword);
        auto changePassword = static_cast<const ChangePassword*>(root);
        ASSERT_EQ(changePassword->username(), user);
        ASSERT_EQ(changePassword->password(), password);
        ASSERT_EQ(changePassword->newPassword(), newPassword);
    }

    // grant role
    {
        const ExecutionPlan *plan = nullptr;
        getPlan(folly::stringPrintf("GRANT ROLE %s ON %s TO %s",
                                     roleTypeName, space, user),
                plan);

        auto root = plan->root();
        ASSERT_EQ(root->kind(), PlanNode::Kind::kGrantRole);
        auto grantRole = static_cast<const GrantRole*>(root);
        ASSERT_EQ(grantRole->username(), user);
        ASSERT_EQ(grantRole->spaceName(), space);
        ASSERT_EQ(grantRole->role(), meta::cpp2::RoleType::ADMIN);
    }

    // revoke role
    {
        const ExecutionPlan *plan = nullptr;
        getPlan(folly::stringPrintf("REVOKE ROLE %s ON %s FROM %s",
                                    roleTypeName, space, user),
                plan);

        auto root = plan->root();
        ASSERT_EQ(root->kind(), PlanNode::Kind::kRevokeRole);
        auto revokeRole = static_cast<const RevokeRole*>(root);
        ASSERT_EQ(revokeRole->username(), user);
        ASSERT_EQ(revokeRole->spaceName(), space);
        ASSERT_EQ(revokeRole->role(), meta::cpp2::RoleType::ADMIN);
    }
}

}  // namespace graph
}  // namespace nebula
