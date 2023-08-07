#include "resolve_user_defined_function_adjustment.h"
#include <db/exception/plan_exception.h>
#include <db/plan/logical/node/user_defined_node.h>
#include <fmt/core.h>

using namespace db::plan::logical;

void ResolveUserDefinedFunctionAdjustment::apply(std::unique_ptr<NodeInterface> &node)
{
    if (node->is_unary())
    {
        if (typeid(*node) == typeid(UserDefinedNode))
        {
            auto *user_defined_function_node = reinterpret_cast<UserDefinedNode *>(node.get());
            for (auto &user_defined_function : user_defined_function_node->user_defined_functions())
            {
                if (this->_database.is_user_defined_function(user_defined_function->function_name()) == false)
                {
                    throw exception::PlanningException{
                        fmt::format("UDF '{}' does not exist.", user_defined_function->function_name())};
                }
                const auto &udf_descriptor =
                    this->_database.user_defined_function(user_defined_function->function_name());
                user_defined_function->descriptor(std::ref(udf_descriptor));

                if (user_defined_function->children().size() != udf_descriptor.input_parameters().size())
                {
                    throw exception::PlanningException{
                        fmt::format("Input of UDF '{}' does not match. Expected {} parameters, given {} parameters.",
                                    udf_descriptor.name(), udf_descriptor.input_parameters().size(),
                                    user_defined_function->children().size())};
                }
            }
        }

        this->apply(reinterpret_cast<UnaryNode *>(node.get())->child());
    }
    else if (node->is_binary())
    {
        auto *binary_node = reinterpret_cast<BinaryNode *>(node.get());
        this->apply(binary_node->left_child());
        this->apply(binary_node->right_child());
    }
}
