//#include "analyzer.h"
//#include <iostream>
//
// using namespace flounder;
//
// Metadata Analyzer::analyze(const Program &program)
//{
//    auto main_block = std::make_shared<Block>(this->_next_block_id++, std::vector<Node *>{program.body().lines()});
//    this->analyze(main_block);
//    main_block->print();
//
//    auto metadata = Metadata{std::move(main_block)};
//
//    return metadata;
//}
//
// void Analyzer::analyze(const std::shared_ptr<Block> &block)
//{
//    for (auto i = 0U; i < block->nodes().size(); ++i)
//    {
//        auto *node = block->nodes()[i];
//        if (*node == NodeType::JMP || *node == NodeType::JE || *node == NodeType::JNE || *node == NodeType::JG ||
//            *node == NodeType::JGE || *node == NodeType::JL || *node == NodeType::JLE)
//        {
//            auto *jmp_label = reinterpret_cast<LabelNode *>(node->child(0U));
//
//            for (auto j = i + 1U; j < block->nodes().size(); ++j)
//            {
//                auto *inner_node = block->nodes()[j];
//                if (*inner_node == NodeType::SECTION)
//                {
//                    auto *section_label = reinterpret_cast<LabelNode *>(inner_node->child(0U));
//                    if (section_label->name() == jmp_label->name())
//                    {
//                        auto [first, second] = block->split(i, j);
//                        auto first_block = std::make_shared<Block>(this->_next_block_id++, std::move(first));
//                        auto second_block = std::make_shared<Block>(this->_next_block_id++, std::move(second));
//                        first_block->connect(second_block);
//                        block->connect(first_block);
//                        block->connect(second_block);
//
//                        this->analyze(first_block);
//                        this->analyze(second_block);
//                        return;
//                    }
//                }
//            }
//        }
//    }
//}
//
// std::pair<std::vector<Node *>, std::vector<Node *>> Block::split(const std::size_t first_from_index,
//                                                                 const std::size_t second_from_index)
//{
//    auto first = std::vector<Node *>{};
//    auto second = std::vector<Node *>{};
//
//    std::move(this->_nodes.begin() + first_from_index + 1U, this->_nodes.end(), std::back_inserter(first));
//    this->_nodes.erase(this->_nodes.begin() + first_from_index + 1U, this->_nodes.end());
//
//    std::move(first.begin() + (second_from_index - first_from_index - 1U), first.end(), std::back_inserter(second));
//    first.erase(first.begin() + (second_from_index - first_from_index - 1U), first.end());
//
//    return std::make_pair(std::move(first), std::move(second));
//}
//
// void Block::print(const std::uint32_t depth)
//{
//    std::cout << fmt::format("{}Block #{} ", std::string(depth * 4, ' '), this->_id);
//    if (this->_nodes.empty() == false)
//    {
//        auto first = *this->_nodes.front() == NodeType::BRANCH_BEGIN_MARKER ? this->_nodes[1U]->to_string()
//                                                                            : this->_nodes.front()->to_string();
//        auto last = *this->_nodes.back() == NodeType::BRANCH_END_MARKER
//                        ? this->_nodes[this->_nodes.size() - 2U]->to_string()
//                        : this->_nodes.back()->to_string();
//
//        std::cout << fmt::format("({} -- {})", first, last);
//    }
//    else
//    {
//        std::cout << "(empty)";
//    }
//    std::cout << std::endl;
//
//    for (const auto &out : this->_outgoing)
//    {
//        out->print(depth + 1U);
//    }
//}