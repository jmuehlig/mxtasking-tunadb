//#pragma once
//
//#include <flounder/program.h>
//#include <memory>
//#include <unordered_map>
//#include <vector>
//
// namespace flounder {
// class Block
//{
// public:
//    Block(const std::uint64_t id, std::vector<Node *> &&nodes) : _id(id), _nodes(std::move(nodes)) {}
//    ~Block() = default;
//
//    void connect(std::shared_ptr<Block> block) { _outgoing.emplace_back(std::move(block)); }
//
//    [[nodiscard]] const std::vector<Node *> &nodes() const noexcept { return _nodes; }
//
//    std::pair<std::vector<Node *>, std::vector<Node *>> split(const std::size_t from_index,
//                                                              const std::size_t first_block_end_index);
//
//    void print(std::uint32_t depth = 0U);
//
// private:
//    const std::uint64_t _id;
//    std::vector<Node *> _nodes;
//    std::vector<std::shared_ptr<Block>> _outgoing;
//};
//
// class Metadata
//{
// public:
//    Metadata(std::shared_ptr<Block> &&block) : _block(std::move(block)) {}
//
// private:
//    std::shared_ptr<Block> _block;
//};
//
// class Analyzer
//{
// public:
//    constexpr Analyzer() = default;
//    ~Analyzer() = default;
//
//    [[nodiscard]] Metadata analyze(const Program &program);
//
// private:
//    std::uint64_t _next_block_id{0U};
//
//    void analyze(const std::shared_ptr<Block> &block);
//};
//} // namespace flounder