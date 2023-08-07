//#pragma once
//#include "annotation.h"
//#include "node.h"
//#include <cstdint>
//#include <fmt/core.h>
//#include <optional>
//#include <utility>
//#include <vector>
//
// namespace flounder {
// class LoopAnnotation final : public Annotation
//{
// public:
//    constexpr LoopAnnotation() noexcept : Annotation(Annotation::Type::Loop) {}
//    constexpr LoopAnnotation(BranchBeginMarkerNode *begin, BranchEndMarkerNode *end) noexcept
//        : Annotation(Annotation::Type::Loop), _begin_marker(begin), _end_marker(end)
//    {
//    }
//    constexpr LoopAnnotation(BranchBeginMarkerNode *begin, BranchEndMarkerNode *end, const bool is_unrolled) noexcept
//        : Annotation(Annotation::Type::Loop), _begin_marker(begin), _end_marker(end), _is_unrolled(is_unrolled)
//    {
//    }
//
//    constexpr LoopAnnotation(LoopAnnotation &&) noexcept = default;
//    LoopAnnotation(const LoopAnnotation &) = default;
//
//    ~LoopAnnotation() noexcept override = default;
//
//    LoopAnnotation &operator=(LoopAnnotation &&) noexcept = default;
//
//    [[nodiscard]] BranchBeginMarkerNode *begin_marker() const noexcept { return _begin_marker; }
//    [[nodiscard]] BranchEndMarkerNode *end_marker() const noexcept { return _end_marker; }
//    [[nodiscard]] std::optional<std::uint64_t> count_iterations() const noexcept { return _count_iterations; }
//    [[nodiscard]] std::optional<std::uint16_t> iteration_data_size() const noexcept { return _iteration_data_size; }
//    [[nodiscard]] std::optional<std::pair<std::uint16_t, std::uint16_t>> iteration_accessed_data_size() const noexcept
//    {
//        return _iteration_accessed_data_size;
//    }
//    [[nodiscard]] bool is_unrolled() const noexcept { return _is_unrolled; }
//
//    void begin_marker(BranchBeginMarkerNode *begin) noexcept { _begin_marker = begin; }
//    void end_marker(BranchEndMarkerNode *end) noexcept { _end_marker = end; }
//    void count_iterations(const std::uint64_t count_iterations) noexcept { _count_iterations = count_iterations; }
//    void iteration_data_size(const std::uint16_t iteration_data_size) noexcept
//    {
//        _iteration_data_size = iteration_data_size;
//    }
//    void iteration_accessed_data_size(
//        const std::pair<std::uint16_t, std::uint16_t> iteration_accessed_data_size) noexcept
//    {
//        _iteration_accessed_data_size = iteration_accessed_data_size;
//    }
//
//    [[nodiscard]] std::string to_string() const override
//    {
//        auto data = std::vector<std::string>{};
//        if (_count_iterations.has_value())
//        {
//            data.emplace_back(fmt::format("iter={}", _count_iterations.value()));
//        }
//        if (_iteration_data_size.has_value())
//        {
//            data.emplace_back(fmt::format("mem/iter={}", _iteration_data_size.value()));
//        }
//        if (_iteration_accessed_data_size.has_value())
//        {
//            data.emplace_back(fmt::format("access/iter=[{},{}]", std::get<0>(_iteration_accessed_data_size.value()),
//                                          std::get<1>(_iteration_accessed_data_size.value())));
//        }
//
//        return fmt::format("@loop({})", fmt::join(std::move(data), ","));
//    }
//
// private:
//    /// Marker where the branch starts.
//    BranchBeginMarkerNode *_begin_marker{nullptr};
//
//    /// Marker where the branch ends.
//    BranchEndMarkerNode *_end_marker{nullptr};
//
//    /// Estimated number of iterations of the loop.
//    std::optional<std::uint64_t> _count_iterations{std::nullopt};
//
//    /// Size of the data of each iteration.
//    std::optional<std::uint16_t> _iteration_data_size;
//
//    /// Size of the accessed data of each iteration.
//    std::optional<std::pair<std::uint16_t, uint16_t>> _iteration_accessed_data_size;
//
//    bool _is_unrolled{false};
//};
//
// class BranchAnnotation final : public Annotation
//{
// public:
//    constexpr BranchAnnotation() noexcept : Annotation(Annotation::Type::Branch) {}
//
//    constexpr BranchAnnotation(BranchBeginMarkerNode *begin_marker, BranchEndMarkerNode *end_marker) noexcept
//        : Annotation(Annotation::Type::Branch), _begin_marker(begin_marker), _end_marker(end_marker)
//    {
//    }
//
//    constexpr BranchAnnotation(BranchAnnotation &&) noexcept = default;
//
//    ~BranchAnnotation() noexcept override = default;
//
//    BranchAnnotation &operator=(BranchAnnotation &&) noexcept = default;
//
//    [[nodiscard]] BranchBeginMarkerNode *begin_marker() const noexcept { return _begin_marker; }
//    [[nodiscard]] BranchEndMarkerNode *end_marker() const noexcept { return _end_marker; }
//    [[nodiscard]] float likeliness() const noexcept { return _likeliness; }
//
//    void likeliness(const float likeliness) noexcept { _likeliness = likeliness; }
//    void mark_likely() noexcept { _likeliness = .95; }
//    void mark_unlikely() noexcept { _likeliness = .05; }
//
//    [[nodiscard]] std::string to_string() const override
//    {
//        return fmt::format("@branch(likeliness={:.2f})", _likeliness);
//    }
//
// private:
//    /// Marker where the branch starts.
//    BranchBeginMarkerNode *_begin_marker{nullptr};
//
//    /// Marker where the branch ends.
//    BranchEndMarkerNode *_end_marker{nullptr};
//
//    /// Chance that the branch is taken.
//    float _likeliness{.5};
//};
//
// class PrefetchAnnotation final : public Annotation
//{
// public:
//    constexpr PrefetchAnnotation(const std::uint16_t distance) noexcept
//        : Annotation(Annotation::Type::Prefetch), _distance(distance)
//    {
//    }
//
//    ~PrefetchAnnotation() noexcept override = default;
//
//    [[nodiscard]] std::uint16_t distance() const noexcept { return _distance; }
//
//    [[nodiscard]] std::string to_string() const override { return fmt::format("@prefetch(distance={}", _distance); }
//
// private:
//    const std::uint16_t _distance;
//};
//
// class ScanPrefetchAnnotation final : public Annotation
//{
// public:
//    constexpr ScanPrefetchAnnotation(PrefetchNode *prefetch_node, const std::uint32_t distance,
//                                     const std::uint32_t record_size,
//                                     const std::pair<std::uint32_t, std::uint32_t> min_max_accessed_bytes) noexcept
//        : Annotation(Annotation::Type::ScanPrefetch), _prefetch_node(prefetch_node), _distance(distance),
//          _record_size(record_size), _min_max_accessed_bytes(min_max_accessed_bytes)
//    {
//    }
//
//    ~ScanPrefetchAnnotation() noexcept override = default;
//
//    [[nodiscard]] PrefetchNode *prefetch_node() const noexcept { return _prefetch_node; }
//    [[nodiscard]] std::uint32_t distance() const noexcept { return _distance; }
//    [[nodiscard]] std::uint32_t record_size() const noexcept { return _record_size; }
//    [[nodiscard]] std::pair<std::uint32_t, std::uint32_t> accessed_bytes() const noexcept
//    {
//        return _min_max_accessed_bytes;
//    }
//
//    [[nodiscard]] std::string to_string() const override { return "@scan-prefetch"; }
//    void distance(const std::uint32_t distance) noexcept { _distance = distance; }
//
// private:
//    PrefetchNode *_prefetch_node;
//    std::uint32_t _distance;
//    const std::uint32_t _record_size;
//    const std::pair<std::uint32_t, std::uint32_t> _min_max_accessed_bytes;
//};
//} // namespace flounder