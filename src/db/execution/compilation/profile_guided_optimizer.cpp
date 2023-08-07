#include "profile_guided_optimizer.h"

using namespace db::execution::compilation;

void ProfileGuidedOptimizer::end_profiling(const std::uint32_t count_profiled_tuples)
{
    /// Aggregate profiling results.
    const auto profiling_end = this->_performance_counter.now();
    const auto performance_value = perf::Counter::get(this->_profiling_start, profiling_end);
    this->_aggregator.add(count_profiled_tuples, performance_value);

    /// Stop after fixed number of sampled records.
    const auto is_sampling_finished = this->_aggregator.sampled_records() >= 100000U;

    /// When sampled enough, either optimize again or choose best version.
    if (is_sampling_finished)
    {
        this->_scores[this->_current_version] = this->_aggregator.value();
        this->_aggregator.clear();

        if (this->_current_version == this->_program.capacity())
        {
            /// Choose the best performing executable.
            this->apply_best_version();
            this->_is_optimizing = false;
        }
        else
        {
            this->optimize(this->_program.flounder());
            this->_program.translate(this->_current_version++, this->_compiler);
        }
    }
}

void ProfileGuidedOptimizer::optimize(flounder::Program & /*program*/)
{
    //    if (this->_prefetch_annotation == nullptr) [[unlikely]]
    //    {
    //        const auto &annotations = _program.flounder().annotations();
    //        const auto scan_prefetch_annotation =
    //            std::find_if(annotations.begin(), annotations.end(), [](const auto &annotation) {
    //                return annotation->type() == flounder::Annotation::Type::ScanPrefetch;
    //            });
    //        if (scan_prefetch_annotation == annotations.end()) [[unlikely]]
    //        {
    //            _is_optimizing = false;
    //            return;
    //        }
    //
    //        _prefetch_annotation = reinterpret_cast<flounder::ScanPrefetchAnnotation
    //        *>(scan_prefetch_annotation->get());
    //    }
    //
    //    const auto new_distance = this->_prefetch_annotation->distance() + 1U;
    //    this->_prefetch_annotation->distance(new_distance);
    //
    //    auto *constant_node = reinterpret_cast<flounder::ConstantNode *>(
    //        this->_prefetch_annotation->prefetch_node()->child(0U)->child(0U)->child(1U));
    //    constant_node->value(std::int32_t(new_distance * this->_prefetch_annotation->record_size() +
    //                                      std::get<0>(this->_prefetch_annotation->accessed_bytes())));
}

void ProfileGuidedOptimizer::apply_best_version()
{
    const auto best_score_iterator = std::min_element(this->_scores.begin(), this->_scores.end());
    const auto best_score_index = std::distance(this->_scores.begin(), best_score_iterator);

    const auto callback = best_score_index == 0U ? this->_program.executable().callback()
                                                 : this->_program.version(best_score_index - 1U).callback();
    this->_program.callback(callback);
}