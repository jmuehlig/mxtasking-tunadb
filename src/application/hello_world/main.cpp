#include <iostream>
#include <mx/tasking/runtime.h>

class HelloWorldTask : public mx::tasking::TaskInterface
{
public:
    constexpr HelloWorldTask() = default;
    ~HelloWorldTask() override = default;

    mx::tasking::TaskResult execute(const std::uint16_t worker_id) override
    {
        std::cout << "Hello World" << std::endl;

        // Stop MxTasking runtime after this task.
        return mx::tasking::TaskResult::make_stop(worker_id);
    }
};

int main()
{
    // Define which cores will be used (1 core here).
    // The core set will map from channel id to physical core id,
    // e.g., [0] = 1 will map the first channel to core id 1
    const auto cores = mx::util::core_set::build(1);

    { // Scope for the MxTasking runtime.

        // Create a runtime for the given cores.
        auto _ = mx::tasking::runtime_guard{cores};

        // Create an instance of the HelloWorldTask with the current core as first
        // parameter. The worker id is required for memory allocation.
        auto *hello_world_task = mx::tasking::runtime::new_task<HelloWorldTask>(0U);

        // Annotate the task to run on the first worker.
        hello_world_task->annotate(std::uint16_t(0U));

        // Schedule the task.
        mx::tasking::runtime::spawn(*hello_world_task);
    }

    return 0;
}