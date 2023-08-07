#include "single_command_client.h"
#include "client_console.h"
#include <mx/system/thread.h>
#include <thread>

using namespace db::io;

mx::tasking::TaskResult StartSingleCommandClientTask::execute(const std::uint16_t /*worker_id*/)
{
    auto *client_thread = new std::thread{
        [port = this->_port, command = std::move(this->_command), output_file = std::move(this->_output_file)] {
            auto console = ClientConsole{"localhost", port, std::optional{output_file}};
            if (console.connect())
            {
                console.execute(std::string{command});
                console.execute(".stop");
            }
        }};
    mx::system::thread::name(*client_thread, "db::query_exec");
    client_thread->detach();

    return mx::tasking::TaskResult::make_remove();
}