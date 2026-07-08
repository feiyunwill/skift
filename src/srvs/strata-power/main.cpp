#include <karm/entry>

import Karm.Logger;
import Karm.App;
import Karm.Ipc;
import Karm.Core;
import Strata.Protos;

using namespace Karm;
using namespace Karm::Ref::Literals;

namespace Strata::Power {

struct Service {
    Opt<Ipc::Client> powerStateService;
    Opt<Ipc::Client> restartStateService;
};

struct PowerSession : Ipc::Session {
    Rc<Service> _service;

    explicit PowerSession(Sys::IpcConnection conn, Rc<Service> service)
        : Session(std::move(conn)), _service(service) {}

    Async::Task<IPower::ChangePowerState::Response> _handleChangePowerStateAsync(Ipc::Message& message, Async::CancellationToken ct) {
        auto request = co_try$(message.unpack<IPower::ChangePowerState>());
        co_return co_try$(_service->powerStateService.okOr(Error::notFound("no power state service available"))).callAsync(request, ct);
    }

    Async::Task<IPower::Restart::Response> _handleRestartAsync(Ipc::Message& message, Async::CancellationToken ct) {
        auto request = co_try$(message.unpack<IPower::Restart>());
        co_return co_try$(_service->restartStateService.okOr(Error::notFound("no restart service available"))).callAsync(request, ct);
    }

    Async::Task<> handleAsync(Ipc::Message& message, Async::CancellationToken ct) override {
        if (message.is<IPower::ChangePowerState>()) {
            co_return resp<IPower::ChangePowerState>(message, co_await _handleChangePowerStateAsync(message, ct));
        } else if (message.is<IPower::Restart>()) {
            co_return resp<IPower::Restart>(message, co_await _handleRestartAsync(message, ct));
        } else {
            co_return unsupported(message);
        };
    }
};

struct PowerHandler : Ipc::Handler {
    Rc<Service> _service;

    PowerHandler(Rc<Service> service) : _service(service) {}

    Async::Task<Rc<Ipc::Session>> acceptSessionAsync(Sys::IpcConnection conn, Ref::Url const&, Async::CancellationToken) override {
        co_return Ok(makeRc<PowerSession>(std::move(conn), _service));
    }
};

} // namespace Strata::Power

Async::Task<> entryPointAsync(Sys::Env&, Async::CancellationToken ct) {
    auto service = makeRc<Strata::Power::Service>();

    service->powerStateService = (co_await Ipc::Client::connectAsync("file:/services/strata-device/isa/debug-exit"_url, ct)).ok();
    service->powerStateService = (co_await Ipc::Client::connectAsync("file:/services/strata-device/isa/ps2/keyboard"_url, ct)).ok();

    auto handler = makeRc<Strata::Power::PowerHandler>(service);
    auto server = co_trya$(Ipc::Server::createAsync("ipc://strata-power"_url, handler));
    co_return co_await server.servAsync(ct);
}
