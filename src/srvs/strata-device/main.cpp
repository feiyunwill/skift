#include <hal/io.h>
#include <karm/entry>

import Karm.App;
import Karm.Core;
import Karm.Ipc;
import Karm.Logger;
import Karm.Sys.Skift;
import Karm.Sys;

import Hjert.Api;
import Strata.Device;
import Strata.Protos;

using namespace Karm;
using namespace Karm::Literals;
using namespace Karm::Ref::Literals;

namespace Strata::Device {

struct IsaRootBus : Node {
    Str name() override {
        return "isa"s;
    }

    Res<> init() override {
        auto i18042port = try$(PortIo::open({0x60, 0x8}));
        try$(attach(makeRc<Ps2::I8042>(i18042port)));

        auto cmosPort = try$(PortIo::open({0x70, 0x2}));
        try$(attach(makeRc<Cmos::Cmos>(cmosPort)));

        return Ok();
    }
};

struct RootBus : Node {
    Opt<Ipc::Server&> _server = NONE;

    RootBus() {}

    Str name() override {
        return "root"s;
    }

    Res<> init() override {
        try$(attach(makeRc<IsaRootBus>()));
        return Ok();
    }

    Res<> bubble(App::Event& e) override {
        if (auto& [server] = _server) {
            if (auto me = e.is<App::MouseEvent>()) {
                server.broadcast<App::MouseEvent>(*me);
                e.accept();
            } else if (auto ke = e.is<App::KeyboardEvent>()) {
                server.broadcast<App::KeyboardEvent>(*ke);
                e.accept();
            }
        }

        return Node::bubble(e);
    }

    Async::Task<> dispatchAsync(Async::CancellationToken ct) {
        auto pipe = co_try$(Sys::Skift::PipeFd::create());
        Vec<Hj::Irq> irqs = {};
        irqs.ensure(16);
        for (usize i = 0; i < 16; i++) {
            auto irq = co_try$(Hj::Irq::create(Hj::ROOT, i));
            co_try$(irq.bind(pipe->_pipe.cap()));
            irqs.pushBack(std::move(irq));
        }

        while (true) {
            Array<u64, 32> incoming;
            auto n = co_trya$(Sys::globalSched().readAsync(pipe, mutBytes(incoming), ct)) / sizeof(u64);
            for (auto irq : sub(incoming, 0, n)) {
                auto e = App::makeEvent<IrqEvent>(irq);
                auto res = event(*e);
                if (not res)
                    logError("an error occurred during irq{} dispatch: {}", irq, res);
                co_try$(irqs[irq].eoi());
            }
        }
    }
};

struct DeviceHandler : Ipc::Handler {
    Rc<Node> _root;

    DeviceHandler(Rc<Node> root) : _root(root) {}

    Res<Rc<Node>> _resolvePath(Ref::Url const& url) {
        Rc<Node> current = _root;
        for (auto& s : url.path.segments()) {
            bool found = false;
            for (auto& c : current->_children) {
                if (c->name() == s) {
                    found = true;
                    current = c;
                    break;
                }
            }
            if (not found)
                return Error::notFound();
        }
        return Ok(current);
    }

    Async::Task<Rc<Ipc::Session>> acceptSessionAsync(Sys::IpcConnection connection, Ref::Url const& url, Async::CancellationToken) override {
        auto node = co_try$(_resolvePath(url));
        co_return Ok(node->open(std::move(connection)));
    }
};

} // namespace Strata::Device

Async::Task<> entryPointAsync(Sys::Env&, Async::CancellationToken ct) {
    auto root = makeRc<Strata::Device::RootBus>();
    auto handler = makeRc<Strata::Device::DeviceHandler>(root);
    auto server = co_trya$(Ipc::Server::createAsync("ipc://strata-device"_url, handler));
    root->_server = server;

    logInfo("devices: building device tree...");
    co_try$(root->init());

    co_return co_await Async::join(
        server.servAsync(ct),
        root->dispatchAsync(ct)
    );
}
