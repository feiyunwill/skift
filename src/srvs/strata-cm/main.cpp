#include <hal/mem.h>
#include <karm/macros>
#include <vaerk-handover/spec.h>

import Karm.Ipc;
import Karm.Logger;
import Karm.Ref;
import Karm.Sys;
import Karm.Sys.Skift;

import Abi.SysV;
import Hjert.Api;

import Strata.Protos;
import Strata.Cm;

using namespace Karm;
using namespace Karm::Literals;
using namespace Karm::Ref::Literals;
using namespace Karm::Fmt::Literals;

static constexpr bool DEBUG_COMPONENT = false;

namespace Strata::Cm {

struct ComponentManager {
    struct Component;

    struct Namespace {
        struct Entry {
            Ref::Path prefix;
            Weak<Component> component;
            Ref::Path target;
        };

        struct Resolved {
            Rc<Component> component;
            Ref::Path path;
        };

        Vec<Entry> _entries;

        Res<Ref::Path> remap(Ref::Url url) {
            if (url.scheme == "ipc")
                return Ok(Ref::Path::parse("/services/{}"_f(url.host)));

            if (url.scheme == "file")
                return Ok(url.path);

            return Error::notFound("unknow schema");
        }

        Res<Resolved> resolve(Ref::Path path) {
            Opt<Entry&> maybeBest = NONE;
            usize bestLen = 0;
            for (auto& e : _entries) {
                if (e.prefix.len() >= bestLen and e.prefix.parentOf(path)) {
                    maybeBest = e;
                    bestLen = e.prefix.len();
                }
            }

            if (auto& [best] = maybeBest) {
                auto component = try$(best.component.upgrade().okOr(Error::notFound()));
                auto segments = next(path.segments(), best.prefix.len());
                auto path = best.target / Ref::Path{segments};
                return Ok(Resolved(component, path));
            }

            return Error::notFound();
        }

        Res<> connect(Rc<Sys::Fd> fd, Ref::Url url) {
            url.path.normalize();
            auto path = try$(remap(url));
            auto resolved = try$(resolve(path));
            return resolved.component->incoming({fd, "file:"_url / resolved.path});
        }

        Async::Task<Vec<Sys::DirEntry>> listAsync(Ref::Url url, Async::CancellationToken ct) {
            Vec<Sys::DirEntry> result = {};
            auto path = co_try$(remap(url));
            for (auto& e : _entries) {
                if (not path.parentOf(e.prefix) or e.prefix.len() <= path.len())
                    continue;

                auto name = e.prefix.segments()[path.len()];

                bool duplicated =
                    iter(result) |
                    Any([&](Sys::DirEntry const& entry) {
                        return entry.name == name;
                    });

                if (duplicated)
                    continue;

                result.pushBack({
                    e.prefix.segments()[path.len()],
                    Sys::Type::DIR,
                });
            }

            auto maybeResolved = resolve(path).ok();
            if (auto& [resolved] = maybeResolved) {
                auto resolvedUrl = "file:"_url / resolved.path;
                auto maybeListing = (co_await resolved.component->listAsync(resolvedUrl, ct)).ok();
                if (auto& [listing] = maybeListing) {

                    for (auto& e : listing) {
                        bool duplicated =
                            iter(result) |
                            Any([&](Sys::DirEntry const& entry) {
                                return entry.name == e.name;
                            });

                        if (duplicated)
                            continue;
                        result.pushBack(e);
                    }
                }
            }

            co_return Ok(std::move(result));
        }

        void mount(Ref::Path prefix, Weak<Component> component, Ref::Path target = "/"_path) {
            _entries.pushBack({prefix, component, target});
        }
    };

    struct Component {
        ComponentManager& _componentManager;
        String _id;
        Hj::Job _job;
        Sys::IpcConnection _connection;
        Rc<Namespace> _namespace;

        Component(ComponentManager& cm, String id, Hj::Job job, Sys::IpcConnection connection, Rc<Namespace> namespace_)
            : _componentManager(cm),
              _id(id),
              _job(std::move(job)),
              _connection(std::move(connection)),
              _namespace(namespace_) {}

        Res<> _handleConnect(Ipc::Message& msg) {
            auto [maybeFd, url] = try$(msg.unpack<ICm::Open>());
            logDebugIf(DEBUG_COMPONENT, "'{}' requested connection to '{}'", _id, url);
            auto fd = try$(maybeFd.okOr(Error::invalidData("connect without fd")));

            auto res = _namespace->connect(fd, url);
            if (not res) {
                Sys::IpcConnection conn{fd};
                (void)Ipc::send(conn, Ipc::SEQ_HELLO, res.none());
            }

            return res;
        }

        Async::Task<ICm::List::Response> _handleListAsync(Ipc::Message& msg, Async::CancellationToken ct) {
            auto [url] = co_try$(msg.unpack<ICm::List>());
            co_return co_await _namespace->listAsync(url, ct);
        }

        Res<> _handleLaunch(Ipc::Message& msg) {
            auto [url] = try$(msg.unpack<ICm::Launch>());
            logDebugIf(DEBUG_COMPONENT, "'{}' requested launch of '{}'", _id, url);
            try$(_componentManager.start(url.host.str(), _namespace));
            return Ok();
        }

        Async::Task<> runAsync(Async::CancellationToken ct) {
            logDebugIf(DEBUG_COMPONENT, "component '{}' attached", _id);
            while (true) {
                co_try$(ct.errorIfCanceled());
                auto msg = co_trya$(Ipc::recvAsync(_connection, ct));
                if (msg->is<ICm::Open>()) {
                    (void)_handleConnect(*msg);
                } else if (msg->is<ICm::List>()) {
                    (void)Ipc::resp<ICm::List>(_connection, *msg, co_await _handleListAsync(*msg, ct));
                } else if (msg->is<ICm::Launch>()) {
                    (void)_handleLaunch(*msg);
                }
            }
        }

        template <typename T>
        Res<> notify(T const& payload) {
            return Ipc::send<T>(_connection, Ipc::SEQ_EVENT, payload);
        }

        Res<> incoming(ICm::Incoming const& incoming) {
            logDebugIf(DEBUG_COMPONENT, "notifying '{}' of incoming connection", _id);
            return notify(incoming);
        }

        Async::Task<Ipc::Client> connectAsync(Ref::Url url, Async::CancellationToken ct) {
            auto [clientFd, serverFd] = co_try$(Sys::Skift::ChannelFd::create(""));
            co_try$(incoming({serverFd, url}));
            co_return co_await Ipc::Client::connectAsync(Sys::IpcConnection{clientFd, true}, url, ct);
        }

        Async::Task<Vec<Sys::DirEntry>> listAsync(Ref::Url url, Async::CancellationToken ct) {
            auto client = co_trya$(connectAsync(url, ct));
            co_return co_await client.callAsync(IFs::ReadDir{}, ct);
        }

        bool operator==(Component const& other) const {
            return this == &other;
        }
    };

    Vec<Rc<Component>> _active;
    Async::Promise<> _exit;
    Async::Cancellation _cancellation;

    ComponentManager() {}

    ~ComponentManager() {
        _cancellation.cancel();
    }

    void shutdown(Rc<Component> component) {
        logDebugIf(DEBUG_COMPONENT, "shutting down component '{}'", component->_id);

        _active.removeAll(component);

        if (_active.len() == 0) {
            logDebugIf(DEBUG_COMPONENT, "no active components, exiting...");
            _exit.resolve(Ok());
        }
    }

    Res<Rc<Component>> start(Str id, Rc<Namespace> ns) {
        logDebugIf(DEBUG_COMPONENT, "starting '{}'", id);

        auto [fd0, fd1] = try$(Sys::Skift::ChannelFd::create(id));
        auto job = try$(runElf(id, fd0));

        auto component = makeRc<Component>(*this, id, std::move(job), Sys::IpcConnection{fd1}, ns);
        _active.pushBack(component);

        Async::detach(component->runAsync(_cancellation.token()), [this, component](auto const&...) {
            shutdown(component);
        });

        return Ok(component);
    }

    Async::Task<> runAsync() {
        logInfo("initializing system services...");

        auto ns = makeRc<Namespace>();

        ns->mount("/services/strata-device"_path, co_try$(start("strata-device"s, ns)));
        ns->mount("/"_path, co_try$(start("strata-fs"s, ns)));
        ns->mount("/services/strata-input"_path, co_try$(start("strata-input"s, ns)));
        ns->mount("/services/strata-shell"_path, co_try$(start("strata-shell"s, ns)));

        co_return co_await _exit.future();
    }
};

} // namespace Strata::Cm

Async::Task<> entryPointAsync(Sys::Env&, Async::CancellationToken) {
    co_try$(Hj::Task::self().label("strata-cm"));
    Strata::Cm::ComponentManager cm{};
    co_return co_await cm.runAsync();
}

void __panicHandler(PanicKind kind, char const* msg, usize len);

extern "C" void __entryPoint(usize rawHandover) {
    Abi::SysV::init();
    registerPanicHandler(__panicHandler);

    Sys::Skift::globalPayload = reinterpret_cast<Handover::Payload*>(rawHandover);

    char const* argv[] = {"strata-cm", nullptr};
    char const* envp[] = {nullptr};
    Sys::Env env{
        1,
        argv,
        envp,
        "file:"_url,
    };
    Async::Cancellation cancellation;
    auto res = Sys::run(entryPointAsync(env, cancellation.token()));

    auto self = Hj::Task::self();

    if (not res) {
        logError("{}: {}", argv[0], res.none().msg());
        self.crash().unwrap();
    }

    Abi::SysV::fini();
    self.ret().unwrap();
    unreachable();
}
