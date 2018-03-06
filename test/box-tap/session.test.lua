#!/usr/bin/env tarantool

local tap = require('tap')
local test = tap.test('session')
local fiber = require('fiber')

box.cfg{
    listen = os.getenv('LISTEN');
    log="tarantool.log";
}

local uri = require('uri').parse(box.cfg.listen)
local HOST, PORT = uri.host or 'localhost', uri.service
session = box.session
space = box.schema.space.create('tweedledum')
index = space:create_index('primary', { type = 'hash' })

test:plan(55)

--
-- Check that can start from Lua only either console or REPL.
--
local ok, err = pcall(box.internal.session.create, 100, "binary")
test:is(err, "Can not start non-console or non-REPL session from Lua", "bad session type")
ok, err = pcall(box.internal.session.create, 100, "applier")
test:is(err, "Can not start non-console or non-REPL session from Lua", "bad session type")

---
--- Check that Tarantool creates ADMIN session for #! script
---
test:ok(session.exists(session.id()), "session is created")
test:isnil(session.peer(session.id()), "session.peer")
ok, err = pcall(session.exists)
test:is(err, "session.exists(sid): bad arguments", "exists bad args #1")
ok, err = pcall(session.exists, 1, 2, 3)
test:is(err, "session.exists(sid): bad arguments", "exists bad args #2")
test:ok(not session.exists(1234567890), "session doesn't exist")

-- check session.id()
test:ok(session.id() > 0, "id > 0")
failed = false
local f = fiber.create(function() failed = session.id() == 0 end)
while f:status() ~= 'dead' do fiber.sleep(0) end
test:ok(not failed, "session not broken")
test:is(session.peer(), session.peer(session.id()), "peer() == peer(id())")

-- check on_connect/on_disconnect triggers
function noop() end
test:is(type(session.on_connect(noop)), "function", "type of trigger noop on_connect")
test:is(type(session.on_disconnect(noop)), "function", "type of trigger noop on_disconnect")

-- check it's possible to reset these triggers
function fail() error('hear') end
test:is(type(session.on_connect(fail, noop)), "function", "type of trigger fail, noop on_connect")
test:is(type(session.on_disconnect(fail, noop)), "function", "type of trigger fail, noop on_disconnect")

-- check on_connect/on_disconnect argument count and type
test:is(type(session.on_connect()), "table", "type of trigger on_connect, no args")
test:is(type(session.on_disconnect()), "table", "type of trigger on_disconnect, no args")

ok, err = pcall(session.on_connect, function() end, function() end)
test:is(err,"trigger reset: Trigger is not found", "on_connect trigger not found")
ok, err = pcall(session.on_disconnect, function() end, function() end)
test:is(err,"trigger reset: Trigger is not found", "on_disconnect trigger not found")

ok, err = pcall(session.on_connect, 1, 2)
test:is(err, "trigger reset: incorrect arguments", "on_connect bad args #1")
ok, err = pcall(session.on_disconnect, 1, 2)
test:is(err, "trigger reset: incorrect arguments", "on_disconnect bad args #1")

ok, err = pcall(session.on_connect, 1)
test:is(err, "trigger reset: incorrect arguments", "on_connect bad args #2")
ok, err = pcall(session.on_disconnect, 1)
test:is(err, "trigger reset: incorrect arguments", "on_disconnect bad args #2")

-- use of nil to clear the trigger
session.on_connect(nil, fail)
session.on_disconnect(nil, fail)

-- check how connect/disconnect triggers work
local peer_name = "peer_name"
function inc() active_connections = active_connections + 1 end
function dec() active_connections = active_connections - 1 end
function peer() peer_name = box.session.peer() end
net = { box = require('net.box') }
test:is(type(session.on_connect(inc)), "function", "type of trigger inc on_connect")
test:is(type(session.on_disconnect(dec)), "function", "type of trigger dec on_disconnect")
test:is(type(session.on_disconnect(peer)), "function", "type of trigger peer on_disconnect")
active_connections = 0
c = net.box.connect(HOST, PORT)
while active_connections < 1 do fiber.sleep(0.001) end
test:is(active_connections, 1, "active_connections after 1 connection")
c1 = net.box.connect(HOST, PORT)
while active_connections < 2 do fiber.sleep(0.001) end
test:is(active_connections, 2, "active_connections after 2 connection")
c:close()
c1:close()
while active_connections > 0 do fiber.sleep(0.001) end
test:is(active_connections, 0, "active_connections after closing")
test:isnil(peer_name, "peer_name after closing")

session.on_connect(nil, inc)
session.on_disconnect(nil, dec)
session.on_disconnect(nil, peer)

-- write audit trail of connect/disconnect into a space
function audit_connect() box.space['tweedledum']:insert{session.id()} end
function audit_disconnect() box.space['tweedledum']:delete{session.id()} end
test:is(type(session.on_connect(audit_connect)), "function", "type of trigger audit_connect on_connect")
test:is(type(session.on_disconnect(audit_disconnect)), "function", "type of trigger audit_connect on_disconnect")

box.schema.user.grant('guest', 'read,write,execute', 'universe')
a = net.box.connect(HOST, PORT)
test:ok(a:eval('return space:get{box.session.id()}[1] == session.id()'), "eval get_id")
test:ok(a:eval('return session.sync() ~= 0'), "eval sync")
a:close()

-- cleanup
session.on_connect(nil, audit_connect)
session.on_disconnect(nil, audit_disconnect)
test:is(active_connections, 0, "active connections after other triggers")

space:drop()

test:is(session.uid(), 1, "uid == 1")
test:is(session.user(), "admin", "user is admin")
test:is(session.sync(), 0, "sync constant")
box.schema.user.revoke('guest', 'read,write,execute', 'universe')

-- audit permission in on_connect/on_disconnect triggers
box.schema.user.create('tester', { password = 'tester' })

on_connect_user = nil
on_disconnect_user = nil
function on_connect() on_connect_user = box.session.effective_user() end
function on_disconnect() on_disconnect_user = box.session.effective_user() end
_ = box.session.on_connect(on_connect)
_ = box.session.on_disconnect(on_disconnect)
local conn = require('net.box').connect("tester:tester@" ..HOST..':'..PORT)
-- Triggers must not lead to privilege escalation
ok, err = pcall(function () conn:eval('box.space._user:select()') end)
test:ok(not ok, "check access")
conn:close()
conn = nil
while not on_disconnect_user do fiber.sleep(0.001) end
-- Triggers are executed with admin permissions
test:is(on_connect_user, 'admin', "check trigger permissions, on_connect")
test:is(on_disconnect_user, 'admin', "check trigger permissions, on_disconnect")

box.session.on_connect(nil, on_connect)
box.session.on_disconnect(nil, on_disconnect)

-- check Session privilege
ok, err = pcall(function() net.box.connect("tester:tester@" ..HOST..':'..PORT) end)
test:ok(ok, "session privilege")
box.schema.user.revoke('tester', 'session', 'universe')
conn = net.box.connect("tester:tester@" ..HOST..':'..PORT)
test:is(conn.state, "error", "session privilege state")
test:ok(conn.error:match("Session"), "sesssion privilege errmsg")
ok, err = pcall(box.session.su, "user1")
test:ok(not ok, "session.su on revoked")
box.schema.user.drop('tester')

local test_run = require('test_run')
local inspector = test_run.new()
test:is(
    inspector:cmd("create server session with script='box/tiny.lua'\n"),
    true, 'instance created'
)
test:is(
    inspector:cmd('start server session'),
    true, 'instance started'
)
local uri = inspector:eval('session', 'box.cfg.listen')[1]
conn = net.box.connect(uri)
test:ok(conn:eval("return box.session.exists(box.session.id())"), "remote session exist check")
test:isnt(conn:eval("return box.session.peer(box.session.id())"), nil, "remote session peer check")
test:ok(conn:eval("return box.session.peer() == box.session.peer(box.session.id())"), "remote session peer check")

-- gh-2994 session uid vs session effective uid
test:is(session.euid(), 1, "session.uid")
test:is(session.su("guest", session.uid), 1, "session.uid from su is admin")
test:is(session.su("guest", session.euid), 0, "session.euid from su is guest")
local id = conn:eval("return box.session.uid()")
test:is(id, 0, "session.uid from netbox")
id = conn:eval("return box.session.euid()")
test:is(id, 0, "session.euid from netbox")
--box.session.su("admin")
conn:eval("box.session.su(\"admin\", box.schema.create_space, \"sp1\")")
local sp = conn:eval("return box.space._space.index.name:get{\"sp1\"}[2]")
test:is(sp, 1, "effective ddl owner")
conn:close()

inspector:cmd('stop server session with cleanup=1')
session = nil
os.exit(test:check() == true and 0 or -1)
