#!/usr/bin/env tarantool

box.cfg {
    listen = os.getenv("LISTEN"),
    force_recovery = true,
    log_nonblock = false,
}

require('console').listen(os.getenv('ADMIN'))
