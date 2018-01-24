#!/usr/bin/env tarantool

box.cfg{
    listen = os.getenv("LISTEN"),
    log_nonblock = false,
}

require('console').listen(os.getenv('ADMIN'))
