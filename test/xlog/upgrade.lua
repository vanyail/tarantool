#!/usr/bin/env tarantool

box.cfg {
    listen              = os.getenv("LISTEN"),
    memtx_memory        = 107374182,
    log_nonblock 	= false,
}

require('console').listen(os.getenv('ADMIN'))
