#!/usr/bin/env tarantool
os = require('os')

box.cfg{
    listen              = os.getenv("LISTEN"),
    memtx_memory        = 107374182,
    pid_file            = "tarantool.pid",
    rows_per_wal        = 50,
    log_nonblock 	= false
}

require('console').listen(os.getenv('ADMIN'))
