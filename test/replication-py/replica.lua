#!/usr/bin/env tarantool
box_cfg_done = false

require('console').listen(os.getenv('ADMIN'))

box.cfg({
    listen              = os.getenv("LISTEN"),
    replication         = os.getenv("MASTER"),
    memtx_memory        = 107374182,
    log_nonblock 	= false,
})

box_cfg_done = true
