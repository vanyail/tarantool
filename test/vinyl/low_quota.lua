#!/usr/bin/env tarantool

local LIMIT = tonumber(arg[1])

box.cfg{
    vinyl_memory = LIMIT,
    vinyl_max_tuple_size = 2 * LIMIT,
    log_nonblock 	= false,
}

require('console').listen(os.getenv('ADMIN'))
