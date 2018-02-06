#!/usr/bin/env tarantool
test = require("sqltester")
test:plan(12)

-- gh-2996 - INDEXED BY clause wasn't working.
-- This functional test ensures that execution of that type of
-- statement is correct.

local function eqp(sql)
    return "EXPLAIN QUERY PLAN " .. sql
end

test:execsql [[
    CREATE TABLE t1(a INT PRIMARY KEY, b);
    CREATE INDEX t1ix1 ON t1(b);
    CREATE INDEX t1ix2 on t1(b);
]]

sample_size = 1000
local query = "INSERT INTO t1 VALUES "

for i = 1, sample_size do
    query = query  .. "(" .. i .. ", " .. i .. ")"
    if (i ~= sample_size) then
        query = query .. ","
    end
end

-- Fill our space with data
test:execsql(query)

-- Make sure that SELECT works correctly when index exists.
test:do_execsql_test(
    "indexed-by-1.1",
    eqp("SELECT b FROM t1 WHERE b <= 5"), {
        0, 0, 0, 'SEARCH TABLE T1 USING COVERING INDEX T1IX2 (B<?)'
    })

test:do_execsql_test(
    "indexed-by-1.2",
    eqp("SELECT b FROM t1 INDEXED BY t1ix1 WHERE b <= 5"), {
        0, 0, 0, 'SEARCH TABLE T1 USING COVERING INDEX T1IX1 (B<?)'
    })

test:execsql [[
    DROP INDEX t1ix1 ON t1;
    DROP INDEX t1ix2 ON t1;
]]

-- Now make sure that when schema was changed (t1ix1 was dropped),
-- SELECT statement won't work.
test:do_catchsql_test(
    "indexed-by-1.3",
    eqp("SELECT b FROM t1 INDEXED BY t1ix1 WHERE b <= 5"), {
        1, "no such index: T1IX1"
    })

test:do_catchsql_test(
    "indexed-by-1.4",
    eqp("SELECT b FROM t1 INDEXED BY t1ix2 WHERE b <= 5"), {
        1, "no such index: T1IX2"
    })

-- Make sure that DELETE statement works correctly with INDEXED BY.
test:execsql [[
    CREATE INDEX t1ix1 ON t1(b);
    CREATE INDEX t1ix2 on t1(b);
]]

test:do_execsql_test(
    "indexed-by-1.5",
    eqp("DELETE FROM t1 WHERE b <= 5"), {
        0, 0, 0, 'SEARCH TABLE T1 USING COVERING INDEX T1IX2 (B<?)'
    })

test:do_execsql_test(
    "indexed-by-1.6",
    eqp("DELETE FROM t1 INDEXED BY t1ix1  WHERE b <= 5"), {
        0, 0, 0, 'SEARCH TABLE T1 USING COVERING INDEX T1IX1 (B<?)'
    })

test:execsql [[
    DROP INDEX t1ix1 ON t1;
    DROP INDEX t1ix2 ON t1;
]]

test:do_catchsql_test(
    "indexed-by-1.7",
    eqp("DELETE FROM t1 INDEXED BY t1ix1 WHERE b <= 5"), {
        1, "no such index: T1IX1"
    })

test:do_catchsql_test(
    "indexed-by-1.8",
    eqp("DELETE FROM t1 INDEXED BY t1ix2 WHERE b <= 5"), {
        1, "no such index: T1IX2"
    })

test:execsql "CREATE INDEX t1ix1 ON t1(b)"
test:execsql "CREATE INDEX t1ix2 ON t1(b)"

test:do_execsql_test(
    "indexed-by-1.9",
    eqp("UPDATE t1 SET b = 20 WHERE b = 10"), {
        0, 0, 0, 'SEARCH TABLE T1 USING COVERING INDEX T1IX2 (B=?)'
    })

test:do_execsql_test(
    "indexed-by-1.10",
    eqp("UPDATE t1 INDEXED BY t1ix1 SET b = 20 WHERE b = 10"), {
        0, 0, 0, 'SEARCH TABLE T1 USING COVERING INDEX T1IX1 (B=?)'
    })

test:execsql [[
    DROP INDEX t1ix1 ON t1;
    DROP INDEX t1ix2 ON t1;
]]

test:do_catchsql_test(
    "indexed-by-1.11",
    eqp("UPDATE t1 INDEXED BY t1ix1 SET b = 20 WHERE b = 10"), {
        1, "no such index: T1IX1"
    })

test:do_catchsql_test(
    "indexed-by-1.12",
    eqp("UPDATE t1 INDEXED BY t1ix2 SET b = 20 WHERE b = 10"), {
        1, "no such index: T1IX2"
    })

test:finish_test()

