#!/usr/bin/env tarantool
-- In SQLite multiple ON CONFLICT REPLACE were allowed in CREATE TABLE statement.
-- However, we decided that we should have only one constraint with ON CONFLICT
-- REPLACE action. This tests check that proper error message will be raised
-- if user makes multiple columns with ON CONFLICT REPLACE.

test = require("sqltester")
test:plan(5)

test:do_execsql_test(
    "replace-1.1",
    [[
        CREATE TABLE t1(a INT PRIMARY KEY,
                        b INT UNIQUE ON CONFLICT REPLACE,
                        c INT UNIQUE
                        );
    ]], {

    })

test:do_catchsql_test(
    "replace-1.2",
    [[
        DROP TABLE IF EXISTS t1;
        CREATE TABLE t1(a INT PRIMARY KEY,
                        b INT UNIQUE ON CONFLICT REPLACE,
                        c INT UNIQUE ON CONFLICT REPLACE
                        );
    ]], {
        1, "Table T1 can feature only one ON CONFLICT REPLACE constraint"
    })

test:do_execsql_test(
    "replace-1.3",
    [[
        DROP TABLE IF EXISTS t1;
        CREATE TABLE t1(a INT PRIMARY KEY,
                        b INT UNIQUE ON CONFLICT REPLACE,
                        c INT NOT NULL ON CONFLICT REPLACE
                        );
    ]], {

    })

test:do_execsql_test(
    "replace-1.4",
    [[
        DROP TABLE IF EXISTS t1;
        CREATE TABLE t1(a INT PRIMARY KEY,
                        b INT NOT NULL ON CONFLICT REPLACE,
                        c INT UNIQUE ON CONFLICT REPLACE
                        );
    ]], {

    })

test:do_catchsql_test(
    "replace-1.5",
    [[
        DROP TABLE IF EXISTS t1;
        CREATE TABLE t1(a INT PRIMARY KEY,
                        b INT NOT NULL ON CONFLICT REPLACE,
                        c INT NOT NULL ON CONFLICT REPLACE
                        );
    ]], {
        1, "Table T1 can feature only one ON CONFLICT REPLACE constraint"
    })

test:finish_test()
