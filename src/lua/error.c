#include <lua.h>
#include <lauxlib.h>
#include "diag.h"
#include "utils.h"

int
lua_error_gettraceback(struct lua_State *L)
{
	struct error *e = luaL_iserror(L, -1);
	if (!e) {
		return 0;
	}
	lua_newtable(L);
	if (e->depth_traceback >= DIAG_MAX_TRACEBACK || e->depth_traceback <= 0) {
		return 1;
	}
	for (int i = 1; i <= e->depth_traceback; i++) {
		/* push index */
		lua_pushnumber(L, i);

		/* push value - table of filename and line */
		lua_newtable(L);

		lua_pushstring(L, "file");
		lua_pushstring(L, e->frames[i - 1].filename);
		lua_settable(L, -3);

		lua_pushstring(L, "line");
		lua_pushinteger(L, e->frames[i - 1].line);
		lua_settable(L, -3);

		lua_settable(L, -3);
	}
	return 1;
}

static const struct luaL_Reg error_internal[] = {
		{"traceback", lua_error_gettraceback},
		{NULL, NULL}
};

void
tarantool_lua_error_internal_init(struct lua_State *L)
{
	luaL_register(L, "errors.internal", error_internal);
	lua_pop(L, 1);
}