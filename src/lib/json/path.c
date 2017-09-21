/*
 * Copyright 2010-2016 Tarantool AUTHORS: please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include "path.h"
#include <ctype.h>
#include "trivia/util.h"

/** Same as strtoull(), but with limited length. */
static inline uint64_t
strntoull(const char *src, int len) {
	uint64_t value = 0;
	for (const char *end = src + len; src < end; ++src) {
		assert(isdigit(*src));
		value = value * 10 + *src - (int)'0';
	}
	return value;
}

/**
 * Parse string identifier in quotes. Parser either stops right
 * after the closing quote, or returns an error position.
 * @param parser JSON path parser.
 * @param[out] node JSON node to store result.
 *
 * @retval     0 Success.
 * @retval not 0 1-based position of a syntax error.
 */
static inline int
json_parse_string(struct json_path_parser *parser, struct json_path_node *node)
{
	const char *end = parser->src + parser->src_len;
	const char *pos = parser->pos;
	assert(pos < end);
	char quote_type = *pos;
	assert(quote_type == '\'' || quote_type == '"');
	/* Skip first quote. */
	int len = 0;
	++pos;
	const char *str = pos;
	for (char c = *pos; pos < end && quote_type != c; c = *++pos)
		++len;
	/* A string must be terminated with quote. */
	if (*pos != quote_type || len == 0)
		return pos - parser->src + 1;
	/* Skip the closing quote. */
	parser->pos = pos + 1;
	node->type = JSON_PATH_STR;
	node->str = str;
	node->len = len;
	return 0;
}

/**
 * Parse digit sequence into integer until non-digit is met.
 * Parser stops right after the last digit.
 * @param parser JSON parser.
 * @param[out] node JSON node to store result.
 *
 * @retval     0 Success.
 * @retval not 0 1-based position of a syntax error.
 */
static inline int
json_parse_integer(struct json_path_parser *parser, struct json_path_node *node)
{
	const char *end = parser->src + parser->src_len;
	const char *pos = parser->pos;
	assert(pos < end);
	const char *str = pos;
	int len = 0;
	for (char c = *pos; pos < end && isdigit(c); c = *++pos)
		++len;
	if (len == 0)
		return pos - parser->src + 1;
	parser->pos = pos;
	node->type = JSON_PATH_NUM;
	node->num = strntoull(str, len);
	return 0;
}

/**
 * Parse identifier out of quotes. It can contain only alphas,
 * digits and underscores. And can not contain digit at the first
 * position. Parser is stoped right after the last non-digit,
 * non-alpha and non-underscore symbol.
 * @param parser JSON parser.
 * @param[out] node JSON node to store result.
 *
 * @retval     0 Success.
 * @retval not 0 1-based position of a syntax error.
 */
static inline int
json_parse_identifier(struct json_path_parser *parser,
		      struct json_path_node *node)
{
	const char *end = parser->src + parser->src_len;
	const char *pos = parser->pos;
	assert(pos < end);
	const char *str = pos;
	char c = *pos;
	/* First symbol can not be digit. */
	if (!isalpha(c) && c != '_')
		return pos - parser->src + 1;
	int len = 1;
	for (c = *++pos; pos < end && (isalpha(c) || c == '_' || isdigit(c));
	     c = *++pos)
		++len;
	assert(len > 0);
	parser->pos = pos;
	node->type = JSON_PATH_STR;
	node->str = str;
	node->len = len;
	return 0;
}

int
json_path_next(struct json_path_parser *parser, struct json_path_node *node)
{
	const char *end = parser->src + parser->src_len;
	if (end == parser->pos) {
		node->type = JSON_PATH_END;
		return 0;
	}
	char c = *parser->pos;
	int rc;
	switch(c) {
	case '[':
		++parser->pos;
		/* Error for []. */
		if (parser->pos == end)
			return parser->pos - parser->src + 1;
		c = *parser->pos;
		if (c == '"' || c == '\'')
			rc = json_parse_string(parser, node);
		else
			rc = json_parse_integer(parser, node);
		if (rc != 0)
			return rc;
		/*
		 * Expression, started from [ must be finished
		 * with ] regardless of its type.
		 */
		if (parser->pos == end || *parser->pos != ']')
			return parser->pos - parser->src + 1;
		/* Skip ]. */
		++parser->pos;
		break;
	case '.':
		/* Skip dot. */
		++parser->pos;
		if (parser->pos == end)
			return parser->pos - parser->src + 1;
		FALLTHROUGH
	default:
		rc = json_parse_identifier(parser, node);
		if (rc != 0)
			return rc;
		break;
	}
	return 0;
}
