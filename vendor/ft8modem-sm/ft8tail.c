/*
 *
 *   ft8tail.c
 *
 *   Get the recent lines from all.txt.
 *
 *   Written in C for speed.
 *
 *   Copyright (C) 2025 by Matt Roberts.
 *   License: GNU GPL3 (www.gnu.org)
 *
 */


#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <ctype.h>
#include <stdlib.h>


/*
 *  to_time(s) - convert ALL.TXT stamp to unix time
 */
int64_t to_time(char *s) {
	if (s == 0 || s[0] == 0)
		return 0;

	char *cp = strchr(s, ' ');
	if (cp)
		*cp = 0;
		
	char *dptr = strchr(s, '_');
	if (dptr) {
		char *tptr = dptr + 1;
		*dptr = 0;
		dptr = s;

		struct tm t;
		bzero(&t, sizeof(t));

		t.tm_mday = atoi(dptr + 4);
		*(dptr + 4) = 0;
		t.tm_mon = atoi(dptr + 2) - 1;
		*(dptr + 2) = 0;
		t.tm_year = (2000 + atoi(dptr)) - 1900;

		t.tm_sec = atoi(tptr + 4);
		*(tptr + 4) = 0;
		t.tm_min = atoi(tptr + 2);
		*(tptr + 2) = 0;
		t.tm_hour = atoi(tptr);

		/* convert to int64_t */
		return mktime(&t) - timezone;
	}

	/* parse out the Unix time */
	return atoll(s);
}


/*
 *  process(since, f)
 */
void process(int64_t since, FILE *fp) {
	static char iobuffer[1024];
	static char tmbuffer[16];

	while ( ! feof(fp)) {
		char *line = fgets(iobuffer, sizeof(iobuffer), fp);
		if ( ! line) {
			return;
		}

		/* copy a few bytes into the time buffer */
		strncpy(tmbuffer, iobuffer, sizeof(tmbuffer));
		tmbuffer[sizeof(tmbuffer) - 1] = 0;

		/* read the time of the line */
		int64_t when = to_time(tmbuffer);

		/* DEBUG: */
		/* fprintf(stderr, "DEBUG: when = %ld; since = %ld\n", when, since); */

		/* filter; TODO: refactor this to reduce number of comparisons */
		if (since < 0) { /* lines before */
			if (when <= -since)
				fputs(iobuffer, stdout);
		} else {         /* lines after */
			if (when >= since)
				fputs(iobuffer, stdout);
		}
	}
}


/*
 *  parse_expr(s) - return the 'since' value
 */
int64_t parse_expr(char *s) {
	if (s == 0 || s[0] == 0)
		return 0;
	
	/* copy to working buffer */
	char w[128];
	strncpy(w, s, sizeof(w));
	w[sizeof(w) - 1] = 0;
	s = w; /* reassign working pointer to temp */

	// eat a leading '+' character; but leave any leading '-'
	if (s[0] == '+') s++;

	/* extract the suffix, which may or may not be there */
	char suffix = s[strlen(s) - 1];
	if (isdigit(suffix)) {
		suffix = 0;
	}

	/* multiply if suffix given */
	int mult = 1;
	switch (suffix) {
		case  0 : return atoll(s); // no suffix; just convert
		case 'w': mult = 7 * 24 * 3600; break;
		case 'd': mult = 24 * 3600; break;
		case 'h': mult = 3600; break;
		case 'm': mult = 60; break;
		case 's': break;
		default: return 0; /* invalid */
	}

	/* delete the suffix to leave just the digits */
	s[strlen(s) - 1] = 0;

	/* handle 'before' case */
	short invert = 0;
	int64_t offset = atoll(s);
	if (offset < 0) {
		offset = -offset;
		invert = 1;
	}

	/* compute the time edge */
	int64_t result = time(0) - (offset * mult);
	if (invert)
		result = -result;
	return result;
}


/*
 *  main() - main program
 */ 
int main (int argc, char **argv) {
	/* validate command line */
	if (argc <= 1) {
		fprintf(stderr, "\n");
		fprintf(stderr, "Usage: %s <when> [<file> ... ]\n", argv[0]);
		fprintf(stderr, "\n");
		fprintf(stderr, "   'when' can be:\n");
		fprintf(stderr, "       unix timestamp (e.g., 1702712345)\n");
		fprintf(stderr, "       now-relative time (e.g., 3w, 2d, 12h, 60s)\n");
		fprintf(stderr, "   If value of 'when' is negative, return lines before; default is after.\n");
		fprintf(stderr, "\n");
		exit(1);
	}
	
	/* get the 'since' time */
	char *expr = argv[1];
	int64_t since = parse_expr(expr);
	if ( ! since) {
		fputs("Invalid 'since' value provided\n", stderr);
		exit(1);
	}

	/* DEBUG: */
	/* fprintf(stderr, "DEBUG: since = %ld\n", since); */

	/* if no files provided, use stdin */
	if (argc == 2) {
		process(since, stdin);
		return 0;
	}

	/* process files provided */
	for (short i = 2; i != argc; ++i) {
		FILE *fp = fopen(argv[i], "r");
		if ( ! fp) {
			fprintf(stderr, "Could not open file: %s\n", argv[i]);
			continue;
		}

		process(since, fp);
	}
}

/* EOF: ft8tail.c */
