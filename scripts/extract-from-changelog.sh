#!/usr/bin/env bash

if [ "$#" -ne 1 ]; then
	cat >&2 <<EOF
usage: $(basename $0) VERSION
Reads a changelog in "Keep a Changelog" format (https://keepachangelog.com) from
standard input and prints just the entries for VERSION to standard output.
EOF
	exit 1
fi

exec awk -v "version=$1" '
BEGIN {
    start_line = sprintf("## [%s]", version)
}
{
    if (index($0, start_line) == 1) {
        printing = 1
    } else if (index($0, "## [") == 1) {
        printing = 0
    } else if (match($0, "^[ \t]*$")) {
        blanks += 1
    } else if (printing) {
        if (printing == 2) {
            for (i = 0; i < blanks; i++) {
                print ""
            }
        }
        print $0
        blanks = 0
	printing = 2
    }
}
'
