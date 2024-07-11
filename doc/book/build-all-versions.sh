#/usr/bin/bash

set -eu

if [ $# -ne 1 ]; then
	echo "usage: $0 <destination-directory>" >&2
	exit 1
fi

dest="$1"

if [ ! -d "$dest" ]; then
	echo "destination directory does not exist" >&2
	exit 1
fi

for i in *; do
	if [ -d "$i" -a ! -L "$i" ]; then
		mdbook build --dest-dir "$dest/$i" "$i"
	elif [ -L "$i" ]; then
		contents=$(readlink "$i")
		(cd "$dest" && ln -s "$contents" "$i")
	fi
done
