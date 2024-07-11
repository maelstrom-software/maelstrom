#/usr/bin/bash

set -eu

if [ $# -ne 1 ]; then
	echo "usage: $0 <destination-directory>" >&2
	exit 1
fi

if [ ! -d "$1" ]; then
	echo "destination directory does not exist" >&2
	exit 1
fi

# Get absolute path.
dest="$(cd "$1" && pwd -P)"

for i in *; do
	if [ -d "$i" -a ! -L "$i" ]; then
		mdbook build --dest-dir "$dest/$i" "$i"
	elif [ -L "$i" ]; then
		contents=$(readlink "$i")
		(cd "$dest" && ln -s "$contents" "$i")
	fi
done
