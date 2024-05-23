#!/usr/bin/env bash
exec jq '.[] | {tag_name} + (.assets[] | {name, download_count}) | select ( .download_count > 0)'
