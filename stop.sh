#!/usr/bin/env bash
# Kill any leftover DFS processes (coordinator + storage nodes).
pkill -f 'dfs_coordinator' 2>/dev/null
pkill -f 'dfs_node' 2>/dev/null
echo "stopped (coordinator + nodes)"
