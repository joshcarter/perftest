package main

import (
	"os"
	"syscall"
)

func parseOpenFlags(flags []string) int {
	openFlags := 0

	for _, flag := range flags {
		switch flag {
		case "o_sync", "O_SYNC", "sync", "SYNC":
			openFlags |= os.O_SYNC
		case "o_direct", "O_DIRECT", "direct", "DIRECT":
			openFlags |= syscall.O_DIRECT
		}
	}

	return openFlags
}
