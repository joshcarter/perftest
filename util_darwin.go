package main

import "os"

func parseOpenFlags(flags []string) int {
	openFlags := 0

	for _, flag := range flags {
		switch flag {
		case "o_sync", "O_SYNC", "sync", "SYNC":
			openFlags |= os.O_SYNC
		}
	}
	return openFlags
}
