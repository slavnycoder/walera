package safego

import (
	"fmt"
	"os"
	"runtime/debug"
)

func Go(name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintf(os.Stderr, "safego: goroutine %q panic: %v\nstack: %s\n",
					name, r, debug.Stack())
			}
		}()
		fn()
	}()
}
