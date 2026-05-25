package main

import "strings"

const (
	prefixEvent = "event:"
	prefixData  = "data:"
	prefixID    = "id:"
)

func ParseFrame(lines []string) (event, data string, ok bool) {
	var haveEvent, haveData bool
	for _, line := range lines {

		if len(line) > 0 && line[0] == ':' {
			continue
		}
		switch {
		case strings.HasPrefix(line, prefixEvent):
			if !haveEvent {
				event = strings.TrimPrefix(strings.TrimPrefix(line, prefixEvent), " ")
				haveEvent = true
			}
		case strings.HasPrefix(line, prefixData):
			if !haveData {
				data = strings.TrimPrefix(strings.TrimPrefix(line, prefixData), " ")
				haveData = true
			}
		}
	}
	if !haveEvent || !haveData {
		return "", "", false
	}
	return event, data, true
}
