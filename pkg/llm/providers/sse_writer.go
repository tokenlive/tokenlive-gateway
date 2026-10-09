package providers

import (
	"fmt"
	"io"
)

func writeSSEEvent(w io.Writer, event string, data []byte) error {
	_, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
	return err
}

func writeSSEData(w io.Writer, data []byte) error {
	_, err := fmt.Fprintf(w, "data: %s\n\n", data)
	return err
}
