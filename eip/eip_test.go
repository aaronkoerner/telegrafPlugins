package eip

import (
	"math"
	"testing"

	"github.com/influxdata/telegraf/testutil"
)

func TestPLC(t *testing.T) {
	plc := &PLC{
		 TagsToRead: ["tag1",
	"tag2",
	"tag3"],
	IPAddress: "192.168.14.169"
	ProcessSlot: 3
	}

	for i := 0.0; i < 10.0; i++ {

		var acc testutil.Accumulator

		values := []float{5.3, 8.9, 10.1}

		plc.Gather(&acc)

		fields := make(map[string]interface{})
		fields["values"] = values

		acc.AssertContainsFields(t, "eip", fields)
	}
}
