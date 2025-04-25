package stream

import (
	"fmi/stream-ingest/domain/base"
	"testing"
)

func TestCompareStreams(t *testing.T) {
	stream1 := Stream{
		Base: base.NewBase(""),
	}
	stream1.ID = "test-uuid-1234"
	stream1.SiteCode = "SiteA"
	stream1.Tags = []base.Tag{{Name: "A", Value: "1"}}

	stream2 := Stream{
		Base: base.NewBase(""),
	}
	stream2.ID = stream1.ID
	stream2.SiteCode = "SiteA"
	stream2.Tags = []base.Tag{{Name: "A", Value: "1"}}

	if !CompareStreams(stream1, stream2) {
		t.Error("streams should be equal")
	}
	stream2.Tags[0].Value = "2"
	if CompareStreams(stream1, stream2) {
		t.Error("streams should not be equal")
	}
}

func TestUpdateStream(t *testing.T) {
	stream1 := Stream{
		Base: base.NewBase(""),
	}
	stream1.SiteCode = "SiteA"
	stream2 := Stream{
		Base: base.NewBase(""),
	}
	stream1.SiteCode = "SiteA"
	stream1.Tags = []base.Tag{{Name: "A", Value: "1"}}
	UpdateStream(&stream1, &stream2, "user1")
	if stream1.SiteCode != stream2.SiteCode || !base.CompareTags(stream1.Tags, stream2.Tags) {
		t.Error("stream1 should match stream2 after update")
	}
}
