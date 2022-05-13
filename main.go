package main

import (
	"context"
	"fmt"
	"os"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	_ "google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/artificialinc/alab-core/common-go/stream"
	"github.com/artificialinc/alab-core/common-go/stream/redis"
	pb "github.com/artificialinc/artificial-protos/go/artificial/api/alab/action"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "invalid arguments\n")
		os.Exit(1)
	}
	ctx := context.Background()

	namespace := os.Args[1]
	orgID := os.Args[2]
	actionID := os.Args[3]

	stream := redis.NewStream(nil)
	payloads, _, err := stream.AggregateAll(
		ctx,
		fmt.Sprintf("%s:org.%s.job.%s.state", namespace, orgID, actionID),
		"",
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot list actionDatas: %v\n", err)
		os.Exit(1)
	}

	for _, payload := range payloads {
		if id, ok := payload["ADD"]; ok {
			getAndPrintActionData(ctx, stream, namespace, orgID, id)
		}
	}
}

func getAndPrintActionData(ctx context.Context, stream stream.Stream, namespace, orgID, id string) {
	// fmt.Printf("Finding [%s]...\n", id)

	get, _, err := stream.GetLatest(ctx, fmt.Sprintf("%s:org.%s.actiondata.%s", namespace, orgID, id))
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot get actionData: %v\n", err)
		os.Exit(1)
	}

	actionData := &pb.ActionData{}
	name := string(actionData.ProtoReflect().Descriptor().FullName())

	for _, payload := range get {
		if b, ok := payload[name]; ok {
			a := &pb.ActionData{}
			err = proto.Unmarshal([]byte(b), a)
			if err != nil {
				fmt.Fprintf(os.Stderr, "cannot deserialize actionData: %v\n", err)
				os.Exit(1)
			}

			opts := protojson.MarshalOptions{
				Multiline: true,
				Indent:    "  ",
			}
			serializedJson, err := opts.Marshal(a)
			if err != nil {
				fmt.Fprintf(os.Stderr, "cannot convert actionData to json: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("%s\n\n", string(serializedJson))
			return
		}
	}

	fmt.Fprintf(os.Stderr, "cannot find or deserialize actionData: %v\n", err)
	os.Exit(1)
}
