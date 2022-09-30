package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	_ "google.golang.org/protobuf/types/known/wrapperspb"

	stream_engine "github.com/artificialinc/alab-core/common-go/stream"
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
	resourceID := os.Args[3]

	stream := redis.NewStream(nil)
	payloads, _, err := stream.AggregateAll(
		ctx,
		// old streams
		// fmt.Sprintf("%s:org.%s.job.%s.state", namespace, orgID, resourceID),
		// fmt.Sprintf("%s:org.%s.action.%s.state", namespace, orgID, resourceID),
		// "spark-artificial-com:org.spark.action.action_281da04e-7e25-4d7c-8a8c-3428b1851809.state"
		// consolidate streams
		// fmt.Sprintf("%s:org.%s.job.%s.snapshots_state", namespace, orgID, resourceID),
		// fmt.Sprintf("%s:org.%s.action.%s.snapshots_state", namespace, orgID, resourceID),
		fmt.Sprintf("%s:org.%s.lab.%s.program.0.snapshots_state", namespace, orgID, resourceID),
		"",
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot list actionDatas: %v\n", err)
		os.Exit(1)
	}

	for _, payload := range payloads {
		// old stream
		// if id, ok := payload["ADD"]; ok {
		// 	getAndPrintActionData(ctx, stream, namespace, orgID, id)
		// }
		// consolidated stream
		printActionData([]stream_engine.Payload{payload})
	}
}

func getAndPrintActionData(ctx context.Context, stream stream_engine.Stream, namespace, orgID, id string) {
	// fmt.Printf("Finding [%s]...\n", id)

	get, _, err := stream.GetLatest(ctx, fmt.Sprintf("%s:org.%s.actiondata.%s", namespace, orgID, id))
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot get actionData: %v\n", err)
		os.Exit(1)
	}

	printActionData(get)
}

func printActionData(msgs []stream_engine.Payload) {
	actionData := &pb.ActionData{}
	name := string(actionData.ProtoReflect().Descriptor().FullName())

	for _, payload := range msgs {
		if b, ok := payload[name]; ok {
			a := &pb.ActionData{}
			err := proto.Unmarshal([]byte(b), a)
			if err != nil {
				fmt.Fprintf(os.Stderr, "cannot deserialize actionData: %v\n", err)
				os.Exit(1)
			}

			opts := protojson.MarshalOptions{
				Multiline: true,
			}
			serializedJson, err := opts.Marshal(a)
			if err != nil {
				fmt.Fprintf(os.Stderr, "cannot convert actionData to json: %v\n", err)
				os.Exit(1)
			}

			var formattedJson bytes.Buffer
			err = json.Indent(&formattedJson, serializedJson, "", "  ")
			if err != nil {
				fmt.Fprintf(os.Stderr, "cannot format actionData json: %v\n", err)
			}

			fmt.Printf("%s\n\n", string(serializedJson))
			return
		}
	}
}
