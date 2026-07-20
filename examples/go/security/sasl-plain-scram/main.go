// Command sasl-plain-scram is master-table variant 13: SASL authentication
// (PLAIN + SCRAM-SHA-256/512) against the KubeMQ Kafka connector.
//
//	SASL/PLAIN  or  SCRAM-SHA-256  or  SCRAM-SHA-512  (chosen by env)
//	  -> authenticated CreateTopic + Produce + Fetch round-trip SUCCEEDS
//	  -> (optional) a DENIED principal -> TOPIC_AUTHORIZATION_FAILED
//
// This variant is RUNNABLE ONLY against a broker that has a credential store
// configured (spec §4.7). It reads credentials from the environment and NEVER
// hard-codes them:
//
//	KAFKA_SASL_USER        SASL username           (required to run the auth path)
//	KAFKA_SASL_PASS        SASL password           (required to run the auth path)
//	KAFKA_SASL_MECHANISM   PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512  (default PLAIN)
//	KAFKA_SASL_DENIED_USER optional principal expected to be DENIED
//	KAFKA_SASL_DENIED_PASS password for the denied principal
//
// If KAFKA_SASL_USER/PASS are unset the program prints how to enable SASL and exits
// 0 (nothing to assert without a credential store) — it does not fail the run.
//
// TLS / mTLS are DOC-ONLY here (see the README): the runnable path is SASL over
// plaintext on :9092; TLS lives on :9093 via kgo.DialTLSConfig.
//
// Run:
//
//	export KUBEMQ_KAFKA_BOOTSTRAP="localhost:9092"
//	export KAFKA_SASL_USER=alice KAFKA_SASL_PASS=secret KAFKA_SASL_MECHANISM=SCRAM-SHA-256
//	go run ./security/sasl-plain-scram
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/kubemq-io/kubemq-kafka-examples/go/internal/kafkaclient"
)

func main() {
	kafkaclient.Banner("security/sasl-plain-scram")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	user := os.Getenv("KAFKA_SASL_USER")
	pass := os.Getenv("KAFKA_SASL_PASS")
	if user == "" || pass == "" {
		fmt.Println("SKIP: SASL example needs a broker with a credential store.")
		fmt.Println("      Set KAFKA_SASL_USER / KAFKA_SASL_PASS (and optionally")
		fmt.Println("      KAFKA_SASL_MECHANISM=PLAIN|SCRAM-SHA-256|SCRAM-SHA-512) and re-run.")
		fmt.Println("PASS: nothing to assert without credentials (see README for setup)")
		return
	}

	mechName := strings.ToUpper(os.Getenv("KAFKA_SASL_MECHANISM"))
	if mechName == "" {
		mechName = "PLAIN"
	}
	mech, err := mechanism(mechName, user, pass)
	if err != nil {
		log.Fatalf("mechanism %s: %v", mechName, err)
	}
	fmt.Printf("SASL: mechanism=%s user=%s\n", mechName, user)

	topic := kafkaclient.Topic("sec", "sasl")

	// Authenticated admin + produce + fetch round-trip.
	adm, admCl, err := kafkaclient.Admin(kgo.SASL(mech))
	if err != nil {
		log.Fatalf("admin client: %v", err)
	}
	defer admCl.Close()
	if _, err := adm.CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		log.Fatalf("authenticated CreateTopic: %v", err)
	}
	fmt.Printf("CreateTopic (authenticated): %s\n", topic)

	body := []byte("authenticated payload")
	prod, err := kafkaclient.New(
		kgo.SASL(mech),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		log.Fatalf("producer: %v", err)
	}
	if err := prod.ProduceSync(ctx, &kgo.Record{Value: body}).FirstErr(); err != nil {
		prod.Close()
		log.Fatalf("authenticated Produce: %v", err)
	}
	prod.Close()
	fmt.Println("Produce (authenticated): ok")

	cons, err := kafkaclient.New(
		kgo.SASL(mech),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(2*time.Second),
	)
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}
	got := pollOne(ctx, cons)
	cons.Close()
	if string(got.Value) != string(body) {
		log.Fatalf("FAIL: authenticated read-back %q != %q", got.Value, body)
	}
	fmt.Printf("Fetch (authenticated): %q round-trip ok\n", got.Value)

	// Optional denied-principal path -> TOPIC_AUTHORIZATION_FAILED.
	dUser := os.Getenv("KAFKA_SASL_DENIED_USER")
	dPass := os.Getenv("KAFKA_SASL_DENIED_PASS")
	if dUser != "" && dPass != "" {
		dMech, err := mechanism(mechName, dUser, dPass)
		if err != nil {
			log.Fatalf("denied mechanism: %v", err)
		}
		badProd, err := kafkaclient.New(
			kgo.SASL(dMech),
			kgo.DefaultProduceTopic(topic),
			kgo.RequiredAcks(kgo.AllISRAcks()),
		)
		if err != nil {
			log.Fatalf("denied producer: %v", err)
		}
		err = badProd.ProduceSync(ctx, &kgo.Record{Value: []byte("nope")}).FirstErr()
		badProd.Close()
		if err == nil {
			log.Fatalf("FAIL: denied principal %q produced successfully", dUser)
		}
		if !errors.Is(err, kerr.TopicAuthorizationFailed) &&
			!errors.Is(err, kerr.SaslAuthenticationFailed) {
			log.Fatalf("FAIL: denied principal rejected with %v, want *_AUTHORIZATION_FAILED", err)
		}
		fmt.Printf("Denied principal %q: rejected with %v (expected)\n", dUser, err)
	} else {
		fmt.Println("Denied-principal path: skipped (set KAFKA_SASL_DENIED_USER/PASS to exercise)")
	}

	if _, err := adm.DeleteTopics(ctx, topic); err != nil {
		log.Printf("warning: DeleteTopics: %v", err)
	}
	fmt.Println("DeleteTopic: ok")
	fmt.Println("PASS: SASL authenticated round-trip verified")
}

// mechanism builds the requested SASL mechanism from a username/password.
func mechanism(name, user, pass string) (sasl.Mechanism, error) {
	switch name {
	case "PLAIN":
		return plain.Auth{User: user, Pass: pass}.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{User: user, Pass: pass}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return scram.Auth{User: user, Pass: pass}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("unsupported mechanism %q (want PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512)", name)
	}
}

func pollOne(ctx context.Context, cl *kgo.Client) *kgo.Record {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		fs := cl.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			log.Fatalf("PollFetches: %v", errs[0].Err)
		}
		for it := fs.RecordIter(); !it.Done(); {
			return it.Next()
		}
	}
	log.Fatalf("FAIL: timed out waiting for the authenticated record")
	return nil
}
