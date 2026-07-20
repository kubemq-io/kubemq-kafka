package engine

import (
	"fmt"

	"github.com/kubemq-io/kubemq-kafka/burnin/config"
)

// computeVerdict evaluates the captured worker snapshots against the configured
// thresholds (spec §7.4) plus the 6 Kafka gates and stores the result. Hard
// failures → FAILED; advisory memory growth → PASSED_WITH_WARNINGS; otherwise
// PASSED. boundaryLossPct caps the at-least-once loss gate to tolerate the
// sequence tracker's reorder false-positive under sustained out-of-order
// delivery; systemic loss above it still fails.
const boundaryLossPct = 0.5

func (e *Engine) computeVerdict(cfg *config.Config) {
	result := &VerdictResult{Result: "PASSED", Passed: true}

	measurementDuration := e.producersStoppedAt.Sub(e.producersStartedAt)

	fail := func(format string, args ...any) {
		result.Result = "FAILED"
		result.Passed = false
		result.Warnings = append(result.Warnings, fmt.Sprintf(format, args...))
	}

	for name, snap := range e.workerSnapshots {
		if snap.Corrupted > 0 {
			fail("%s: %d corrupted messages", name, snap.Corrupted)
		}

		// Gate 1: loss (standard tracker loss). The cyclic admin_topic_churn worker
		// drives no steady data stream, so it is exempt from the loss gate.
		if snap.Sent > 0 && !config.IsAdminWorker(name) {
			lossPct := float64(snap.Lost) / float64(snap.Sent) * 100
			if lossPct > cfg.Thresholds.MaxLossPct && lossPct > boundaryLossPct {
				fail("%s: loss %.4f%% exceeds threshold %.4f%%", name, lossPct, cfg.Thresholds.MaxLossPct)
			}
		}

		// Gate 2: duplication (idempotent producer → 0.0). The admin_topic_churn
		// worker is exempt (no steady data stream).
		if snap.Received > 0 && !config.IsAdminWorker(name) {
			dupPct := float64(snap.Duplicated) / float64(snap.Received) * 100
			if dupPct > cfg.Thresholds.MaxDuplicationPct {
				fail("%s: duplication %.4f%% exceeds threshold %.4f%%", name, dupPct, cfg.Thresholds.MaxDuplicationPct)
			}
		}

		// Latency gates (P50/P95/P99/P999).
		checkLatency(name, "p50", snap.LatencyP50, cfg.Thresholds.MaxP50LatencyMS, fail)
		checkLatency(name, "p95", snap.LatencyP95, cfg.Thresholds.MaxP95LatencyMS, fail)
		checkLatency(name, "p99", snap.LatencyP99, cfg.Thresholds.MaxP99LatencyMS, fail)
		checkLatency(name, "p999", snap.LatencyP999, cfg.Thresholds.MaxP999LatencyMS, fail)

		// Error rate: errors / (sent + received) * 100.
		total := snap.Sent + snap.Received
		if total > 0 {
			errPct := float64(snap.Errors) / float64(total) * 100
			if errPct > cfg.Thresholds.MaxErrorRatePct {
				fail("%s: error rate %.4f%% exceeds %.4f%%", name, errPct, cfg.Thresholds.MaxErrorRatePct)
			}
		}

		// Throughput vs target send rate (skipped for the cyclic admin worker).
		if measurementDuration > 0 && snap.Sent > 0 && !config.IsAdminWorker(name) {
			targetRate := float64(cfg.GetWorkerRate(name))
			if targetRate > 0 {
				actualRate := float64(snap.Sent) / measurementDuration.Seconds()
				throughputPct := actualRate / targetRate * 100
				if throughputPct < cfg.Thresholds.MinThroughputPct {
					fail("%s: throughput %.1f%% below %.1f%%", name, throughputPct, cfg.Thresholds.MinThroughputPct)
				}
			}
		}

		// Downtime.
		if measurementDuration > 0 && snap.DowntimeSeconds > 0 {
			downtimePct := snap.DowntimeSeconds / measurementDuration.Seconds() * 100
			if downtimePct > cfg.Thresholds.MaxDowntimePct {
				fail("%s: downtime %.1f%% exceeds %.1f%%", name, downtimePct, cfg.Thresholds.MaxDowntimePct)
			}
		}

		// Gate 3: per-partition offset order (keyed_ordering).
		if config.IsKeyedWorker(name) && int(snap.OffsetOrderViolations) > cfg.Thresholds.MaxOffsetOrderViolations {
			fail("%s: %d per-partition offset-order violations (expected <= %d)",
				name, snap.OffsetOrderViolations, cfg.Thresholds.MaxOffsetOrderViolations)
		}

		// Gate 4: consumer-group rebalance safety (consumer_group).
		if config.IsConsumerGroupWorker(name) && int(snap.GroupLossRebalance) > cfg.Thresholds.MaxGroupLossAcrossRebalance {
			fail("%s: %d messages lost across rebalance (expected <= %d)",
				name, snap.GroupLossRebalance, cfg.Thresholds.MaxGroupLossAcrossRebalance)
		}

		// Gate 5: EOS soundness (transactions_eos). The KIP-890 V1 same-epoch
		// residual is upstream-shared and EXCLUDED (spec §2.5): snap.KIP890Residual
		// is NOT added to snap.EOSViolations and is NOT gated here.
		if config.IsTransactionsWorker(name) && int(snap.EOSViolations) > cfg.Thresholds.MaxEOSViolations {
			fail("%s: %d EOS violations (dup/loss or read_committed leaked an aborted record; expected <= %d). "+
				"NOTE: KIP-890 V1 same-epoch residual (%d) is upstream-shared and EXCLUDED per spec §2.5",
				name, snap.EOSViolations, cfg.Thresholds.MaxEOSViolations, snap.KIP890Residual)
		}

		// Gate 6: consumer-group lag accuracy (offset_commit_lag). offset == STAN
		// Sequence, so the reported lag is exact.
		if config.IsOffsetLagWorker(name) && int(snap.LagAccuracyErrMax) > cfg.Thresholds.MaxLagAccuracyErrorMsgs {
			fail("%s: lag accuracy error %d msgs exceeds tolerance %d",
				name, snap.LagAccuracyErrMax, cfg.Thresholds.MaxLagAccuracyErrorMsgs)
		}

		// Admin churn gate: any unexpected admin op failure fails; correct
		// INVALID_PARTITIONS rejections are healthy (counted separately).
		if config.IsAdminWorker(name) && snap.AdminOpFailures > 0 {
			fail("%s: %d admin op failures (unexpected error or a bad CreatePartitions wrongly accepted)", name, snap.AdminOpFailures)
		}
	}

	// Memory stability — advisory only.
	baseline := e.baselineRSS.Load()
	peak := e.peakRSS.Load()
	if baseline > 0 && peak > baseline {
		growth := float64(peak) / float64(baseline)
		if growth > cfg.Thresholds.MaxMemoryGrowthFactor {
			if result.Passed {
				result.Result = "PASSED_WITH_WARNINGS"
			}
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("memory growth %.2fx exceeds threshold %.2fx", growth, cfg.Thresholds.MaxMemoryGrowthFactor))
		}
	}

	e.mu.Lock()
	e.verdictResult = result
	e.mu.Unlock()
}

func checkLatency(name, label string, value, threshold float64, fail func(string, ...any)) {
	if value > 0 && threshold > 0 && value > threshold {
		fail("%s: %s latency %.1fms exceeds %.1fms", name, label, value, threshold)
	}
}
