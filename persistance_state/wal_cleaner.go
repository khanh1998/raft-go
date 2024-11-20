package persistance_state

import (
	"context"
	"fmt"
	"khanh/raft-go/common"
	"sort"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// term and index of the last log appears in the previous WAL.
// in order to know term and index of the last log of the first WAL,
// we read the key `prev_wal_last_log` in the second WAL,
// this way we don't need to read the whole first WAL to know the last log.
// and the key appears exact one time in each WAL, usually in the beginning of the file,
// so we just need to read the first occurrence of the key, and it's fast to get the key.
const prevWalLastLogInfoKey = "prev_wal_last_log"

// this function can be triggered to be running in background every time a new snapshot get committed.
// this is a bad design, when it violate the separation-of-concern principle,
// but currently i haven't figure out any better design.
func (r *RaftPersistanceStateImpl) cleanupWal(ctx context.Context, sm common.SnapshotMetadata) error {
	ctx, span := tracer.Start(ctx, "CleanupWAL")
	defer span.End()

	metadata, fileNames, err := r.storage.GetWalMetadata([]string{prevWalLastLogInfoKey})
	if err != nil {
		r.log().ErrorContext(ctx, "WalCleanup_GetWalMetadata", err)

		return err
	}

	r.log().DebugContext(ctx, "CleanupWAL", "snapshot", sm, "metadata", metadata, "fileNames", fileNames)

	toBeDeletedWal := []string{}

	for i, fileName := range fileNames {
		if i == len(fileNames)-1 {
			break // skip the last wal, we can't delete it anyway
		}

		md := metadata[i+1] // metadata of ith WAL contains index and term of the last log of (i-1)th WAL
		if len(md) == 2 {
			key, value := md[0], md[1]
			if key == prevWalLastLogInfoKey {
				var term, index int
				_, err := fmt.Sscanf(value, "%d|%d", &term, &index)
				if err != nil {
					r.log().ErrorContext(ctx, "WalCleanup_Sscanf", err)
					return err
				}

				if term < sm.LastLogTerm {
					toBeDeletedWal = append(toBeDeletedWal, fileName)
				} else if term == sm.LastLogTerm && index <= sm.LastLogIndex {
					toBeDeletedWal = append(toBeDeletedWal, fileName)
				}
			}
		}
	}

	sort.Strings(toBeDeletedWal)

	if len(toBeDeletedWal) > 0 {
		fileName := toBeDeletedWal[len(toBeDeletedWal)-1]

		err = r.storage.DeleteWALsOlderOrEqual(fileName)
		if err != nil {
			r.log().ErrorContext(ctx, "WalCleanup_DeleteObject", err)
			span.AddEvent(
				"delete WAL failed",
				trace.WithAttributes(
					attribute.String("error", err.Error()),
					attribute.String("files", fileName),
				),
			)
		} else {
			span.AddEvent(
				"delete WAL success",
				trace.WithAttributes(
					attribute.String("file", fileName),
				),
			)
		}
	}

	r.log().InfoContext(ctx, "cleanupWAL finished", "deleted_wal", toBeDeletedWal, "metadata", sm, "error", err)

	return err
}
