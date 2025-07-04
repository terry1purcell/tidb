// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package errors

import (
	"context"
	stderrors "errors"

	"github.com/pingcap/errors"
)

// Is tests whether the specificated error causes the error `err`.
func Is(err error, is *errors.Error) bool {
	errorFound := errors.Find(err, func(e error) bool {
		normalizedErr, ok := e.(*errors.Error)
		return ok && normalizedErr.ID() == is.ID()
	})
	return errorFound != nil
}

// IsContextCanceled checks whether the is caused by context.Canceled.
// errors.Cause does not work for the error wrapped by %w in fmt.Errorf.
// So we need to call stderrors.Is to unwrap the error.
func IsContextCanceled(err error) bool {
	err = errors.Cause(err)
	if err == context.Canceled || err == context.DeadlineExceeded {
		return true
	}
	return stderrors.Is(err, context.Canceled) || stderrors.Is(err, context.DeadlineExceeded)
}

// BR errors.
var (
	ErrUnknown                      = errors.Normalize("internal error", errors.RFCCodeText("BR:Common:ErrUnknown"))
	ErrInvalidArgument              = errors.Normalize("invalid argument", errors.RFCCodeText("BR:Common:ErrInvalidArgument"))
	ErrUndefinedRestoreDbOrTable    = errors.Normalize("undefined restore databases or tables", errors.RFCCodeText("BR:Common:ErrUndefinedDbOrTable"))
	ErrVersionMismatch              = errors.Normalize("version mismatch", errors.RFCCodeText("BR:Common:ErrVersionMismatch"))
	ErrFailedToConnect              = errors.Normalize("failed to make gRPC channels", errors.RFCCodeText("BR:Common:ErrFailedToConnect"))
	ErrInvalidMetaFile              = errors.Normalize("invalid metafile: %s", errors.RFCCodeText("BR:Common:ErrInvalidMetaFile"))
	ErrEnvNotSpecified              = errors.Normalize("environment variable not found", errors.RFCCodeText("BR:Common:ErrEnvNotSpecified"))
	ErrUnsupportedOperation         = errors.Normalize("the operation is not supported", errors.RFCCodeText("BR:Common:ErrUnsupportedOperation"))
	ErrInvalidRange                 = errors.Normalize("invalid restore range", errors.RFCCodeText("BR:Common:ErrInvalidRange"))
	ErrMigrationNotFound            = errors.Normalize("no migration found", errors.RFCCodeText("BR:Common:ErrMigrationNotFound"))
	ErrMigrationVersionNotSupported = errors.Normalize("the migration version isn't supported", errors.RFCCodeText("BR:Common:ErrMigrationVersionNotSupported"))

	ErrPDUpdateFailed         = errors.Normalize("failed to update PD", errors.RFCCodeText("BR:PD:ErrPDUpdateFailed"))
	ErrPDLeaderNotFound       = errors.Normalize("PD leader not found", errors.RFCCodeText("BR:PD:ErrPDLeaderNotFound"))
	ErrPDInvalidResponse      = errors.Normalize("PD invalid response", errors.RFCCodeText("BR:PD:ErrPDInvalidResponse"))
	ErrPDBatchScanRegion      = errors.Normalize("batch scan region", errors.RFCCodeText("BR:PD:ErrPDBatchScanRegion"))
	ErrPDUnknownScatterResult = errors.Normalize("failed to wait region scattered", errors.RFCCodeText("BR:PD:ErrPDUknownScatterResult"))
	ErrPDNotFullyScatter      = errors.Normalize("pd not fully scattered", errors.RFCCodeText("BR:PD:ErrPDNotFullyScatter"))
	ErrPDSplitFailed          = errors.Normalize("failed to wait region splitted", errors.RFCCodeText("BR:PD:ErrPDUknownScatterResult"))

	ErrBackupChecksumMismatch    = errors.Normalize("backup checksum mismatch", errors.RFCCodeText("BR:Backup:ErrBackupChecksumMismatch"))
	ErrBackupInvalidRange        = errors.Normalize("backup range invalid", errors.RFCCodeText("BR:Backup:ErrBackupInvalidRange"))
	ErrBackupNoLeader            = errors.Normalize("backup no leader", errors.RFCCodeText("BR:Backup:ErrBackupNoLeader"))
	ErrBackupGCSafepointExceeded = errors.Normalize("backup GC safepoint exceeded", errors.RFCCodeText("BR:Backup:ErrBackupGCSafepointExceeded"))
	ErrBackupKeyIsLocked         = errors.Normalize("backup key is locked", errors.RFCCodeText("BR:Backup:ErrBackupKeyIsLocked"))
	ErrBackupRegion              = errors.Normalize("backup region error", errors.RFCCodeText("BR:Backup:ErrBackupRegion"))

	ErrRestoreModeMismatch       = errors.Normalize("restore mode mismatch", errors.RFCCodeText("BR:Restore:ErrRestoreModeMismatch"))
	ErrRestoreRangeMismatch      = errors.Normalize("restore range mismatch", errors.RFCCodeText("BR:Restore:ErrRestoreRangeMismatch"))
	ErrRestoreCheckpointMismatch = errors.Normalize("restore checkpoint mismatch", errors.RFCCodeText("BR:Restore:ErrRestoreCheckpointMismatch"))
	ErrRestoreChecksumMismatch   = errors.Normalize("restore checksum mismatch", errors.RFCCodeText("BR:Restore:ErrRestoreChecksumMismatch"))
	ErrRestoreTableIDMismatch    = errors.Normalize("restore table ID mismatch", errors.RFCCodeText("BR:Restore:ErrRestoreTableIDMismatch"))
	ErrRestoreRejectStore        = errors.Normalize("failed to restore remove rejected store", errors.RFCCodeText("BR:Restore:ErrRestoreRejectStore"))
	ErrRestoreNoPeer             = errors.Normalize("region does not have peer", errors.RFCCodeText("BR:Restore:ErrRestoreNoPeer"))
	ErrRestoreSplitFailed        = errors.Normalize("fail to split region", errors.RFCCodeText("BR:Restore:ErrRestoreSplitFailed"))
	ErrRestoreInvalidRewrite     = errors.Normalize("invalid rewrite rule", errors.RFCCodeText("BR:Restore:ErrRestoreInvalidRewrite"))
	ErrRestoreInvalidBackup      = errors.Normalize("invalid backup", errors.RFCCodeText("BR:Restore:ErrRestoreInvalidBackup"))
	ErrRestoreWriteAndIngest     = errors.Normalize("failed to write and ingest", errors.RFCCodeText("BR:Restore:ErrRestoreWriteAndIngest"))
	ErrRestoreSchemaNotExists    = errors.Normalize("schema not exists", errors.RFCCodeText("BR:Restore:ErrRestoreSchemaNotExists"))
	ErrRestoreNotFreshCluster    = errors.Normalize("cluster is not fresh", errors.RFCCodeText("BR:Restore:ErrRestoreNotFreshCluster"))
	ErrRestoreIncompatibleSys    = errors.Normalize("incompatible system table", errors.RFCCodeText("BR:Restore:ErrRestoreIncompatibleSys"))
	ErrUnsupportedSystemTable    = errors.Normalize("the system table isn't supported for restoring yet", errors.RFCCodeText("BR:Restore:ErrUnsupportedSysTable"))
	ErrDatabasesAlreadyExisted   = errors.Normalize("databases already existed in restored cluster", errors.RFCCodeText("BR:Restore:ErrDatabasesAlreadyExisted"))
	ErrTablesAlreadyExisted      = errors.Normalize("tables already existed in restored cluster", errors.RFCCodeText("BR:Restore:ErrTablesAlreadyExisted"))

	// ErrStreamLogTaskExist is the error when stream log task already exists, because of supporting single task currently.
	ErrStreamLogTaskExist        = errors.Normalize("stream task already exists", errors.RFCCodeText("BR:Stream:ErrStreamLogTaskExist"))
	ErrStreamLogTaskHasNoStorage = errors.Normalize("stream task has no storage", errors.RFCCodeText("BR:Stream:ErrStreamLogTaskHasNoStorage"))

	// TODO maybe it belongs to PiTR.
	ErrRestoreRTsConstrain = errors.Normalize("resolved ts constrain violation", errors.RFCCodeText("BR:Restore:ErrRestoreResolvedTsConstrain"))

	ErrPiTRInvalidCDCLogFormat = errors.Normalize("invalid cdc log format", errors.RFCCodeText("BR:PiTR:ErrPiTRInvalidCDCLogFormat"))
	ErrPiTRTaskNotFound        = errors.Normalize("task not found", errors.RFCCodeText("BR:PiTR:ErrTaskNotFound"))
	ErrPiTRInvalidTaskInfo     = errors.Normalize("task info is invalid", errors.RFCCodeText("BR:PiTR:ErrInvalidTaskInfo"))
	ErrPiTRMalformedMetadata   = errors.Normalize("malformed metadata", errors.RFCCodeText("BR:PiTR:ErrMalformedMetadata"))

	ErrStorageUnknown           = errors.Normalize("unknown external storage error", errors.RFCCodeText("BR:ExternalStorage:ErrStorageUnknown"))
	ErrStorageInvalidConfig     = errors.Normalize("invalid external storage config", errors.RFCCodeText("BR:ExternalStorage:ErrStorageInvalidConfig"))
	ErrStorageInvalidPermission = errors.Normalize("external storage permission", errors.RFCCodeText("BR:ExternalStorage:ErrStorageInvalidPermission"))

	// Snapshot restore
	ErrRestoreTotalKVMismatch   = errors.Normalize("restore total tikvs mismatch", errors.RFCCodeText("BR:EBS:ErrRestoreTotalKVMismatch"))
	ErrRestoreInvalidPeer       = errors.Normalize("restore met a invalid peer", errors.RFCCodeText("BR:EBS:ErrRestoreInvalidPeer"))
	ErrRestoreRegionWithoutPeer = errors.Normalize("restore met a region without any peer", errors.RFCCodeText("BR:EBS:ErrRestoreRegionWithoutPeer"))

	// Errors reported from TiKV.
	ErrKVStorage           = errors.Normalize("tikv storage occur I/O error", errors.RFCCodeText("BR:KV:ErrKVStorage"))
	ErrKVUnknown           = errors.Normalize("unknown error occur on tikv", errors.RFCCodeText("BR:KV:ErrKVUnknown"))
	ErrKVClusterIDMismatch = errors.Normalize("tikv cluster ID mismatch", errors.RFCCodeText("BR:KV:ErrKVClusterIDMismatch"))
	ErrKVNotLeader         = errors.Normalize("not leader", errors.RFCCodeText("BR:KV:ErrKVNotLeader"))
	ErrKVNotTiKV           = errors.Normalize("storage is not tikv", errors.RFCCodeText("BR:KV:ErrNotTiKVStorage"))
	ErrKVDiskFull          = errors.Normalize("disk is full", errors.RFCCodeText("BR:KV:ErrKVDiskFull"))

	// ErrKVEpochNotMatch is the error raised when ingestion failed with "epoch
	// not match". This error is retryable.
	ErrKVEpochNotMatch = errors.Normalize("epoch not match", errors.RFCCodeText("BR:KV:ErrKVEpochNotMatch"))
	// ErrKVKeyNotInRegion is the error raised when ingestion failed with "key not
	// in region". This error cannot be retried.
	ErrKVKeyNotInRegion = errors.Normalize("key not in region", errors.RFCCodeText("BR:KV:ErrKVKeyNotInRegion"))
	// ErrKVRewriteRuleNotFound is the error raised when download failed with
	// "rewrite rule not found". This error cannot be retried
	ErrKVRewriteRuleNotFound = errors.Normalize("rewrite rule not found", errors.RFCCodeText("BR:KV:ErrKVRewriteRuleNotFound"))
	// ErrKVRangeIsEmpty is the error raised when download failed with "range is
	// empty". This error cannot be retried.
	ErrKVRangeIsEmpty = errors.Normalize("range is empty", errors.RFCCodeText("BR:KV:ErrKVRangeIsEmpty"))
	// ErrKVDownloadFailed indicates a generic download error, expected to be
	// retryable.
	ErrKVDownloadFailed = errors.Normalize("download sst failed", errors.RFCCodeText("BR:KV:ErrKVDownloadFailed"))
	// ErrKVIngestFailed indicates a generic, retryable ingest error.
	ErrKVIngestFailed = errors.Normalize("ingest sst failed", errors.RFCCodeText("BR:KV:ErrKVIngestFailed"))

	ErrPossibleInconsistency = errors.Normalize("the cluster state might be inconsistent", errors.RFCCodeText("BR:KV:ErrPossibleInconsistency"))
)
