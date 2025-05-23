// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package version

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/version/build"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tidb/pkg/util/engine"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	"go.uber.org/zap"
)

var (
	minTiKVVersion          = semver.New("3.1.0-beta.2")
	incompatibleTiKVMajor3  = semver.New("3.1.0")
	incompatibleTiKVMajor4  = semver.New("4.0.0-rc.1")
	compatibleTiFlashMajor3 = semver.New("3.1.0")
	compatibleTiFlashMajor4 = semver.New("4.0.0")

	versionHash = regexp.MustCompile("-[0-9]+-g[0-9a-f]{7,}")

	checkpointSupportError error = nil
	// pitrSupportBatchKVFiles specifies whether TiKV-server supports batch PITR.
	pitrSupportBatchKVFiles bool = false

	// Once TableInfoVersion updated. BR need to check compatibility with
	// new TableInfoVersion. both snapshot restore and pitr need to be checked.
	CURRENT_BACKUP_SUPPORT_TABLE_INFO_VERSION = model.TableInfoVersion5
)

// NextMajorVersion returns the next major version.
func NextMajorVersion() semver.Version {
	nextMajorVersion, err := semver.NewVersion(removeVAndHash(build.ReleaseVersion))
	if err != nil {
		// build.ReleaseVersion is unknown, assuming infinitely-new nightly version.
		return semver.Version{Major: math.MaxInt64, PreRelease: "nightly"}
	}
	nextMajorVersion.BumpMajor()
	return *nextMajorVersion
}

// removeVAndHash sanitizes a version string.
func removeVAndHash(v string) string {
	v = versionHash.ReplaceAllLiteralString(v, "")
	v = strings.TrimSuffix(v, "-dirty")
	return strings.TrimPrefix(v, "v")
}

func checkTiFlashVersion(store *metapb.Store) error {
	flash, err := semver.NewVersion(removeVAndHash(store.Version))
	if err != nil {
		return errors.Annotatef(berrors.ErrVersionMismatch, "failed to parse TiFlash %s version %s, err %s",
			store.GetPeerAddress(), store.Version, err)
	}

	if flash.Major == 3 && flash.LessThan(*compatibleTiFlashMajor3) {
		return errors.Annotatef(berrors.ErrVersionMismatch, "incompatible TiFlash %s version %s, try update it to %s",
			store.GetPeerAddress(), store.Version, compatibleTiFlashMajor3)
	}

	if flash.Major == 4 && flash.LessThan(*compatibleTiFlashMajor4) {
		return errors.Annotatef(berrors.ErrVersionMismatch, "incompatible TiFlash %s version %s, try update it to %s",
			store.GetPeerAddress(), store.Version, compatibleTiFlashMajor4)
	}

	return nil
}

// VerChecker is a callback for the CheckClusterVersion, decides whether the cluster is suitable to execute restore.
// See also: CheckVersionForBackup and CheckVersionForBR.
type VerChecker func(store *metapb.Store, ver *semver.Version) error

// CheckClusterVersion check TiKV version.
func CheckClusterVersion(ctx context.Context, client pd.Client, checker VerChecker) error {
	stores, err := client.GetAllStores(ctx, opt.WithExcludeTombstone())
	if err != nil {
		return errors.Trace(err)
	}
	for _, s := range stores {
		isTiFlash := engine.IsTiFlash(s)
		log.Debug("checking compatibility of store in cluster",
			zap.Uint64("ID", s.GetId()),
			zap.Bool("TiFlash?", isTiFlash),
			zap.String("address", s.GetAddress()),
			zap.String("version", s.GetVersion()),
		)
		if isTiFlash {
			if err := checkTiFlashVersion(s); err != nil {
				return errors.Trace(err)
			}
			continue
		}

		tikvVersionString := removeVAndHash(s.Version)
		tikvVersion, getVersionErr := semver.NewVersion(tikvVersionString)
		if getVersionErr != nil {
			return errors.Annotatef(berrors.ErrVersionMismatch, "%s: TiKV node %s version %s is invalid", getVersionErr, s.Address, tikvVersionString)
		}
		if checkerErr := checker(s, tikvVersion); checkerErr != nil {
			return checkerErr
		}
	}
	return nil
}

// CheckVersionForBackup checks the version for backup and
func CheckVersionForBackup(backupVersion *semver.Version) VerChecker {
	return func(store *metapb.Store, ver *semver.Version) error {
		if backupVersion.Major > ver.Major && backupVersion.Major-ver.Major > 1 {
			return errors.Annotatef(berrors.ErrVersionMismatch,
				"backup with cluster version %s cannot be restored at cluster of version %s: major version mismatches",
				backupVersion, ver)
		}
		return nil
	}
}

// CheckVersionForBRPiTR checks whether version of the cluster and BR-pitr itself is compatible.
// Note: BR'version >= 6.1.0 at least in this function
func CheckVersionForBRPiTR(s *metapb.Store, tikvVersion *semver.Version) error {
	if build.ReleaseVersion == build.ReleaseVersionForTest {
		return nil
	}
	BRVersion, err := semver.NewVersion(removeVAndHash(build.ReleaseVersion))
	if err != nil {
		return errors.Annotatef(berrors.ErrVersionMismatch, "%s: invalid version, please recompile using `git fetch origin --tags && make build`", err)
	}

	// tikvVersion should at least 6.1.0
	if tikvVersion.Major < 6 || (tikvVersion.Major == 6 && tikvVersion.Minor == 0) {
		return errors.Annotatef(berrors.ErrVersionMismatch, "TiKV node %s version %s is too low when use PiTR, please update tikv's version to at least v6.1.0(v6.2.0+ recommanded)",
			s.Address, tikvVersion)
	}
	// If tikv version < 6.5, PITR do not support restoring batch kv files.
	if tikvVersion.Major < 6 || (tikvVersion.Major == 6 && tikvVersion.Minor < 5) {
		pitrSupportBatchKVFiles = false
	} else {
		pitrSupportBatchKVFiles = true
	}

	// The versions of BR and TiKV should be the same when use BR 6.1.0
	if BRVersion.Major == 6 && BRVersion.Minor == 1 {
		if tikvVersion.Major != 6 || tikvVersion.Minor != 1 {
			return errors.Annotatef(berrors.ErrVersionMismatch, "TiKV node %s version %s and BR %s version mismatch when use PiTR v6.1.0, please use the same version of BR",
				s.Address, tikvVersion, build.ReleaseVersion)
		}
	} else {
		// If BRVersion > v6.1.0, the version of TiKV should be at least v6.2.0
		if tikvVersion.Major == 6 && tikvVersion.Minor <= 1 {
			return errors.Annotatef(berrors.ErrVersionMismatch, "TiKV node %s version %s and BR %s version mismatch when use PiTR v6.2.0+, please use the tikv with version v6.2.0+",
				s.Address, tikvVersion, build.ReleaseVersion)
		}
	}
	return nil
}

// CheckVersionForDDL checks whether we use queue or table to execute ddl during restore.
func CheckVersionForDDL(s *metapb.Store, tikvVersion *semver.Version) error {
	// use tikvVersion instead of tidbVersion since br doesn't have mysql client to connect tidb.
	requireVersion := semver.New("6.2.0-alpha")
	if tikvVersion.Compare(*requireVersion) < 0 {
		return errors.Errorf("detected the old version of tidb cluster, require: >= 6.2.0, but got %s", tikvVersion.String())
	}
	return nil
}

// CheckVersionForKeyspaceBR checks whether the cluster is support Backup/Restore keyspace data.
func CheckVersionForKeyspaceBR(_ *metapb.Store, tikvVersion *semver.Version) error {
	requireVersion := semver.New("6.6.0-alpha")
	if tikvVersion.Compare(*requireVersion) < 0 {
		return errors.Errorf("detected the old version of tidb cluster, require: >= 6.6.0, but got %s", tikvVersion.String())
	}
	return nil
}

// CheckVersionForBR checks whether version of the cluster and BR itself is compatible.
func CheckVersionForBR(s *metapb.Store, tikvVersion *semver.Version) error {
	if build.ReleaseVersion == build.ReleaseVersionForTest {
		return nil
	}
	BRVersion, err := semver.NewVersion(removeVAndHash(build.ReleaseVersion))
	if err != nil {
		return errors.Annotatef(berrors.ErrVersionMismatch, "%s: invalid version, please recompile using `git fetch origin --tags && make build`", err)
	}

	if tikvVersion.Compare(*minTiKVVersion) < 0 {
		return errors.Annotatef(berrors.ErrVersionMismatch, "TiKV node %s version %s don't support BR, please upgrade cluster to %s",
			s.Address, tikvVersion, build.ReleaseVersion)
	}

	// BR 6.x works with TiKV 5.x and not guarantee works with 4.x
	if BRVersion.Major < tikvVersion.Major || BRVersion.Major-tikvVersion.Major > 1 {
		return errors.Annotatef(berrors.ErrVersionMismatch, "TiKV node %s version %s and BR %s major version mismatch, please use the same version of BR",
			s.Address, tikvVersion, build.ReleaseVersion)
	}

	// BR(https://github.com/pingcap/br/pull/233) and TiKV(https://github.com/tikv/tikv/pull/7241) have breaking changes
	// if BR include #233 and TiKV not include #7241, BR will panic TiKV during restore
	// These incompatible version is 3.1.0 and 4.0.0-rc.1
	if tikvVersion.Major == 3 {
		if tikvVersion.Compare(*incompatibleTiKVMajor3) < 0 && BRVersion.Compare(*incompatibleTiKVMajor3) >= 0 {
			return errors.Annotatef(berrors.ErrVersionMismatch, "TiKV node %s version %s and BR %s version mismatch, please use the same version of BR",
				s.Address, tikvVersion, build.ReleaseVersion)
		}
	}

	if tikvVersion.Major == 4 {
		if tikvVersion.Compare(*incompatibleTiKVMajor4) < 0 && BRVersion.Compare(*incompatibleTiKVMajor4) >= 0 {
			return errors.Annotatef(berrors.ErrVersionMismatch, "TiKV node %s version %s and BR %s version mismatch, please use the same version of BR",
				s.Address, tikvVersion, build.ReleaseVersion)
		}
	}

	// reset the checkpoint support error
	checkpointSupportError = nil
	if tikvVersion.Major < 6 || (tikvVersion.Major == 6 && tikvVersion.Minor < 5) {
		// checkpoint mode only support after v6.5.0
		checkpointSupportError = errors.Annotatef(berrors.ErrVersionMismatch, "TiKV node %s version %s is too low when use checkpoint, please update tikv's version to at least v6.5.0",
			s.Address, tikvVersion)
	}

	// don't warn if we are the master build, which always have the version v4.0.0-beta.2-*
	if build.GitBranch != "master" && tikvVersion.Compare(*BRVersion) > 0 {
		log.Warn(fmt.Sprintf("BR version is outdated, please consider use version %s of BR", tikvVersion))
	}
	return nil
}

// CheckVersion checks if the actual version is within [requiredMinVersion, requiredMaxVersion).
func CheckVersion(component string, actual, requiredMinVersion, requiredMaxVersion semver.Version) error {
	if actual.Compare(requiredMinVersion) < 0 {
		return errors.Annotatef(berrors.ErrVersionMismatch,
			"%s version too old, required to be in [%s, %s), found '%s'",
			component,
			requiredMinVersion,
			requiredMaxVersion,
			actual,
		)
	}
	// Compare the major version number to make sure beta version does not pass
	// the check. This is because beta version may contains incompatible
	// changes.
	if actual.Major >= requiredMaxVersion.Major {
		return errors.Annotatef(berrors.ErrVersionMismatch,
			"%s version too new, expected to be within [%s, %d.0.0), found '%s'",
			component,
			requiredMinVersion,
			requiredMaxVersion.Major,
			actual,
		)
	}
	return nil
}

// ExtractTiDBVersion extracts TiDB version from TiDB SQL `version()` outputs.
func ExtractTiDBVersion(version string) (*semver.Version, error) {
	// version format: "5.7.10-TiDB-v2.1.0-rc.1-7-g38c939f"
	//                               ^~~~~~~~~^ we only want this part
	// version format: "5.7.10-TiDB-v2.0.4-1-g06a0bf5"
	//                               ^~~~^
	// version format: "5.7.10-TiDB-v2.0.7"
	//                               ^~~~^
	// version format: "5.7.25-TiDB-v3.0.0-beta-211-g09beefbe0-dirty"
	//                               ^~~~~~~~~^
	// The version is generated by `git describe --tags` on the TiDB repository.
	versions := strings.Split(strings.TrimSuffix(version, "-dirty"), "-")
	end := len(versions)
	switch end {
	case 3, 4:
	case 5, 6:
		end -= 2
	default:
		return nil, errors.Annotatef(berrors.ErrVersionMismatch, "not a valid TiDB version: %s", version)
	}
	rawVersion := strings.Join(versions[2:end], "-")
	rawVersion = strings.TrimPrefix(rawVersion, "v")
	return semver.NewVersion(rawVersion)
}

// CheckTiDBVersion is equals to ExtractTiDBVersion followed by CheckVersion.
func CheckTiDBVersion(versionStr string, requiredMinVersion, requiredMaxVersion semver.Version) error {
	serverInfo := ParseServerInfo(versionStr)
	if serverInfo.ServerType != ServerTypeTiDB {
		return errors.Errorf("server with version '%s' is not TiDB", versionStr)
	}
	return CheckVersion("TiDB", *serverInfo.ServerVersion, requiredMinVersion, requiredMaxVersion)
}

// NormalizeBackupVersion normalizes the version string from backupmeta.
func NormalizeBackupVersion(version string) *semver.Version {
	// We need to unquote here because we get the version from PD HTTP API,
	// which returns quoted string.
	trimmedVerStr := strings.TrimSpace(version)
	unquotedVerStr, err := strconv.Unquote(trimmedVerStr)
	if err != nil {
		unquotedVerStr = trimmedVerStr
	}
	normalizedVerStr := strings.TrimSpace(unquotedVerStr)
	ver, err := semver.NewVersion(normalizedVerStr)
	if err != nil {
		log.Warn("cannot parse backup version", zap.String("version", normalizedVerStr), zap.Error(err))
	}
	return ver
}

// FetchVersion gets the version information from the database server
//
// NOTE: the executed query will be:
// - `select tidb_version()` if target db is tidb
// - `select version()` if target db is not tidb
func FetchVersion(ctx context.Context, db dbutil.QueryExecutor) (string, error) {
	var versionInfo string
	const queryTiDB = "SELECT tidb_version();"
	tidbRow := db.QueryRowContext(ctx, queryTiDB)
	err := tidbRow.Scan(&versionInfo)
	if err == nil && tidbReleaseVersionFullRegex.FindString(versionInfo) != "" {
		return versionInfo, nil
	}
	log.L().Warn("select tidb_version() failed, will fallback to 'select version();'", logutil.ShortError(err))
	const query = "SELECT version();"
	row := db.QueryRowContext(ctx, query)
	err = row.Scan(&versionInfo)
	if err != nil {
		return "", errors.Annotatef(err, "sql: %s", query)
	}
	return versionInfo, nil
}

func CheckCheckpointSupport() error {
	return checkpointSupportError
}

func CheckPITRSupportBatchKVFiles() bool {
	return pitrSupportBatchKVFiles
}

type ServerType int

const (
	// ServerTypeUnknown represents unknown server type
	ServerTypeUnknown = iota
	// ServerTypeMySQL represents MySQL server type
	ServerTypeMySQL
	// ServerTypeMariaDB represents MariaDB server type
	ServerTypeMariaDB
	// ServerTypeTiDB represents TiDB server type
	ServerTypeTiDB

	// ServerTypeAll represents All server types
	ServerTypeAll
)

var serverTypeString = []string{
	ServerTypeUnknown: "Unknown",
	ServerTypeMySQL:   "MySQL",
	ServerTypeMariaDB: "MariaDB",
	ServerTypeTiDB:    "TiDB",
}

// String implements Stringer.String
func (s ServerType) String() string {
	if s >= ServerTypeAll {
		return ""
	}
	return serverTypeString[s]
}

// ServerInfo is the combination of ServerType and ServerInfo
type ServerInfo struct {
	ServerType    ServerType
	ServerVersion *semver.Version
	HasTiKV       bool
}

var (
	mysqlVersionRegex = regexp.MustCompile(`^\d+\.\d+\.\d+([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?`)
	// `select version()` result
	tidbVersionRegex = regexp.MustCompile(`-[v]?\d+\.\d+\.\d+([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?`)
	// `select tidb_version()` result
	tidbReleaseVersionRegex = regexp.MustCompile(`v\d+\.\d+\.\d+([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?`)
	// `select tidb_version()` result with full release version
	tidbReleaseVersionFullRegex = regexp.MustCompile(`Release Version:\s*v\d+\.\d+\.\d+([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?`)
)

// ParseServerInfo parses exported server type and version info from version string
func ParseServerInfo(src string) ServerInfo {
	lowerCase := strings.ToLower(src)
	serverInfo := ServerInfo{}
	isReleaseVersion := false
	switch {
	case strings.Contains(lowerCase, "release version:"):
		// this version string is tidb release version
		serverInfo.ServerType = ServerTypeTiDB
		isReleaseVersion = true
	case strings.Contains(lowerCase, "tidb"):
		serverInfo.ServerType = ServerTypeTiDB
	case strings.Contains(lowerCase, "mariadb"):
		serverInfo.ServerType = ServerTypeMariaDB
	case mysqlVersionRegex.MatchString(lowerCase):
		serverInfo.ServerType = ServerTypeMySQL
	default:
		serverInfo.ServerType = ServerTypeUnknown
	}

	var versionStr string
	if serverInfo.ServerType == ServerTypeTiDB {
		if isReleaseVersion {
			versionStr = tidbReleaseVersionRegex.FindString(src)
		} else {
			versionStr = tidbVersionRegex.FindString(src)
			versionStr = strings.TrimPrefix(versionStr, "-")
		}
		versionStr = strings.TrimPrefix(versionStr, "v")
	} else {
		versionStr = mysqlVersionRegex.FindString(src)
	}

	var err error
	serverInfo.ServerVersion, err = semver.NewVersion(versionStr)
	if err != nil {
		log.L().Warn("fail to parse version, fallback to 0.0.0",
			zap.String("version", versionStr))
		serverInfo.ServerVersion = semver.New("0.0.0")
	}

	log.L().Info("detect server version",
		zap.String("type", serverInfo.ServerType.String()),
		zap.String("version", serverInfo.ServerVersion.String()))

	return serverInfo
}
