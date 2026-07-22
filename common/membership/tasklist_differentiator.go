package membership

import (
	"regexp"
	"strings"

	"github.com/dgryski/go-farm"
)

const uuidRegex = `[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`

// bitsMarker is the marker the cadencefx BITS interceptor injects into dynamic/short-lived task list names
const bitsMarker = "/cdnc-bits/"

var uuidRegexp = regexp.MustCompile(uuidRegex)

func TaskListExcludedFromShardDistributor(taskListName string, percentageOnboarded uint64, excludeShortLivedTaskLists bool) bool {
	// A task list is considered short-lived (and therefore not managed by the
	// shard distributor) if its name either contains a UUID or carries the BITS
	// marker. The marker check catches BITS task lists whose UUID was truncated
	// by the client-side length cap.
	excludeShortLivedTaskLists = excludeShortLivedTaskLists && isShortLivedTaskList(taskListName)
	return excludeShortLivedTaskLists || !belowPercentage(taskListName, percentageOnboarded)
}

func isShortLivedTaskList(taskListName string) bool {
	return uuidRegexp.MatchString(taskListName) || strings.Contains(taskListName, bitsMarker)
}

func belowPercentage(taskListName string, percentageOnboarded uint64) bool {
	hash := farm.Fingerprint64([]byte(taskListName))
	return isbelowPercentage(hash, percentageOnboarded)
}

func isbelowPercentage(hash uint64, percentageOnboarded uint64) bool {
	return hash%100 < percentageOnboarded
}
