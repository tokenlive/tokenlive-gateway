// Package versionreport publishes lightweight Gateway build identities.
package versionreport

import (
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"
)

const reportTTL = 3 * time.Minute

var namespacePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// Node mirrors the Admin versionregistry v1 wire protocol without a module dependency.
type Node struct {
	SchemaVersion int    `json:"schema_version"`
	Namespace     string `json:"namespace"`
	InstanceID    string `json:"instance_id"`
	Version       string `json:"version"`
	BuildKind     string `json:"build_kind"`
}

// normalizeNode keeps direct Redis writes compatible with Admin's validation:
// namespace is explicit and case-sensitive; UUID text is hyphenated and keys
// use lowercase canonical UUIDs. Unknown version text remains displayable.
func normalizeNode(node Node) (Node, error) {
	if node.SchemaVersion != 1 {
		return Node{}, fmt.Errorf("versionreport: schema_version must be 1")
	}
	if !namespacePattern.MatchString(node.Namespace) {
		return Node{}, fmt.Errorf("versionreport: namespace must match [A-Za-z0-9_-]{1,64}")
	}
	if len(node.InstanceID) != 36 {
		return Node{}, fmt.Errorf("versionreport: instance_id must be a hyphenated UUID")
	}
	id, err := uuid.Parse(node.InstanceID)
	if err != nil {
		return Node{}, fmt.Errorf("versionreport: instance_id must be a valid UUID")
	}
	node.InstanceID = id.String()
	if len(node.Version) > 128 {
		return Node{}, fmt.Errorf("versionreport: version exceeds 128 bytes")
	}
	if node.BuildKind != "release" && node.BuildKind != "dev" {
		return Node{}, fmt.Errorf("versionreport: build_kind must be release or dev")
	}
	return node, nil
}
