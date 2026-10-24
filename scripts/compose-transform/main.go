// Command compose-transform reads a docker-compose YAML from stdin,
// replaces build: blocks with image: references for GHCR, and writes
// the result to stdout. Used by CI to sync sukko → sukko-cli.
//
// The transform preserves YAML comments, ordering, and formatting
// via yaml.Node tree manipulation.
//
// Usage: go run . < docker-compose.yml > docker-compose-transformed.yml
//
//nolint:forbidigo // standalone CLI tool — fmt output is intentional
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// serviceImageMap maps docker-compose service names to GHCR image references.
// Hardcoded intentionally — new services are rare and changes should be explicit.
var serviceImageMap = map[string]string{
	"provisioning":   "ghcr.io/sukko-dev/sukko-provisioning:latest",
	"ws-server":      "ghcr.io/sukko-dev/sukko-server:latest",
	"ws-gateway":     "ghcr.io/sukko-dev/sukko-gateway:latest",
	"webhook-worker": "ghcr.io/sukko-dev/sukko-webhook-worker:latest",
	"sukko-tester":   "ghcr.io/sukko-dev/sukko-tester:latest",
	"postgres":       "postgres:16-alpine",
	"push-service":   "ghcr.io/sukko-dev/sukko-push:latest",
}

func main() {
	if err := run(os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

// run is the testable core logic.
func run(in io.Reader, out io.Writer, errOut io.Writer) error {
	data, err := io.ReadAll(in)
	if err != nil {
		return fmt.Errorf("read stdin: %w", err)
	}
	if len(data) == 0 {
		return errors.New("empty input")
	}

	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("parse YAML: %w", err)
	}

	if doc.Kind != yaml.DocumentNode || len(doc.Content) == 0 {
		return fmt.Errorf("expected YAML document, got kind %d", doc.Kind)
	}

	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		return fmt.Errorf("expected root mapping, got kind %d", root.Kind)
	}

	if err := transformServices(root, errOut); err != nil {
		return fmt.Errorf("transform services: %w", err)
	}

	enc := yaml.NewEncoder(out)
	enc.SetIndent(2)
	if err := enc.Encode(&doc); err != nil {
		return fmt.Errorf("encode YAML: %w", err)
	}
	return enc.Close()
}

// transformServices finds the "services" key in the root mapping and transforms each service.
func transformServices(root *yaml.Node, errOut io.Writer) error {
	servicesNode := findMapValue(root, "services")
	if servicesNode == nil {
		return fmt.Errorf("no 'services' key found in compose file")
	}
	if servicesNode.Kind != yaml.MappingNode {
		return fmt.Errorf("'services' must be a mapping, got kind %d", servicesNode.Kind)
	}

	// Iterate service entries (key-value pairs)
	for i := 0; i < len(servicesNode.Content)-1; i += 2 {
		serviceNameNode := servicesNode.Content[i]
		serviceDefNode := servicesNode.Content[i+1]

		if serviceDefNode.Kind != yaml.MappingNode {
			continue
		}

		serviceName := serviceNameNode.Value
		transformService(serviceName, serviceDefNode, errOut)
	}

	return nil
}

// transformService replaces build: with image: for a single service.
func transformService(name string, def *yaml.Node, errOut io.Writer) {
	// Strip dev-only host-path bind-mounts regardless of build: — the published/CLI
	// compose runs from the operator's .sukko dir, where repo-relative paths don't exist.
	stripHostPathBindMounts(def)

	buildIdx := findMapKeyIndex(def, "build")
	if buildIdx < 0 {
		return // No build key — pass through unchanged
	}

	imageRef, known := serviceImageMap[name]
	if !known {
		fmt.Fprintf(errOut, "warning: unknown service %q has build: block — removing it (no image mapping)\n", name)
	}

	// Remove the build key-value pair (key at buildIdx, value at buildIdx+1)
	def.Content = append(def.Content[:buildIdx], def.Content[buildIdx+2:]...)

	// Strip platform: — it's a dev-only build architecture constraint. Published images
	// should use host-native platform resolution via the image manifest.
	if platformIdx := findMapKeyIndex(def, "platform"); platformIdx >= 0 {
		def.Content = append(def.Content[:platformIdx], def.Content[platformIdx+2:]...)
	}

	if !known {
		return // Unknown service — just remove build, don't add image
	}

	// Check if image: already exists (idempotency)
	if existingIdx := findMapKeyIndex(def, "image"); existingIdx >= 0 {
		// Update existing image value
		def.Content[existingIdx+1].Value = imageRef
		return
	}

	// Insert image: key-value at position 0 (top of service definition)
	imageKeyNode := &yaml.Node{
		Kind:  yaml.ScalarNode,
		Tag:   "!!str",
		Value: "image",
	}
	imageValueNode := &yaml.Node{
		Kind:  yaml.ScalarNode,
		Tag:   "!!str",
		Value: imageRef,
	}

	// Prepend image key-value pair
	newContent := make([]*yaml.Node, 0, len(def.Content)+2)
	newContent = append(newContent, imageKeyNode, imageValueNode)
	newContent = append(newContent, def.Content...)
	def.Content = newContent
}

// stripHostPathBindMounts removes dev-only host-path bind-mount volumes (relative
// sources starting with ./ or ../) from a service, in both short syntax
// ("./host:/container[:mode]") and long syntax ({type: bind, source: ./host, …}). The
// published/CLI compose is run from the operator's .sukko project dir, where repo-relative
// paths don't exist — such a bind-mount there (with the default create_host_path) would
// silently create empty directories and mount nothing. Named volumes and absolute paths
// are left untouched.
func stripHostPathBindMounts(def *yaml.Node) {
	volIdx := findMapKeyIndex(def, "volumes")
	if volIdx < 0 {
		return
	}
	seq := def.Content[volIdx+1]
	if seq.Kind != yaml.SequenceNode {
		return
	}
	kept := seq.Content[:0]
	for _, item := range seq.Content {
		if isRelativeHostPathBindMount(item) {
			continue // drop dev-only host-path bind-mount
		}
		kept = append(kept, item)
	}
	seq.Content = kept
	// Drop the now-empty volumes key entirely.
	if len(seq.Content) == 0 {
		def.Content = append(def.Content[:volIdx], def.Content[volIdx+2:]...)
	}
}

// isRelativeHostPathBindMount reports whether a volume list entry is a bind-mount whose
// source is a relative path (./ or ../), in either short or long compose syntax.
func isRelativeHostPathBindMount(item *yaml.Node) bool {
	var src string
	switch item.Kind {
	case yaml.ScalarNode:
		src = item.Value // "source:target[:mode]"
	case yaml.MappingNode:
		if s := findMapValue(item, "source"); s != nil {
			src = s.Value
		}
	}
	return strings.HasPrefix(src, "./") || strings.HasPrefix(src, "../")
}

// findMapValue finds a key in a mapping node and returns its value node.
func findMapValue(mapping *yaml.Node, key string) *yaml.Node {
	idx := findMapKeyIndex(mapping, key)
	if idx < 0 || idx+1 >= len(mapping.Content) {
		return nil
	}
	return mapping.Content[idx+1]
}

// findMapKeyIndex finds the index of a key in a mapping node's Content slice.
// Returns -1 if not found.
func findMapKeyIndex(mapping *yaml.Node, key string) int {
	for i := 0; i < len(mapping.Content)-1; i += 2 {
		if mapping.Content[i].Value == key {
			return i
		}
	}
	return -1
}
