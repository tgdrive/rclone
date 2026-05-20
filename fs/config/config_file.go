package config

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/unknwon/goconfig"
)

// LoadFile reads an rclone config file and returns all entries as
// map[section]map[key]value. The file may be encrypted or plain —
// Decrypt is applied automatically.
func LoadFile(filePath string) (map[string]map[string]string, error) {
	fd, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer fd.Close()

	cryptReader, err := Decrypt(fd)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt config file: %w", err)
	}

	return ParseConfigINI(cryptReader)
}

// ParseConfigINI reads and parses an rclone config in INI format.
// The input may be plain text or output from Decrypt.
func ParseConfigINI(r io.Reader) (map[string]map[string]string, error) {
	gc, err := goconfig.LoadFromReader(r)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	result := map[string]map[string]string{}
	for _, section := range gc.GetSectionList() {
		if section == "DEFAULT" {
			continue
		}
		keys := gc.GetKeyList(section)
		sectionMap := make(map[string]string, len(keys))
		for _, key := range keys {
			sectionMap[key], _ = gc.GetValue(section, key)
		}
		result[section] = sectionMap
	}
	return result, nil
}

// SerializeConfigINI serializes a config map to INI format.
func SerializeConfigINI(sections map[string]map[string]string) string {
	var buf strings.Builder
	for section, keys := range sections {
		fmt.Fprintf(&buf, "[%s]\n", section)
		for key, value := range keys {
			fmt.Fprintf(&buf, "%s = %s\n", key, value)
		}
		buf.WriteString("\n")
	}
	return strings.TrimRight(buf.String(), "\n")
}

// DecryptConfigData decrypts config data (which may be plain or encrypted)
// and returns the decrypted content. The data parameter is the full
// content of a config file or stored config blob.
func DecryptConfigData(data string) (io.Reader, error) {
	return Decrypt(bytes.NewReader([]byte(data)))
}
