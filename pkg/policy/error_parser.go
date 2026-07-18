package policy

import (
	"encoding/json"
	"fmt"
	"mime"
	"regexp"
	"strings"

	"github.com/yalp/jsonpath"
	"gopkg.in/yaml.v3"
)

// ErrorParserPolicy extracts error codes/messages from status, headers, or body.
type ErrorParserPolicy struct {
	Parser       string              `yaml:"parser" json:"parser"`
	Expression   string              `yaml:"expression" json:"expression"`
	Statuses     []string            `yaml:"statuses" json:"statuses"`
	ContentTypes *CaseInsensitiveSet `yaml:"content_types" json:"content_types"`
}

// UnmarshalJSON accepts camelCase contentTypes.
func (p *ErrorParserPolicy) UnmarshalJSON(data []byte) error {
	type Alias ErrorParserPolicy
	aux := &struct {
		*Alias
		ContentTypesCamel *CaseInsensitiveSet `json:"contentTypes"`
	}{
		Alias: (*Alias)(p),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if aux.ContentTypesCamel != nil {
		p.ContentTypes = aux.ContentTypesCamel
	}
	return nil
}

// Match reports whether status and content type hit this parser rule.
func (p *ErrorParserPolicy) Match(status string, contentType string, okStatus string) bool {
	statusMatch := false
	if status != "" && status == okStatus {
		statusMatch = true
	} else if len(p.Statuses) == 0 {
		statusMatch = true
	} else if status != "" {
		for _, s := range p.Statuses {
			if s == status {
				statusMatch = true
				break
			}
		}
	}

	contentTypeMatch := false
	if p.ContentTypes.IsEmpty() {
		contentTypeMatch = true
	} else if contentType != "" {
		// Strip charset/boundary via mime.ParseMediaType
		mediaType, _, err := mime.ParseMediaType(contentType)
		if err == nil && mediaType != "" {
			if p.ContentTypes.Contains(mediaType) {
				contentTypeMatch = true
			}
		} else {
			// Non-standard Content-Type: fall back to raw string match
			if p.ContentTypes.Contains(contentType) {
				contentTypeMatch = true
			}
		}
	}

	return statusMatch && contentTypeMatch
}

// ParseValue extracts a string from body via JsonPath or Regexp.
func (p *ErrorParserPolicy) ParseValue(body []byte) (string, error) {
	if p.Parser == "" || p.Expression == "" || len(body) == 0 {
		return "", nil
	}

	switch strings.ToLower(p.Parser) {
	case "jsonpath", "json_path":
		var data interface{}
		if err := json.Unmarshal(body, &data); err != nil {
			return "", err
		}
		val, err := jsonpath.Read(data, p.Expression)
		if err != nil {
			return "", err
		}
		if val == nil {
			return "", nil
		}
		switch v := val.(type) {
		case string:
			return v, nil
		default:
			return fmt.Sprintf("%v", v), nil
		}
	case "regexp", "regex":
		re, err := regexp.Compile(p.Expression)
		if err != nil {
			return "", err
		}
		matches := re.FindStringSubmatch(string(body))
		if len(matches) > 1 {
			return matches[1], nil // first capture group
		} else if len(matches) > 0 {
			return matches[0], nil // full match
		}
		return "", nil
	default:
		return "", fmt.Errorf("unsupported parser: %s", p.Parser)
	}
}

// CaseInsensitiveSet stores media types case-insensitively.
type CaseInsensitiveSet struct {
	data map[string]struct{}
}

// UnmarshalJSON accepts a JSON array or single string; stores lowercased keys.
func (s *CaseInsensitiveSet) UnmarshalJSON(data []byte) error {
	var list []string
	if err := json.Unmarshal(data, &list); err != nil {
		var single string
		if err := json.Unmarshal(data, &single); err != nil {
			return err
		}
		list = []string{single}
	}
	s.data = make(map[string]struct{})
	for _, val := range list {
		if val != "" {
			s.data[strings.ToLower(val)] = struct{}{}
		}
	}
	return nil
}

// MarshalJSON serializes as a []string for protocol/DB compatibility.
func (s *CaseInsensitiveSet) MarshalJSON() ([]byte, error) {
	if s == nil || s.data == nil {
		return json.Marshal([]string{})
	}
	list := make([]string, 0, len(s.data))
	for val := range s.data {
		list = append(list, val)
	}
	return json.Marshal(list)
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (s *CaseInsensitiveSet) UnmarshalYAML(value *yaml.Node) error {
	var list []string
	if err := value.Decode(&list); err != nil {
		var single string
		if err := value.Decode(&single); err != nil {
			return err
		}
		list = []string{single}
	}
	s.data = make(map[string]struct{})
	for _, val := range list {
		if val != "" {
			s.data[strings.ToLower(val)] = struct{}{}
		}
	}
	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (s *CaseInsensitiveSet) MarshalYAML() (interface{}, error) {
	if s == nil || s.data == nil {
		return []string{}, nil
	}
	list := make([]string, 0, len(s.data))
	for val := range s.data {
		list = append(list, val)
	}
	return list, nil
}

// Contains reports membership case-insensitively.
func (s *CaseInsensitiveSet) Contains(val string) bool {
	if s == nil || s.data == nil {
		return false
	}
	_, ok := s.data[strings.ToLower(val)]
	return ok
}

// IsEmpty reports whether the set is nil or empty.
func (s *CaseInsensitiveSet) IsEmpty() bool {
	return s == nil || len(s.data) == 0
}
