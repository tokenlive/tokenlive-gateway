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

// ErrorParserPolicy 错误解析策略，用于从 HTTP 状态码、Header、响应体中解析出具体的错误码或错误消息
type ErrorParserPolicy struct {
	Parser       string              `yaml:"parser" json:"parser"`
	Expression   string              `yaml:"expression" json:"expression"`
	Statuses     []string            `yaml:"statuses" json:"statuses"`
	ContentTypes *CaseInsensitiveSet `yaml:"content_types" json:"content_types"`
}

// UnmarshalJSON 自定义反序列化，兼容小驼峰 contentTypes 的字段映射
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

// Match 检查给定的状态码、内容类型是否命中此解析规则
func (p *ErrorParserPolicy) Match(status string, contentType string, okStatus string) bool {
	// 判断 status 是否与 okStatus 匹配
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

	// 判断 contentType 是否与配置的内容类型匹配
	contentTypeMatch := false
	if p.ContentTypes.IsEmpty() {
		contentTypeMatch = true
	} else if contentType != "" {
		// 使用 mime.ParseMediaType 解析 Content-Type，剥离掉可能存在的 charset 或 boundary 等额外参数
		mediaType, _, err := mime.ParseMediaType(contentType)
		if err == nil && mediaType != "" {
			if p.ContentTypes.Contains(mediaType) {
				contentTypeMatch = true
			}
		} else {
			// 解析失败（非标 Content-Type 头），Fallback 回退做纯字符串匹配
			if p.ContentTypes.Contains(contentType) {
				contentTypeMatch = true
			}
		}
	}

	return statusMatch && contentTypeMatch
}

// ParseValue 从给定的响应体中，根据表达式解析出目标字符串（支持 JsonPath 和 Regexp）
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
		// 转为 string
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
			return matches[1], nil // 返回第一个捕获组
		} else if len(matches) > 0 {
			return matches[0], nil // 返回整行匹配
		}
		return "", nil
	default:
		return "", fmt.Errorf("unsupported parser: %s", p.Parser)
	}
}

// CaseInsensitiveSet 是一个大小写不敏感的集合，用于存储 Content-Type 等媒体类型
type CaseInsensitiveSet struct {
	data map[string]struct{}
}

// UnmarshalJSON 实现了 json.Unmarshaler 接口，将 JSON 数组或单字符串反序列化为底层小写存储的 map 集合
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

// MarshalJSON 实现了 json.Marshaler 接口，序列化回 []string 数组以保持与协议/数据库的兼容性
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

// UnmarshalYAML 实现了 yaml.Unmarshaler 接口，以便支持 YAML 文件解析
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

// MarshalYAML 实现了 yaml.Marshaler 接口，以便支持序列化回 YAML 数组结构
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

// Contains 判定给定的值是否在集合中（大小写不敏感）
func (s *CaseInsensitiveSet) Contains(val string) bool {
	if s == nil || s.data == nil {
		return false
	}
	_, ok := s.data[strings.ToLower(val)]
	return ok
}

// IsEmpty 判定集合是否为空
func (s *CaseInsensitiveSet) IsEmpty() bool {
	return s == nil || len(s.data) == 0
}
