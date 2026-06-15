# Endpoint Custom Headers Implementation Plan

This plan details the steps required to implement custom headers for endpoints in both `tokenlive-gateway-admin` and `tokenlive-gateway`.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow users to define custom HTTP headers for each endpoint in the admin console, sync them to Redis, and apply/override them during outgoing upstream LLM provider calls.

**Architecture:** Extend endpoint database table and GORM models. Extract and serialize custom headers to Redis `aigw:config:endpoints:<model>` format. Extend the service discovery models in the gateway and iterate through custom headers to append them into the final `http.Request` sent by the providers.

**Tech Stack:** Go (Gin, GORM, Go-Redis), Vue 3 (Vite, Ant Design Vue), MySQL

---

## User Review Required

> [!IMPORTANT]
> The database migration requires updating the `endpoint` table. Be sure to run database migrations or apply the SQL change in the developer database environment before starting the gateway or admin backend.

> [!WARNING]
> Since users opted not to automate git commits, developers must manually verify and commit files at checkpoints.

## Open Questions

All key design decisions (Database schema modeling, headers overriding logic, UI dynamic inputs, and static config support) have been aligned and finalized with the user. No open questions remain.

---

## Proposed Changes

### [Component 1: Database Migration]

#### [MODIFY] [init.sql](file:///Users/chenzhiguo/Projects/tokenlive-gateway-admin/scripts/init.sql)

Add `headers` JSON field to `endpoint` table schema.

- [ ] **Step 1: Modify endpoint table SQL definition**
  Add the column definition:

  ```sql
  headers     JSON                                                           DEFAULT NULL COMMENT '自定义请求头，如 {"X-Custom-Header": "value"}',
  ```

  in `scripts/init.sql` (after `enabled` column).

---

### [Component 2: Admin Console Backend]

#### [MODIFY] [endpoint.go](file:///Users/chenzhiguo/Projects/tokenlive-gateway-admin/internal/mods/resource/schema/endpoint.go)

Add `Headers` mapping fields into database struct and form struct.

- [ ] **Step 1: Update schema.Endpoint struct**
  Add:

  ```go
  Headers     json.RawMessage `json:"headers,omitempty" gorm:"type:json;"` // Custom HTTP headers
  ```

- [ ] **Step 2: Update schema.EndpointForm struct**
  Add:

  ```go
  Headers     json.RawMessage `json:"headers"`                             // Custom HTTP headers
  ```

- [ ] **Step 3: Update schema.EndpointForm.FillTo mapping method**
  Add:

  ```go
  endpoint.Headers = e.Headers
  ```

#### [MODIFY] [redis_sync.go](file:///Users/chenzhiguo/Projects/tokenlive-gateway-admin/internal/mods/resource/biz/redis_sync.go)

Extract and serialize custom headers to Redis config.

- [ ] **Step 1: Update ResolvedEndpoint struct**
  Add:

  ```go
  Headers          map[string]string `json:"headers,omitempty"`
  ```

- [ ] **Step 2: Unmarshal and assign Headers in SyncModelByCode**
  Update serialization mapping loop:

  ```go
  var headersMap map[string]string
  if len(ep.Headers) > 0 {
      _ = json.Unmarshal(ep.Headers, &headersMap)
  }
  ```

  And map to `ResolvedEndpoint`:

  ```go
  resolvedList = append(resolvedList, ResolvedEndpoint{
      ...
      Headers:          headersMap,
  })
  ```

---

### [Component 3: Admin Console Frontend]

#### [MODIFY] [EndpointEditDialog.vue](file:///Users/chenzhiguo/Projects/tokenlive-gateway-admin/frontend/src/views/resource/EndpointEditDialog.vue)

Build custom headers KV list interface.

- [ ] **Step 1: Add frontend component reactive state and helper functions**
  Add `headersList` ref and mapping functions `addHeader`, `removeHeader`, `headersToJSON`, `jsonToHeaders` (similar to metadata helper methods):

  ```javascript
  const headersList = ref([])

  function addHeader() {
      headersList.value.push({ key: '', value: '' })
  }

  function removeHeader(index) {
      headersList.value.splice(index, 1)
  }

  function headersToJSON(list) {
      const obj = {}
      list.forEach((item) => {
          if (item.key.trim()) {
              obj[item.key.trim()] = item.value
          }
      })
      return Object.keys(obj).length > 0 ? obj : null
  }

  function jsonToHeaders(json) {
      if (!json) return []
      const obj = typeof json === 'string' ? JSON.parse(json) : json
      return Object.entries(obj).map(([key, value]) => ({ key, value: String(value) }))
  }
  ```

- [ ] **Step 2: Initialize headers list on Dialog creation/edit**
  In `handleCreate` initialize `headersList.value = []`.
  In `handleEdit` load:

  ```javascript
  headersList.value = jsonToHeaders(record.headers)
  ```

- [ ] **Step 3: Append headers parameters on form submission**
  In `handleOk` retrieve headers JSON and pass it inside `params`:

  ```javascript
  const params = {
      ...values,
      headers: headersToJSON(headersList.value),
  }
  ```

- [ ] **Step 4: Add form elements to the Dialog template**
  Insert the form item just below `metadata` form item:

  ```html
  <a-form-item
      :label="$t('pages.endpoint.form.headers')"
      name="headers">
      <div class="metadata-list">
          <div
              v-for="(item, index) in headersList"
              :key="index"
              class="metadata-row">
              <a-input
                  v-model:value="item.key"
                  :placeholder="$t('pages.endpoint.form.headers.key')"
                  style="width: 35%" />
              <a-input
                  v-model:value="item.value"
                  :placeholder="$t('pages.endpoint.form.headers.value')"
                  style="width: 50%" />
              <delete-outlined
                  class="metadata-delete"
                  @click="removeHeader(index)" />
          </div>
          <a-button
              type="dashed"
              block
              @click="addHeader">
              <plus-outlined />
              {{ $t('pages.endpoint.form.headers.add') }}
          </a-button>
      </div>
  </a-form-item>
  ```

#### [MODIFY] [pages.js (zh-CN)](file:///Users/chenzhiguo/Projects/tokenlive-gateway-admin/frontend/src/locales/lang/zh-CN/pages.js)

- [ ] **Step 1: Add zh-CN translations**
  Append tokens inside pages export object:

  ```javascript
  'pages.endpoint.form.headers': '自定义请求头',
  'pages.endpoint.form.headers.key': '请求头名称',
  'pages.endpoint.form.headers.value': '值',
  'pages.endpoint.form.headers.add': '添加请求头',
  ```

#### [MODIFY] [pages.js (en-US)](file:///Users/chenzhiguo/Projects/tokenlive-gateway-admin/frontend/src/locales/lang/en-US/pages.js)

- [ ] **Step 1: Add en-US translations**
  Append tokens inside pages export object:

  ```javascript
  'pages.endpoint.form.headers': 'Custom Headers',
  'pages.endpoint.form.headers.key': 'Header Name',
  'pages.endpoint.form.headers.value': 'Value',
  'pages.endpoint.form.headers.add': 'Add Header',
  ```

---

### [Component 4: Gateway Core Models & Configs]

#### [MODIFY] [types.go (config)](file:///Users/chenzhiguo/Projects/tokenlive-gateway/pkg/config/types.go)

- [ ] **Step 1: Update EndpointConfig struct**
  Add headers tags:

  ```go
  Headers   map[string]string `mapstructure:"headers" yaml:"headers"`
  ```

- [ ] **Step 2: Update ResolvedEndpoint struct**
  Add json headers serialization mapping:

  ```go
  Headers   map[string]string `json:"headers,omitempty"`
  ```

#### [MODIFY] [types.go (core)](file:///Users/chenzhiguo/Projects/tokenlive-gateway/pkg/core/types.go)

- [ ] **Step 1: Update core.Endpoint struct**
  Add Headers field:

  ```go
  Headers   map[string]string
  ```

#### [MODIFY] [service_discovery.go](file:///Users/chenzhiguo/Projects/tokenlive-gateway/pkg/discovery/service_discovery.go)

- [ ] **Step 1: Update ServiceInstance struct**
  Add headers mapping tags:

  ```go
  Headers   map[string]string `json:"headers" yaml:"headers"`
  ```

- [ ] **Step 2: Update DynamicEndpoint struct**
  Add Headers field:

  ```go
  Headers   map[string]string
  ```

---

### [Component 5: Service Discovery Data Binding]

#### [MODIFY] [engine.go](file:///Users/chenzhiguo/Projects/tokenlive-gateway/cmd/server/wire/engine.go)

- [ ] **Step 1: Bind Headers inside registerEndpointsFromResolvedEndpoints**
  Map `re.Headers` onto `discovery.ServiceInstance` configuration:

  ```go
  Headers: re.Headers,
  ```

- [ ] **Step 2: Bind Headers inside dynamicEndpointAdapter.GetEndpoints**
  Map `ep.Headers` onto `discovery.DynamicEndpoint` configuration:

  ```go
  Headers: ep.Headers,
  ```

#### [MODIFY] [dynamic_discovery.go](file:///Users/chenzhiguo/Projects/tokenlive-gateway/pkg/discovery/dynamic_discovery.go)

- [ ] **Step 1: Bind Headers in ListInstances**
  Map `ep.Headers` onto `ServiceInstance`:

  ```go
  Headers: ep.Headers,
  ```

#### [MODIFY] [discovery.go](file:///Users/chenzhiguo/Projects/tokenlive-gateway/pkg/core/discovery.go)

- [ ] **Step 1: Propagate Headers inside serviceInstanceToEndpoint**
  Assign `inst.Headers` into Core `Endpoint` entity:

  ```go
  Headers: inst.Headers,
  ```

---

### [Component 6: Provider HTTP Request Handling]

#### [MODIFY] [openai.go](file:///Users/chenzhiguo/Projects/tokenlive-gateway/pkg/llm/providers/openai.go)

- [ ] **Step 1: Set custom headers inside doRequest**
  Right after setting standard credentials:

  ```go
  if gctx.SelectedEndpoint != nil && len(gctx.SelectedEndpoint.Headers) > 0 {
      for k, v := range gctx.SelectedEndpoint.Headers {
          req.Header.Set(k, v)
      }
  }
  ```

#### [MODIFY] [openai_model_list.go](file:///Users/chenzhiguo/Projects/tokenlive-gateway/pkg/llm/providers/openai_model_list.go)

- [ ] **Step 1: Set custom headers inside Invoke**
  Inject headers mapping loop:

  ```go
  if gctx.SelectedEndpoint != nil && len(gctx.SelectedEndpoint.Headers) > 0 {
      for k, v := range gctx.SelectedEndpoint.Headers {
          req.Header.Set(k, v)
      }
  }
  ```

#### [MODIFY] [anthropic_chat.go](file:///Users/chenzhiguo/Projects/tokenlive-gateway/pkg/llm/providers/anthropic_chat.go)

- [ ] **Step 1: Set custom headers inside Invoke**
  Inject headers mapping loop:

  ```go
  if gctx.SelectedEndpoint != nil && len(gctx.SelectedEndpoint.Headers) > 0 {
      for k, v := range gctx.SelectedEndpoint.Headers {
          req.Header.Set(k, v)
      }
  }
  ```

---

## Verification Plan

### Automated Tests

- Run `go test ./pkg/llm/providers/...` to verify provider tests continue passing.
- Write a unit test validating custom header inclusion on `pkg/llm/providers/openai_test.go` and `pkg/llm/providers/anthropic_test.go`.

### Manual Verification

- Compile and run both services: `make build` or standard runner.
- Edit an endpoint from UI, add custom headers.
- Inspect MySQL table and Redis keys.
- Check request forwarding.
