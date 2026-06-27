#include "checkpoint/s3/S3CheckpointStore.hpp"

#include "checkpoint/CheckpointCodec.hpp"

#include <curl/curl.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace cex::checkpoint {
namespace {

enum class HttpMethod {
  Get,
  Put,
};

struct HttpResponse {
  long status{0};
  std::string body;
};

struct UploadState {
  std::string_view body;
  std::size_t offset{0};
};

class CurlHandle {
 public:
  CurlHandle() : handle_(curl_easy_init()) {
    if (handle_ == nullptr) {
      throw std::runtime_error("failed to initialize curl handle");
    }
  }

  CurlHandle(const CurlHandle&) = delete;
  CurlHandle& operator=(const CurlHandle&) = delete;

  ~CurlHandle() {
    curl_easy_cleanup(handle_);
  }

  [[nodiscard]] CURL* get() const noexcept {
    return handle_;
  }

 private:
  CURL* handle_{nullptr};
};

class CurlHeaders {
 public:
  CurlHeaders() = default;

  CurlHeaders(const CurlHeaders&) = delete;
  CurlHeaders& operator=(const CurlHeaders&) = delete;

  ~CurlHeaders() {
    curl_slist_free_all(head_);
  }

  void append(const std::string& header) {
    curl_slist* next = curl_slist_append(head_, header.c_str());
    if (next == nullptr) {
      throw std::runtime_error("failed to allocate curl header");
    }
    head_ = next;
  }

  [[nodiscard]] curl_slist* get() const noexcept {
    return head_;
  }

 private:
  curl_slist* head_{nullptr};
};

void require_curl(CURLcode code, std::string_view operation) {
  if (code != CURLE_OK) {
    throw std::runtime_error("curl " + std::string(operation) +
                             " failed: " + curl_easy_strerror(code));
  }
}

void ensure_curl_global_initialized() {
  static std::once_flag once;
  static CURLcode init_result = CURLE_OK;
  std::call_once(once, [] {
    init_result = curl_global_init(CURL_GLOBAL_DEFAULT);
  });
  require_curl(init_result, "global init");
}

[[nodiscard]] bool blank(std::string_view value) {
  for (const unsigned char ch : value) {
    if (std::isspace(ch) == 0) {
      return false;
    }
  }
  return true;
}

void require_non_blank(std::string_view value, std::string_view name) {
  if (blank(value)) {
    throw std::invalid_argument(std::string(name) + " must not be empty");
  }
}

[[nodiscard]] bool starts_with(std::string_view value,
                               std::string_view prefix) {
  return value.size() >= prefix.size() &&
         value.substr(0, prefix.size()) == prefix;
}

[[nodiscard]] std::string normalize_endpoint(std::string endpoint) {
  require_non_blank(endpoint, "S3 checkpoint endpoint");
  if (!starts_with(endpoint, "http://") &&
      !starts_with(endpoint, "https://")) {
    throw std::invalid_argument(
        "S3 checkpoint endpoint must start with http:// or https://");
  }
  while (!endpoint.empty() && endpoint.back() == '/') {
    endpoint.pop_back();
  }
  return endpoint;
}

[[nodiscard]] std::string normalize_bucket(std::string bucket) {
  require_non_blank(bucket, "S3 checkpoint bucket");
  if (bucket.find('/') != std::string::npos) {
    throw std::invalid_argument("S3 checkpoint bucket must not contain '/'");
  }
  return bucket;
}

[[nodiscard]] std::string normalize_prefix(std::string prefix) {
  while (!prefix.empty() && prefix.front() == '/') {
    prefix.erase(prefix.begin());
  }
  if (!prefix.empty() && prefix.back() != '/') {
    prefix.push_back('/');
  }
  return prefix;
}

[[nodiscard]] char hex_digit(unsigned int value) {
  return static_cast<char>(value < 10 ? ('0' + value) : ('A' + value - 10));
}

[[nodiscard]] bool is_unreserved(unsigned char value) {
  return (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') ||
         (value >= '0' && value <= '9') || value == '-' || value == '_' ||
         value == '.' || value == '~';
}

[[nodiscard]] std::string percent_encode(std::string_view value,
                                          bool preserve_slash) {
  std::string encoded;
  encoded.reserve(value.size());

  for (const unsigned char ch : value) {
    if (is_unreserved(ch) || (preserve_slash && ch == '/')) {
      encoded.push_back(static_cast<char>(ch));
      continue;
    }
    encoded.push_back('%');
    encoded.push_back(hex_digit(ch >> 4U));
    encoded.push_back(hex_digit(ch & 0x0FU));
  }

  return encoded;
}

void append_utf8(std::string& output, unsigned int codepoint) {
  if (codepoint <= 0x7FU) {
    output.push_back(static_cast<char>(codepoint));
  } else if (codepoint <= 0x7FFU) {
    output.push_back(static_cast<char>(0xC0U | (codepoint >> 6U)));
    output.push_back(static_cast<char>(0x80U | (codepoint & 0x3FU)));
  } else if (codepoint <= 0xFFFFU) {
    output.push_back(static_cast<char>(0xE0U | (codepoint >> 12U)));
    output.push_back(static_cast<char>(0x80U | ((codepoint >> 6U) & 0x3FU)));
    output.push_back(static_cast<char>(0x80U | (codepoint & 0x3FU)));
  } else if (codepoint <= 0x10FFFFU) {
    output.push_back(static_cast<char>(0xF0U | (codepoint >> 18U)));
    output.push_back(static_cast<char>(0x80U | ((codepoint >> 12U) & 0x3FU)));
    output.push_back(static_cast<char>(0x80U | ((codepoint >> 6U) & 0x3FU)));
    output.push_back(static_cast<char>(0x80U | (codepoint & 0x3FU)));
  }
}

[[nodiscard]] std::optional<unsigned int> parse_numeric_entity(
    std::string_view entity) {
  if (entity.size() < 2 || entity.front() != '#') {
    return std::nullopt;
  }

  unsigned int parsed{0};
  const char* begin = entity.data() + 1;
  int base = 10;
  if (entity.size() > 2 && (entity[1] == 'x' || entity[1] == 'X')) {
    begin = entity.data() + 2;
    base = 16;
  }
  const char* end = entity.data() + entity.size();
  const auto [ptr, error] = std::from_chars(begin, end, parsed, base);
  if (error != std::errc{} || ptr != end) {
    return std::nullopt;
  }
  return parsed;
}

[[nodiscard]] std::string xml_unescape(std::string_view value) {
  std::string output;
  output.reserve(value.size());

  for (std::size_t index = 0; index < value.size(); ++index) {
    if (value[index] != '&') {
      output.push_back(value[index]);
      continue;
    }

    const std::size_t semicolon = value.find(';', index + 1);
    if (semicolon == std::string_view::npos) {
      output.push_back(value[index]);
      continue;
    }

    const std::string_view entity =
        value.substr(index + 1, semicolon - index - 1);
    if (entity == "amp") {
      output.push_back('&');
    } else if (entity == "lt") {
      output.push_back('<');
    } else if (entity == "gt") {
      output.push_back('>');
    } else if (entity == "quot") {
      output.push_back('"');
    } else if (entity == "apos") {
      output.push_back('\'');
    } else if (const auto codepoint = parse_numeric_entity(entity);
               codepoint.has_value()) {
      append_utf8(output, *codepoint);
    } else {
      output.push_back('&');
      output.append(entity);
      output.push_back(';');
    }
    index = semicolon;
  }

  return output;
}

[[nodiscard]] std::vector<std::string> xml_tag_values(std::string_view xml,
                                                       std::string_view tag) {
  std::vector<std::string> values;
  const std::string open = "<" + std::string(tag) + ">";
  const std::string close = "</" + std::string(tag) + ">";

  std::size_t search = 0;
  while (search < xml.size()) {
    const std::size_t begin = xml.find(open, search);
    if (begin == std::string_view::npos) {
      break;
    }
    const std::size_t value_begin = begin + open.size();
    const std::size_t end = xml.find(close, value_begin);
    if (end == std::string_view::npos) {
      break;
    }
    values.push_back(xml_unescape(xml.substr(value_begin, end - value_begin)));
    search = end + close.size();
  }

  return values;
}

[[nodiscard]] std::optional<std::string> first_xml_tag_value(
    std::string_view xml,
    std::string_view tag) {
  auto values = xml_tag_values(xml, tag);
  if (values.empty()) {
    return std::nullopt;
  }
  return std::move(values.front());
}

[[nodiscard]] bool ends_with(std::string_view value, std::string_view suffix) {
  return value.size() >= suffix.size() &&
         value.substr(value.size() - suffix.size()) == suffix;
}

[[nodiscard]] bool starts_with_prefix(std::string_view value,
                                      std::string_view prefix) {
  return value.size() >= prefix.size() &&
         value.substr(0, prefix.size()) == prefix;
}

[[nodiscard]] std::string response_excerpt(std::string_view body) {
  constexpr std::size_t kMaxExcerpt = 256;
  if (body.size() <= kMaxExcerpt) {
    return std::string(body);
  }
  std::string excerpt(body.substr(0, kMaxExcerpt));
  excerpt += "...";
  return excerpt;
}

[[nodiscard]] std::runtime_error http_error(std::string_view operation,
                                            long status,
                                            std::string_view body) {
  std::string message = "S3 checkpoint " + std::string(operation) +
                        " failed with HTTP " + std::to_string(status);
  if (!body.empty()) {
    message += ": ";
    message += response_excerpt(body);
  }
  return std::runtime_error(std::move(message));
}

[[nodiscard]] std::size_t write_body_callback(char* ptr,
                                               std::size_t size,
                                               std::size_t nmemb,
                                               void* userdata) {
  const std::size_t byte_count = size * nmemb;
  auto* output = static_cast<std::string*>(userdata);
  try {
    output->append(ptr, byte_count);
  } catch (const std::exception&) {
    return 0;
  }
  return byte_count;
}

[[nodiscard]] std::size_t read_body_callback(char* ptr,
                                              std::size_t size,
                                              std::size_t nmemb,
                                              void* userdata) {
  const std::size_t capacity = size * nmemb;
  auto* state = static_cast<UploadState*>(userdata);
  const std::size_t remaining = state->body.size() - state->offset;
  const std::size_t byte_count = std::min(capacity, remaining);
  if (byte_count > 0) {
    std::memcpy(ptr, state->body.data() + state->offset, byte_count);
    state->offset += byte_count;
  }
  return byte_count;
}

[[nodiscard]] HttpResponse perform_request(
    const S3CheckpointStoreConfig& config,
    std::string_view sigv4_scope,
    std::string_view url,
    HttpMethod method,
    std::string_view request_body = {}) {
  ensure_curl_global_initialized();

  CurlHandle curl;
  CurlHeaders headers;
  headers.append("x-amz-content-sha256: UNSIGNED-PAYLOAD");
  if (method == HttpMethod::Put) {
    headers.append("Content-Type: text/plain");
    headers.append("Expect:");
  } else {
    headers.append("Accept: application/xml");
  }

  std::array<char, CURL_ERROR_SIZE> error_buffer{};
  HttpResponse response;
  const std::string url_text(url);
  const std::string sigv4_scope_text(sigv4_scope);
  const std::string userpwd = config.access_key + ":" + config.secret_key;
  UploadState upload{.body = request_body, .offset = 0};

  require_curl(curl_easy_setopt(curl.get(), CURLOPT_URL, url_text.c_str()),
               "set URL");
  require_curl(curl_easy_setopt(curl.get(),
                                CURLOPT_AWS_SIGV4,
                                sigv4_scope_text.c_str()),
               "set AWS SigV4");
  require_curl(curl_easy_setopt(curl.get(), CURLOPT_USERPWD, userpwd.c_str()),
               "set credentials");
  require_curl(curl_easy_setopt(curl.get(), CURLOPT_HTTPHEADER, headers.get()),
               "set headers");
  require_curl(curl_easy_setopt(
                   curl.get(), CURLOPT_ERRORBUFFER, error_buffer.data()),
               "set error buffer");
  require_curl(curl_easy_setopt(curl.get(), CURLOPT_NOSIGNAL, 1L),
               "set no-signal");
  require_curl(curl_easy_setopt(curl.get(),
                                CURLOPT_CONNECTTIMEOUT_MS,
                                config.connect_timeout_ms),
               "set connect timeout");
  require_curl(curl_easy_setopt(
                   curl.get(), CURLOPT_TIMEOUT_MS, config.request_timeout_ms),
               "set request timeout");
  require_curl(curl_easy_setopt(
                   curl.get(), CURLOPT_WRITEFUNCTION, write_body_callback),
               "set write callback");
  require_curl(curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, &response.body),
               "set write data");

  if (method == HttpMethod::Put) {
    require_curl(curl_easy_setopt(curl.get(), CURLOPT_UPLOAD, 1L),
                 "set upload");
    require_curl(curl_easy_setopt(curl.get(), CURLOPT_CUSTOMREQUEST, "PUT"),
                 "set PUT method");
    require_curl(curl_easy_setopt(
                     curl.get(), CURLOPT_READFUNCTION, read_body_callback),
                 "set read callback");
    require_curl(curl_easy_setopt(curl.get(), CURLOPT_READDATA, &upload),
                 "set read data");
    require_curl(
        curl_easy_setopt(curl.get(),
                         CURLOPT_INFILESIZE_LARGE,
                         static_cast<curl_off_t>(request_body.size())),
        "set upload size");
  }

  const CURLcode result = curl_easy_perform(curl.get());
  if (result != CURLE_OK) {
    std::string message = "S3 checkpoint request failed: ";
    message += error_buffer[0] != '\0' ? error_buffer.data()
                                       : curl_easy_strerror(result);
    throw std::runtime_error(std::move(message));
  }

  require_curl(curl_easy_getinfo(
                   curl.get(), CURLINFO_RESPONSE_CODE, &response.status),
               "get response code");
  return response;
}

}  // namespace

S3CheckpointStore::S3CheckpointStore(S3CheckpointStoreConfig config)
    : config_(std::move(config)) {
  config_.endpoint = normalize_endpoint(std::move(config_.endpoint));
  config_.bucket = normalize_bucket(std::move(config_.bucket));
  require_non_blank(config_.access_key, "S3 checkpoint access key");
  require_non_blank(config_.secret_key, "S3 checkpoint secret key");
  require_non_blank(config_.region, "S3 checkpoint region");
  if (config_.connect_timeout_ms <= 0) {
    throw std::invalid_argument(
        "S3 checkpoint connect timeout must be positive");
  }
  if (config_.request_timeout_ms <= 0) {
    throw std::invalid_argument(
        "S3 checkpoint request timeout must be positive");
  }
  config_.prefix = normalize_prefix(std::move(config_.prefix));
  sigv4_scope_ = "aws:amz:" + config_.region + ":s3";
}

void S3CheckpointStore::save(EngineCheckpoint checkpoint) {
  const std::string key = checkpoint_key_for_id(checkpoint.checkpoint_id);
  const std::string body = serialize_checkpoint(checkpoint);
  const HttpResponse response = perform_request(
      config_, sigv4_scope_, object_url(key), HttpMethod::Put, body);
  if (response.status < 200 || response.status >= 300) {
    throw http_error("put", response.status, response.body);
  }
}

std::optional<EngineCheckpoint> S3CheckpointStore::load_latest() const {
  const std::vector<std::string> keys = list_checkpoint_keys();
  if (keys.empty()) {
    return std::nullopt;
  }

  const HttpResponse response = perform_request(
      config_, sigv4_scope_, object_url(keys.back()), HttpMethod::Get);
  if (response.status < 200 || response.status >= 300) {
    throw http_error("get", response.status, response.body);
  }

  return try_deserialize_checkpoint(response.body);
}

const S3CheckpointStoreConfig& S3CheckpointStore::config() const noexcept {
  return config_;
}

std::string S3CheckpointStore::checkpoint_key_for_id(
    std::string_view checkpoint_id) const {
  return config_.prefix + checkpoint_filename_for_id(checkpoint_id);
}

std::string S3CheckpointStore::object_url(std::string_view key) const {
  return config_.endpoint + "/" + percent_encode(config_.bucket, false) + "/" +
         percent_encode(key, true);
}

std::string S3CheckpointStore::list_url(
    std::optional<std::string_view> continuation_token) const {
  std::string url = config_.endpoint + "/" +
                    percent_encode(config_.bucket, false) +
                    "?list-type=2&prefix=" +
                    percent_encode(config_.prefix, false);
  if (continuation_token.has_value()) {
    url += "&continuation-token=";
    url += percent_encode(*continuation_token, false);
  }
  return url;
}

std::vector<std::string> S3CheckpointStore::list_checkpoint_keys() const {
  std::vector<std::string> candidates;
  std::optional<std::string> continuation_token;

  while (true) {
    const std::string url =
        list_url(continuation_token.has_value()
                     ? std::optional<std::string_view>{*continuation_token}
                     : std::nullopt);
    const HttpResponse response =
        perform_request(config_, sigv4_scope_, url, HttpMethod::Get);
    if (response.status < 200 || response.status >= 300) {
      throw http_error("list", response.status, response.body);
    }

    for (auto key : xml_tag_values(response.body, "Key")) {
      if (starts_with_prefix(key, config_.prefix) &&
          ends_with(key, CheckpointFileExtension)) {
        candidates.push_back(std::move(key));
      }
    }

    const auto truncated = first_xml_tag_value(response.body, "IsTruncated");
    if (!truncated.has_value() || *truncated != "true") {
      break;
    }

    continuation_token =
        first_xml_tag_value(response.body, "NextContinuationToken");
    if (!continuation_token.has_value() || continuation_token->empty()) {
      throw std::runtime_error(
          "S3 checkpoint list response was truncated without continuation "
          "token");
    }
  }

  std::sort(candidates.begin(), candidates.end());
  return candidates;
}

}  // namespace cex::checkpoint
