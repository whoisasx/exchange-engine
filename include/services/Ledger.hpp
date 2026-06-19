#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "services/ProtocolMessage.hpp"

namespace services {

struct LedgerEntry {
  std::uint64_t user_id{0};
  std::string kind;
  std::string asset;
  std::int64_t total_delta{0};
  std::int64_t locked_delta{0};
  std::string reference_id;
  StreamPosition source_position;
};

enum class LedgerConsumeStatus {
  Ignored,
  Duplicate,
  Written,
  Invalid,
};

struct LedgerConsumeResult {
  LedgerConsumeStatus status{LedgerConsumeStatus::Ignored};
  std::vector<LedgerEntry> entries;
  std::string reason;
};

class ILedgerStore {
public:
  virtual ~ILedgerStore() = default;

  [[nodiscard]] virtual bool already_recorded(
      const StreamPosition& position) const = 0;

  // Implementations should write one immutable journal row for the stream record
  // and zero or more normalized ledger_entries in the same transaction.
  virtual void append_journal_and_entries(
      const ProtocolMessage& wallet_event,
      const std::vector<LedgerEntry>& entries) = 0;
};

class LedgerService {
public:
  explicit LedgerService(ILedgerStore& store);

  // Contract: ledger consumes wallet.events only. Engine events may be audit
  // context referenced by wallet payloads, but they are not accounting facts and
  // must not create ledger rows on their own.
  [[nodiscard]] LedgerConsumeResult consume(const ProtocolMessage& message);

  [[nodiscard]] static std::vector<LedgerEntry> normalize_wallet_event(
      const ProtocolMessage& wallet_event);

private:
  ILedgerStore& store_;
};

}  // namespace services
