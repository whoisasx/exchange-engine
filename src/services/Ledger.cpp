#include "services/Ledger.hpp"

#include <stdexcept>
#include <utility>

namespace services {
namespace {

std::uint64_t required_u64(const ProtocolMessage& message,
                           const std::string& field) {
  const auto value = payload_u64(message, field);
  if (!value.has_value()) {
    throw std::invalid_argument("missing or invalid payload field: " + field);
  }
  return *value;
}

std::int64_t required_i64(const ProtocolMessage& message,
                          const std::string& field) {
  const auto value = payload_i64(message, field);
  if (!value.has_value()) {
    throw std::invalid_argument("missing or invalid payload field: " + field);
  }
  return *value;
}

std::string required_string(const ProtocolMessage& message,
                            const std::string& field) {
  const auto value = payload_string(message, field);
  if (!value.has_value() || value->empty()) {
    throw std::invalid_argument("missing payload field: " + field);
  }
  return *value;
}

LedgerEntry make_entry(const ProtocolMessage& event,
                       std::uint64_t user_id,
                       std::string kind,
                       std::string asset,
                       std::int64_t total_delta,
                       std::int64_t locked_delta,
                       std::string reference_id) {
  return LedgerEntry{
      user_id,
      std::move(kind),
      std::move(asset),
      total_delta,
      locked_delta,
      std::move(reference_id),
      event.position(),
  };
}

}  // namespace

LedgerService::LedgerService(ILedgerStore& store) : store_(store) {}

LedgerConsumeResult LedgerService::consume(const ProtocolMessage& message) {
  if (message.topic != WalletEventsTopic) {
    return LedgerConsumeResult{LedgerConsumeStatus::Ignored, {}, {}};
  }

  if (store_.already_recorded(message.position())) {
    return LedgerConsumeResult{LedgerConsumeStatus::Duplicate, {}, {}};
  }

  try {
    auto entries = normalize_wallet_event(message);
    store_.append_journal_and_entries(message, entries);
    return LedgerConsumeResult{
        LedgerConsumeStatus::Written,
        std::move(entries),
        {},
    };
  } catch (const std::invalid_argument& error) {
    return LedgerConsumeResult{LedgerConsumeStatus::Invalid, {}, error.what()};
  }
}

std::vector<LedgerEntry> LedgerService::normalize_wallet_event(
    const ProtocolMessage& wallet_event) {
  if (wallet_event.topic != WalletEventsTopic) {
    return {};
  }

  const auto user_id = required_u64(wallet_event, "user_id");

  if (wallet_event.type == "DepositApplied") {
    const auto amount = required_i64(wallet_event, "amount");
    return {make_entry(wallet_event,
                       user_id,
                       "DEPOSIT",
                       required_string(wallet_event, "asset"),
                       amount,
                       0,
                       required_string(wallet_event, "reference_id"))};
  }

  if (wallet_event.type == "WithdrawalApplied") {
    const auto amount = required_i64(wallet_event, "amount");
    return {make_entry(wallet_event,
                       user_id,
                       "WITHDRAWAL",
                       required_string(wallet_event, "asset"),
                       -amount,
                       0,
                       required_string(wallet_event, "request_id"))};
  }

  if (wallet_event.type == "FundsReserved") {
    const auto amount = required_i64(wallet_event, "amount");
    return {make_entry(wallet_event,
                       user_id,
                       "RESERVE",
                       required_string(wallet_event, "asset"),
                       0,
                       amount,
                       required_string(wallet_event, "reservation_id"))};
  }

  if (wallet_event.type == "FundsReleased") {
    const auto amount = required_i64(wallet_event, "amount");
    return {make_entry(wallet_event,
                       user_id,
                       "RELEASE",
                       required_string(wallet_event, "asset"),
                       0,
                       -amount,
                       required_string(wallet_event, "reservation_id"))};
  }

  if (wallet_event.type == "TradeSettled") {
    const auto debit_amount = required_i64(wallet_event, "debit_amount");
    const auto credit_amount = required_i64(wallet_event, "credit_amount");
    const auto fill_id = required_string(wallet_event, "fill_id");
    return {
        make_entry(wallet_event,
                   user_id,
                   "TRADE_DEBIT",
                   required_string(wallet_event, "debit_asset"),
                   -debit_amount,
                   -debit_amount,
                   fill_id),
        make_entry(wallet_event,
                   user_id,
                   "TRADE_CREDIT",
                   required_string(wallet_event, "credit_asset"),
                   credit_amount,
                   0,
                   fill_id),
    };
  }

  if (wallet_event.type == "AccountDeltaApplied") {
    return {make_entry(wallet_event,
                       user_id,
                       required_string(wallet_event, "kind"),
                       required_string(wallet_event, "asset"),
                       required_i64(wallet_event, "total_delta"),
                       required_i64(wallet_event, "locked_delta"),
                       required_string(wallet_event, "reference_id"))};
  }

  return {};
}

}  // namespace services
