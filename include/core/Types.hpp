#pragma once
#include<cstdint>

using SymbolId=uint32_t;
using UserId=uint64_t;
using OrderId=uint64_t;
using ClientOrderId=uint64_t; // id sent by the rust api client
using CommandId=uint64_t; // tracking the request/instruction
using TradeId=uint64_t;
using EventId=uint64_t; // unique id for output stream
using EngineSequence=uint64_t; // monotonically increasing counter for determinsim
using AssetId=uint32_t;
using FeeRate=uint32_t;


enum Side{
  Buy=0,
  Sell=1
};

enum  OrderType{
  Limit=0,
  Market=1
};

enum TimeInForce{
  GTC=0, // good till cancelled
  IOC=1, // immediate or cancel
  FOK=2, // fill or kill
  PO=3 // post only
};

enum OrderStatus{
  Pending=0,
  New=1,
  PartiallyFilled=2,
  Filled=3,
  Cancelled=4, 
  Rejected=5
};

enum RejectReason{
  InvalidSymbol=0,
  InvalidPrice=1,
  InvalidQuantity=2,
  DuplicateCommand=3,
  DuplicateOrder=4,
  OrderNotFound=5,
  WouldSelfTrade=6, // if the user places same order on ask and bid both.
  MarketClosed=7,
  InsufficientBalanceLiquidity=8
};
