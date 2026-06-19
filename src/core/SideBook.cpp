#include"SideBook.hpp"
#include"PriceLevel.hpp"
#include"Order.hpp"

namespace {
[[nodiscard]] bool is_better_price(Side side, Price candidate, Price current){
  if(side==Buy){
    return candidate>current;
  }
  return candidate<current;
}

[[nodiscard]] int64_t price_to_ladder_tick(Price price, int64_t tickSizeTicks) {
  if(tickSizeTicks<=0){
    return price.ticks();
  }
  return price.ticks()/tickSizeTicks;
}
}

SideBook::SideBook(Side side, int64_t tickSizeTicks, int64_t baseTick, uint64_t capacity)
  : side(side),
    ringPriceLadder(side, tickSizeTicks, baseTick, capacity),
    farPriceMap(side){}

void SideBook::insert_resting_order(Order* order){
  auto* price_level=get_or_create_level_for_price(order->price);
  price_level->push_back(order);

  if(cachedBestLevel==nullptr || is_better_price(side, price_level->price, cachedBestLevel->price)){
    cachedBestLevel=price_level;
  }
}

PriceLevel* SideBook::best_level() const{
  if(cachedBestLevel==nullptr || cachedBestLevel->empty()){
    return nullptr;
  }
  return cachedBestLevel;
}

Order* SideBook::best_order() const{
  const auto* level=best_level();
  return level==nullptr ? nullptr : level->front();
}

void SideBook::remove_order(Order* order){
  if(order==nullptr || order->priceLevel==nullptr) return;

  auto* priceLevel=order->priceLevel;
  if(order->previousOrder!=nullptr){
    order->previousOrder->nextOrder=order->nextOrder;
  }
  else{
    priceLevel->head=order->nextOrder;
  }

  if(order->nextOrder!=nullptr){
    order->nextOrder->previousOrder=order->previousOrder;
  }
  else{
    priceLevel->tail=order->previousOrder;
  }

  priceLevel->decrease_total(order->remainingQuantity);
  if(priceLevel->orderCount>0){
    --priceLevel->orderCount;
  }
  order->detach_links();
  update_after_fill_or_cancel(priceLevel);
}

void SideBook::update_after_fill_or_cancel(PriceLevel* priceLevel){
  if(priceLevel==nullptr) return;

  const auto price=priceLevel->price;
  const bool wasCachedBest = cachedBestLevel==priceLevel;

  if(priceLevel->empty() || priceLevel->totalQuantity.is_zero()){
    if(ringPriceLadder.get_level(price)==priceLevel){
      ringPriceLadder.remove_level_if_empty(price);
    }
    else{
      farPriceMap.remove_level_if_empty(price);
    }
  }

  if(wasCachedBest || cachedBestLevel==nullptr || cachedBestLevel->empty()){
    refresh_cached_best();
  }
}

bool SideBook::price_crosses(Price incomingPrice) const{
  const auto* level=best_level();
  if(level==nullptr){
    return false;
  }

  if(side==Sell){
    return incomingPrice>=level->price;
  }
  return incomingPrice<=level->price;
}

PriceLevel* SideBook::get_or_create_level_for_price(Price price){
  const auto ladderTick=price_to_ladder_tick(price, ringPriceLadder.tickSizeTicks);
  if(ringPriceLadder.contains_tick(ladderTick)){
    return ringPriceLadder.get_or_create_level(price);
  }
  return farPriceMap.get_or_create_level(price);
}

void SideBook::refresh_cached_best(){
  auto* ringBest=ringPriceLadder.best_level();
  auto* farBest=farPriceMap.best_level();

  if(ringBest==nullptr){
    cachedBestLevel=farBest;
    return;
  }
  if(farBest==nullptr){
    cachedBestLevel=ringBest;
    return;
  }

  cachedBestLevel=is_better_price(side, ringBest->price, farBest->price) ? ringBest : farBest;
}

bool SideBook::empty() const{
  return best_level()==nullptr;
}
