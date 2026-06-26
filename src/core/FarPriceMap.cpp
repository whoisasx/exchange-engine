#include"FarPriceMap.hpp"
#include"PriceLevel.hpp"
#include"FixedPoint.hpp"

FarPriceMap::FarPriceMap(Side side):side(side){}
PriceLevel* FarPriceMap::get_or_create_level(Price price){
  auto it = levels.find(price);
  if(it != levels.end()) return it->second.get();
  auto level = std::make_unique<PriceLevel>(price);
  levels[price]=std::move(level);
  return levels[price].get();
}
void FarPriceMap::remove_level_if_empty(Price price){
  auto it=levels.find(price);
  if(it == levels.end()) return;
  if(it->second->empty() || it->second->totalQuantity.is_zero()) levels.erase(it);
  return;
}
PriceLevel* FarPriceMap::best_level(){
  if(side==Buy){
    for(auto it=levels.rbegin(); it!=levels.rend(); ++it){
      if(!it->second->empty()) return it->second.get();
    }
  }
  else{
    for(auto it=levels.begin(); it!=levels.end(); ++it){
      if(!it->second->empty()) return it->second.get();
    }
  }
  return nullptr;
}
