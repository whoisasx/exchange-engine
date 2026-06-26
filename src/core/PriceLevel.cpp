#include"PriceLevel.hpp"
#include"FixedPoint.hpp"
#include"Order.hpp"

void PriceLevel::push_back(Order* order){
  if(order==nullptr) return;
  order->nextOrder=nullptr;
  order->previousOrder=tail;
  order->priceLevel=this;
  if(tail!=nullptr){
    tail->nextOrder=order;
  }
  else{
    head=order;
  }
  tail=order;
  ++orderCount;
  increase_total(order->remainingQuantity);
}
void PriceLevel::increase_total(Quantity quantity){
  totalQuantity.add_lots(quantity.lots());
}
void PriceLevel::decrease_total(Quantity quantity){
  if(quantity.lots()>=totalQuantity.lots()){
    totalQuantity=Quantity{};
    return;
  }
  totalQuantity.subtract_lots(quantity.lots());
}
