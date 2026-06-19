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
Order* PriceLevel::pop_front(){
  Order* removed=head;
  if(removed==nullptr) return nullptr;

  head=removed->nextOrder;
  if(head!=nullptr){
    head->previousOrder=nullptr;
  }
  else{
    tail=nullptr;
  }

  if(!removed->remainingQuantity.is_zero()){
    decrease_total(removed->remainingQuantity);
  }
  if(orderCount>0){
    --orderCount;
  }
  removed->detach_links();
  return removed;
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
void PriceLevel::clear(){
  Order* current=head;
  while(current){
    Order *next=current->nextOrder;
    current->detach_links();
    current=next;
  }
  head=nullptr;
  tail=nullptr;
  orderCount=0;
  totalQuantity=Quantity{};
}
