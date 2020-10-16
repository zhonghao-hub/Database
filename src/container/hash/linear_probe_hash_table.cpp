//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
      auto header_page = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->NewPage(&header_page_id_, nullptr)->GetData());
      num_buckets_ = num_buckets;
      header_page->SetSize(buffer_pool_manager->GetPoolSize());
      for( size_t i=0; i< buffer_pool_manager->GetPoolSize(); i++){
        page_id_t block_page_id;
        buffer_pool_manager->NewPage(&block_page_id, nullptr);
        header_page->AddBlockPageId(block_page_id);
      }

    }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_, nullptr)->GetData());
  auto hash_key = hash_fn_.GetHash(key);
  auto header_index = (hash_key%num_buckets_)/(num_buckets_/buffer_pool_manager_->GetPoolSize());
  auto first_header = header_index;
  auto block_index = (hash_key%num_buckets_)%(num_buckets_/buffer_pool_manager_->GetPoolSize());
  auto first_block = block_index;
  // std::cout<<"--"<<"key():"<<key<<"--"<<"hash_key:"<<hash_key<<"--"<<"header_index:"<<header_index<<"--"<<"block_index:"<<block_index<<"--"<<std::endl;
  page_id_t block_page_id = header_page->GetBlockPageId(header_index);
  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(buffer_pool_manager_->FetchPage(block_page_id, nullptr)->GetData());
  if(!block_page->IsOccupied(block_index)){
    return false;
  }
  if(block_page->IsReadable(block_index) && !comparator_(block_page->KeyAt(block_index), key)){
    // std::cout<<"---"<<"ValueAt()"<<"---"<<block_page->ValueAt(block_index);
    // table_latch_.RLock();
    result->push_back(block_page->ValueAt(block_index));
    // table_latch_.RUnlock();
  }
  if(block_index < num_buckets_/buffer_pool_manager_->GetPoolSize()-1){
    block_index++;
  }
  else{
    header_index = (header_index+1)%buffer_pool_manager_->GetPoolSize();
    block_index = 0;
  }
  while(!(first_header == header_index and first_block == block_index)){
    // std::cout<<block_index;
    // std::cout<<comparator_(block_page->KeyAt(block_index), 
    page_id_t block_page_id = header_page->GetBlockPageId(header_index);
    auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(buffer_pool_manager_->FetchPage(block_page_id, nullptr)->GetData());
    if(!block_page->IsOccupied(block_index)){
      break;
    }
    if(block_page->IsReadable(block_index) && !comparator_(block_page->KeyAt(block_index), key)){
      // std::cout<<"---"<<"ValueAt()"<<"---"<<block_page->ValueAt(block_index);
      // table_latch_.RLock();
      result->push_back(block_page->ValueAt(block_index));
      // table_latch_.RUnlock();
    }
    if(block_index < num_buckets_/buffer_pool_manager_->GetPoolSize()-1){
      block_index++;
    }
    else{
      header_index = (header_index+1)%buffer_pool_manager_->GetPoolSize();
      block_index = 0;
    }
  }
  return true;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_, nullptr)->GetData());
  auto hash_key = hash_fn_.GetHash(key);
  auto header_index = (hash_key%num_buckets_)/(num_buckets_/buffer_pool_manager_->GetPoolSize());
  auto first_header = header_index;
  auto block_index = (hash_key%num_buckets_)%(num_buckets_/buffer_pool_manager_->GetPoolSize());
  auto first_block = block_index;
  // std::cout<<"--"<<"key():"<<key<<"--"<<"hash_key:"<<hash_key<<"--"<<"header_index:"<<header_index<<"--"<<"block_index:"<<block_index<<"--"<<std::endl;
  page_id_t block_page_id = header_page->GetBlockPageId(header_index);
  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(buffer_pool_manager_->FetchPage(block_page_id, nullptr)->GetData());
  if(!block_page->IsOccupied(block_index)){
    // table_latch_.WLock();
    // table_latch_.RLock();
    bool  B = block_page->Insert(block_index, key, value);
    // table_latch_.RUnlock();
    // table_latch_.WUnlock();
  return B;
  }
  if(block_page->IsReadable(block_index) && !comparator_(block_page->KeyAt(block_index), key) && block_page->ValueAt(block_index)==value){
    return false;
  }
  if(block_index < num_buckets_/buffer_pool_manager_->GetPoolSize()-1){
    block_index++;
  }
  else{
    header_index = (header_index+1)%buffer_pool_manager_->GetPoolSize();
    block_index = 0;
  }
  while(!(first_header == header_index and first_block == block_index)){
    page_id_t block_page_id = header_page->GetBlockPageId(header_index);
    auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(buffer_pool_manager_->FetchPage(block_page_id, nullptr)->GetData());
    if(!block_page->IsOccupied(block_index)){
      break;
    }
    if(block_page->IsReadable(block_index) && !comparator_(block_page->KeyAt(block_index), key) && block_page->ValueAt(block_index)==value){
      return false;
    }
    if(block_index < num_buckets_/buffer_pool_manager_->GetPoolSize()-1){
      block_index++;
    }
    else{
      header_index = (header_index+1)%buffer_pool_manager_->GetPoolSize();
      block_index = 0;
    }
    
  }
  if(first_header == header_index and first_block == block_index){
    Resize(num_buckets_);
    return Insert(transaction, key, value);
  }
  // std::cout<<"--"<<"insert()"<<"--"<<"("<<header_index<<","<<block_index<<","<<key<<","<<value<<")"<<std::endl;
  // table_latch_.WLock();
  // table_latch_.RLock();
  bool  B = block_page->Insert(block_index, key, value);
  // table_latch_.RUnlock();
  // table_latch_.WUnlock();
  return B;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_, nullptr)->GetData());
  auto hash_key = hash_fn_.GetHash(key);
  auto header_index = (hash_key%num_buckets_)/(num_buckets_/buffer_pool_manager_->GetPoolSize());
  auto first_header = header_index;
  auto block_index = (hash_key%num_buckets_)%(num_buckets_/buffer_pool_manager_->GetPoolSize());
  auto first_block = block_index;
  // std::cout<<"--"<<"key():"<<key<<"--"<<"hash_key:"<<hash_key<<"--"<<"header_index:"<<header_index<<"--"<<"block_index:"<<block_index<<"--"<<std::endl;
  page_id_t block_page_id = header_page->GetBlockPageId(header_index);
  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(buffer_pool_manager_->FetchPage(block_page_id, nullptr)->GetData());
  if(block_page->IsOccupied(block_index)){
    if(block_page->IsReadable(block_index) && !comparator_(block_page->KeyAt(block_index), key) && block_page->ValueAt(block_index)==value){
      // table_latch_.WLock();
      // table_latch_.RLock();
      block_page->Remove(block_index);
      // table_latch_.RUnlock();
      // table_latch_.WUnlock();
      return true;
    }
    if(block_index < num_buckets_/buffer_pool_manager_->GetPoolSize()-1){
      block_index++;
    }
    else{
      header_index = (header_index+1)%buffer_pool_manager_->GetPoolSize();
      block_index = 0;
    }
  }
  while(!(first_header == header_index and first_block == block_index)){
    page_id_t block_page_id = header_page->GetBlockPageId(header_index);
    auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(buffer_pool_manager_->FetchPage(block_page_id, nullptr)->GetData());
    if(!block_page->IsOccupied(block_index)){
      break;
    }
    if(block_page->IsReadable(block_index) && !comparator_(block_page->KeyAt(block_index), key) && block_page->ValueAt(block_index)==value){
      // table_latch_.WLock();
      // table_latch_.RLock();
      block_page->Remove(block_index);
      // table_latch_.RUnlock();
      // table_latch_.WUnlock();
      return true;
    }
    if(block_index < num_buckets_/buffer_pool_manager_->GetPoolSize()-1){
      block_index++;
    }
    else{
      header_index = (header_index+1)%buffer_pool_manager_->GetPoolSize();
      block_index = 0;
    }
  }
  return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_, nullptr)->GetData());
  page_id_t new_page_id;
  auto new_header_page = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->NewPage(&new_page_id, nullptr)->GetData());
  new_header_page->SetSize(2*initial_size);
  for(size_t i = 0; i<buffer_pool_manager_->GetPoolSize(); i+=2){
    auto block_id_1 = header_page->GetBlockPageId(i);
    auto block_id_2 = header_page->GetBlockPageId(i+1);
    auto block1 = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(buffer_pool_manager_->FetchPage(block_id_1, nullptr)->GetData());
    auto block2 = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(buffer_pool_manager_->FetchPage(block_id_2, nullptr)->GetData());
    for(size_t j =0; j<num_buckets_/buffer_pool_manager_->GetPoolSize(); j++){
      block1->Insert(num_buckets_/buffer_pool_manager_->GetPoolSize()+j, block2->KeyAt(j), block2->ValueAt(j));
    }
    new_header_page->AddBlockPageId(block_id_1);
  }
  num_buckets_ = 2*initial_size;
  header_page_id_ = new_page_id;
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_, nullptr)->GetData());
  return header_page->GetSize();
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
