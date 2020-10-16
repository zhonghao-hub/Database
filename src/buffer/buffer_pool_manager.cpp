//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);
  Page *page;
  if(page_id == INVALID_PAGE_ID){return nullptr;}
  frame_id_t frame_id;
  auto found = page_table_.find(page_id);
  if(found != page_table_.end()){
    frame_id = page_table_[page_id];
    page = &pages_[frame_id];
    replacer_->Pin(frame_id);
    page->pin_count_++;
    return page;
  }
  if(!free_list_.empty()){
    frame_id = free_list_.front();
    page=&pages_[frame_id];
    free_list_.erase(free_list_.begin());
  }
  else{
    if(!replacer_->Victim(&frame_id))return nullptr;
    page = &pages_[frame_id];
    if(page->IsDirty()){
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->is_dirty_=false;
    }
    page_table_.erase(page->GetPageId());
  }
  disk_manager_->ReadPage(page_id, page->GetData());
  page_table_[page_id]=frame_id;
  page->page_id_=page_id;
  // std::cout<<"!!!!!"<<page->GetData()<<"!!!!!";
  page->pin_count_++;

  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  Page *page = nullptr;
  if(page_table_.find(page_id) == page_table_.end()){
    // std::cout<<"find:false";
    return false;
  }
  frame_id = page_table_[page_id];
  page = &pages_[frame_id];
  if(page->pin_count_<1)return false;
  page->pin_count_--;
  if(page->pin_count_==0){
    replacer_->Unpin(frame_id);
  }
  if(is_dirty)page->is_dirty_=true;
  return true; 
  }

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  Page *page=nullptr;
  if(page_id == INVALID_PAGE_ID){return false;}
  if(page_table_.find(page_id) == page_table_.end()){
    return false;
  }
  frame_id = page_table_[page_id];
  page = &pages_[frame_id];
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_=false;
  return true;
  
  // Make sure you call DiskManager::WritePage!
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  Page *page=nullptr;
  if(!free_list_.empty()){
    frame_id = free_list_.front();
    // std::cout<<"--new:"<<frame_id<<"--";
    page=&pages_[frame_id];
    free_list_.erase(free_list_.begin()); 
  }
  else{
    if(!replacer_->Victim(&frame_id)){
      return nullptr;
    }
    page = &pages_[frame_id];
    if(page->is_dirty_){
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->is_dirty_=false;
    }
    page_table_.erase(page->GetPageId());
  }
  
  *page_id = disk_manager_->AllocatePage();
  // std::cout<<"new page id: "<<*page_id<<"--";
  page_table_[*page_id]=frame_id;
  page->ResetMemory();
  page->page_id_=*page_id;
  page->is_dirty_=true;
  page->pin_count_++;
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  Page *page=nullptr;
  if(page_table_.find(page_id)==page_table_.end()){disk_manager_->DeallocatePage(page_id); return true;}
  frame_id = page_table_[page_id];
  page = &pages_[frame_id];
  if(page->pin_count_>0){return false;}
  replacer_->Pin(frame_id);
  page_table_.erase(page->GetPageId());
  free_list_.push_back(frame_id);
  page->page_id_=INVALID_PAGE_ID;
  page->is_dirty_=false;
  page->ResetMemory();

  disk_manager_->DeallocatePage(page_id);

  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  Page *page=nullptr;
  for(auto iter=page_table_.begin(); iter!=page_table_.end();iter++){
    frame_id = iter->second;
    page = &pages_[frame_id];
    page_table_.erase(iter->first);
    disk_manager_->WritePage(iter->first, page->GetData());
    page->is_dirty_=false;
  }
}

}  // namespace bustub
