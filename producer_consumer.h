#ifndef _PRODUCER_CONSUMER_H_
#define _PRODUCER_CONSUMER_H_

#include <bits/stdc++.h>

namespace {
using namespace std;
}

namespace producer_consumer {
struct Item {
  int id;
  string content;
  Item(int id, string content) : id(id), content(content) {}
  Item() = default;
};

class ProducerConsumerQueue {
 public:
  explicit ProducerConsumerQueue(int queue_size) : size_(queue_size) {}

  void publish(const Item& m) {
    {
      unique_lock lock(mutex_);
      condition_.wait(lock, [this]() -> bool {
        return data_.size() < size_ || shutdown_;
      });
      if (!shutdown_) {
        data_.push(m);
      }
    }
    condition_.notify_all();
  }

  optional<Item> consume() {
    Item item;
    {
      unique_lock lock(mutex_);
      condition_.wait(lock, [this]() -> bool {
        return !data_.empty() || shutdown_;
      });

      if (shutdown_ && data_.empty()) {
        return nullopt;
      }

      item = data_.front();
      data_.pop();
    }
    condition_.notify_all();
    return item;
  }

  void shutdown() {
    unique_lock lock(mutex_);
    shutdown_ = true;
    condition_.notify_all();
  }

 private:
  int size_;
  mutex mutex_;
  condition_variable condition_;
  queue<Item> data_;
  atomic_bool shutdown_{false};
};

void producer(ProducerConsumerQueue& pcq,
              int producer_id,
              int items_to_produce) {
  for (int i = 0; i < items_to_produce; ++i) {
    int item_id = stoi(to_string(producer_id) + to_string(i + 1));
    Item item(item_id, to_string(item_id));
    pcq.publish(item);
    cout << "Publisher " << producer_id << " published item " << item_id
         << endl;
    this_thread::sleep_for(chrono::milliseconds(rand() % 200 + 10));
  }
  std::cout << "Producer " << producer_id << " shutting down." << std::endl;
}

void consumer(ProducerConsumerQueue& pcq, int consumer_id) {
  while (true) {
    auto item = pcq.consume();
    if (item == nullopt) { break; }
    cout << "Consumer " << consumer_id << " consumed item " << item.value().id
         << endl;
    this_thread::sleep_for(chrono::milliseconds(rand() % 200 + 10));
  }
  std::cout << "Consumer " << consumer_id << " shutting down." << std::endl;
}

void runner() {
  vector<thread> producers, consumers;
  int producer_cnt = 2;
  int consumer_cnt = 3;
  int per_producer_item = 5;

  ProducerConsumerQueue pcq(1);

  for (int i = 0; i < producer_cnt; ++i) {
    producers.emplace_back(producer, ref(pcq), i + 1, per_producer_item);
  }

  for (int i = 0; i < consumer_cnt; ++i) {
    consumers.emplace_back(consumer, ref(pcq), i + 1);
  }

  this_thread::sleep_for(chrono::seconds(4));

  pcq.shutdown();

  for (auto& p: producers) {
    p.join();
  }
  for (auto& p: consumers) {
    p.join();
  }
}
}

#endif //_PRODUCER_CONSUMER_H_
