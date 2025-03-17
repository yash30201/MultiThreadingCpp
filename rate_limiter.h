#ifndef _RATE_LIMITER_H_
#define _RATE_LIMITER_H_

#include <bits/stdc++.h>

namespace rate_limiter {
namespace {
using namespace std;

struct RateLimiterI {
  ~RateLimiterI() {}
  virtual bool request(string requestId) = 0;
  virtual optional<string> consume() = 0;
};

class TokenBucketRateLimiter : public RateLimiterI {
 public:
  TokenBucketRateLimiter(int bucket_size, double tokens_per_second) :
      bucket_size_{bucket_size},
      tokens_per_second_{tokens_per_second},
      last_refill_time_(chrono::steady_clock::now()),
      tokens_{bucket_size} {}

  bool request(string requestID) override {
    bool allowed = false;
    {
      unique_lock lock(mutex_);
      if (!shutdown_) {
        refillTokens();
        if (tokens_ > 0) {
          allowed = true;
          --tokens_;
          requests_.push(requestID);
        }
      }
    }
    condition_.notify_all();
    return allowed;
  }

  optional<string> consume() override {
    optional<string> requestId = nullopt;
    {
      unique_lock lock(mutex_);
      condition_.wait(lock, [this]() -> bool {
        return !requests_.empty() || shutdown_;
      });
      if (!shutdown_ || !requests_.empty()) {
        requestId = requests_.front();
        requests_.pop();
      }
    }

    // Notify consumers
    condition_.notify_all();

    if (requestId.has_value()) {
      // Simulate request processing
      this_thread::sleep_for(chrono::milliseconds(200 + rand() % 100));
    }

    return requestId;
  }

  void shutdown() {
    unique_lock lock(mutex_);
    shutdown_ = true;
    condition_.notify_all();
  }

 private:
  int bucket_size_;
  mutex mutex_;
  chrono::steady_clock::time_point last_refill_time_;
  double tokens_per_second_;
  int tokens_;
  condition_variable condition_;
  queue<string> requests_;
  bool shutdown_ = false;

  void refillTokens() {
    auto new_refill_time = chrono::steady_clock::now();
    double time_elapsed = chrono::duration_cast<chrono::duration<double>>(
        new_refill_time - last_refill_time_).count();
    double tokens_to_add = time_elapsed * tokens_per_second_;
    if (tokens_to_add > 0) {
      tokens_ = min(bucket_size_, static_cast<int>(tokens_ + tokens_to_add));
      last_refill_time_ = new_refill_time;
    }
  }
};

void token_bucket_runner() {
  cout << "Starting token bucket runner\n" << endl;
  int bucket_size = 4;
  double tokens_per_second = 2.0;
  int number_of_clients = 3, per_client_request = 5, number_of_consumers = 3;
  TokenBucketRateLimiter rateLimiter(bucket_size, tokens_per_second);

  vector<thread> consumers;
  auto consumerRunner = [&rateLimiter](int consumerId) {
    while (true) {
      auto requestId = rateLimiter.consume();
      if (!requestId.has_value()) {
        cout << "Closing consumer " << consumerId << endl;
        break;
      }
      cout << "Processed request " << requestId.value() << endl;
    }
  };
  for (int i = 0; i < number_of_consumers; ++i) {
    consumers.emplace_back(consumerRunner, i + 1);
  }

  vector<thread> clients;
  auto clientRunner = [&rateLimiter, per_client_request](int client_id) {
    for (int i = 0; i < per_client_request; ++i) {
      string requestId = to_string(client_id) + ":" + to_string(i);
      cout << "Client " << client_id << " sent request  " << requestId << endl;
      rateLimiter.request(requestId);
      this_thread::sleep_for(chrono::milliseconds(500 + rand() % 1000));
    }
    cout << "Client " << client_id << " completed running" << endl;
  };
  for (int i = 0; i < number_of_clients; ++i) {
    clients.emplace_back(clientRunner, i + 1);
  }

  for (auto& t: clients) {
    t.join();
  }
  rateLimiter.shutdown();
  this_thread::sleep_for(chrono::seconds(2));
  for (auto& t: consumers) {
    t.join();
  }

  cout << "Stopping token bucket runner\n" << endl;
}

class LeakyBucketRateLimiter : public RateLimiterI {
 public:
  LeakyBucketRateLimiter(int bucket_size, double leak_rate) :
      bucket_size_{bucket_size},
      leak_rate_{leak_rate} {}

  bool request(string requestId) override {
    bool allowed = false;
    {
      unique_lock lock(mutex_);
      if (!shutdown_ && requests_.size() < bucket_size_) {
        requests_.push(requestId);
        allowed = true;
      }
    }
    consumer_.notify_one();
    return allowed;
  }

  optional<string> consume() override {
    optional<string> requestId = nullopt;
    {
      unique_lock lock(mutex_);
      consumer_.wait(lock, [this]() -> bool {
        return !requests_.empty() || shutdown_;
      });
      if (!shutdown_ || !requests_.empty()) {
        requestId = requests_.front();
        requests_.pop();
      }
    }

    producer_.notify_all();
    consumer_.notify_one();

    return requestId;
  }

  void startConsumers(int consumerCount) {
    for (int i = 0; i < consumerCount; ++i) {
      consumers_.emplace_back([this, i]() {
        while (true) {
          auto requestId = consume();
          if (!requestId.has_value()) {
            cout << "Stopping consumer " << i << endl;
            break;
          }
          cout << "Processed request " << requestId.value() << endl;
          this_thread::sleep_for(chrono::milliseconds(static_cast<int>(1000
              / leak_rate_)));
        }
      });
    }
  }

  void shutdown() {
    unique_lock lock(mutex_);
    shutdown_ = true;
    producer_.notify_all();
    consumer_.notify_all();
  }

  void shutDownConsumers() {
    for (int i = 0 ; i < consumers_.size() ; ++i) {
      consumers_[i].join();
      cout << "Closed consumer " << i << endl;
    }
  }
 private:
  int bucket_size_;
  mutex mutex_;
  double leak_rate_;
  queue<string> requests_;
  condition_variable consumer_, producer_;
  bool shutdown_ = false;
  vector<thread> consumers_;
};

void leaky_bucket_runner() {
  cout << "Starting leaky bucket runner\n" << endl;
  int bucket_size = 4;
  double leak_rate_per_second = 0.5;
  int number_of_clients = 3, per_client_request = 5, number_of_consumers = 3;
  LeakyBucketRateLimiter rateLimiter(bucket_size, leak_rate_per_second);

  rateLimiter.startConsumers(number_of_consumers);

  vector<thread> clients;
  auto clientRunner = [&rateLimiter, per_client_request](int client_id) {
    for (int i = 0; i < per_client_request; ++i) {
      string requestId = to_string(client_id) + ":" + to_string(i);
      if (rateLimiter.request(requestId)) {
        cout << "Request " << requestId << " allowed" << endl;
      } else {
        cout << "Request " << requestId << " denied" << endl;
      }
      this_thread::sleep_for(chrono::milliseconds(400 + rand() % 1000));
    }
    cout << "Client " << client_id << " completed running" << endl;
  };
  for (int i = 0; i < number_of_clients; ++i) {
    clients.emplace_back(clientRunner, i + 1);
  }

  for (auto& t: clients) {
    t.join();
  }
  rateLimiter.shutdown();
  this_thread::sleep_for(chrono::seconds(2));
  rateLimiter.shutDownConsumers();

  cout << "Stopping leaky bucket runner\n" << endl;
}

}

void runner() {
//  token_bucket_runner();
  leaky_bucket_runner();
}


}

#endif //_RATE_LIMITER_H_
