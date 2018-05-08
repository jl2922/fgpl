#pragma once

#include <cstddef>
#include <functional>
#include <type_traits>

namespace fgpl {
namespace internal {
namespace hash {

template <class K, class V, class H = std::hash<K>, class Enable = void>
class HashEntry {
 public:
  K key;

  V value;

  bool filled;

  HashEntry() : filled(false){};

  void fill(const K& key, const size_t hash_value, const V& value) {
    this->key = key;
    this->hash_value = hash_value;
    this->value = value;
    filled = true;
  }

  bool key_equal(const K& key, const size_t hash_value) const {
    return this->hash_value == hash_value && this->key == key;
  }

  size_t get_hash_value() const { return hash_value; }

 private:
  size_t hash_value;
};

template <class K, class V, class H>
class HashEntry<K, V, H, typename std::enable_if<std::is_arithmetic<K>::value, void>::type> {
 public:
  K key;

  V value;

  bool filled;

  H hasher;

  HashEntry() : filled(false){};

  void fill(const K& key, const size_t, const V& value) {
    this->key = key;
    this->value = value;
    filled = true;
  }

  bool key_equal(const K& key, const size_t) const { return this->key == key; }

  size_t get_hash_value() const { return hasher(key); }
};
}  // namespace hash
}  // namespace internal
}  // namespace fgpl
