//
// Created by Aaditya Rangarajan on 11/9/23.
//

#ifndef CONCURRENT_BETREE_BETREE_HPP
#define CONCURRENT_BETREE_BETREE_HPP
//! concurrent B epsilon tree implementation.
//! this code is based on the B-eps tree from project 2.
//! For now, this only accepts integer keys and values for simplicity.
//! Our focus with this implementation is to provide concurrency to this data structure.
#include <map>
#include <cassert>
#include <cstdio>
#include <cstdint>
#include <vector>
#include <iostream>
#include <sched.h>
#include "lock.hpp"
#define DEFAULT_MAX_NODE_SIZE (1ULL<<18)
#define DEFAULT_MIN_FLUSH_SIZE (DEFAULT_MAX_NODE_SIZE / 16ULL)
//! we will use this to provide hints to the compiler.
//! Our goal is to improve branch prediction accuracy.
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#ifdef DEBUG
#define debug(x) (x)
#else
#define debug(x)
#endif


//! defining operations of a B-epsilon tree.
//! We follow the original implementation and support 3 operations.
//! @xrefitem operations.
enum {
    INSERT,
    DELETE,
    UPDATE
};


class MessageKey {
private:
    //! data members of this class
    uint64_t key;
    uint64_t timestamp;
public:
    //! default constructor
    MessageKey() : key(), timestamp() {}

    //! parameterized constructor
    MessageKey(uint64_t k, uint64_t v) : key(k), timestamp(v) {}

    //! special functions
    static MessageKey get_default_message_key_min_value(const uint64_t &key) {
        return {key, 0};
    }

    static MessageKey get_default_message_key_max_value(const uint64_t &key) {
        return {key, UINT64_MAX};
    }

    //! the below functions will call special functions
    MessageKey range_start(void) const {
        return get_default_message_key_min_value(key);
    }

    MessageKey range_end(void) const {
        return get_default_message_key_max_value(key);
    }

    //! getters and setters
    uint64_t get_key() const {
        return key;
    }

    uint64_t get_timestamp() const {
        return timestamp;
    }
};

//! operator overloads for class MessageKey
bool operator<(const MessageKey &message_key_1, const MessageKey &message_key_2) {
    return message_key_1.get_key() < message_key_2.get_key() ||
           (message_key_1.get_key() == message_key_2.get_key() &&
            message_key_1.get_timestamp() < message_key_2.get_timestamp());
}

bool operator<(const uint64_t &key, const MessageKey &message_key) {
    return key < message_key.get_key();
}

bool operator<(const MessageKey &message_key, const uint64_t &key) {
    return message_key.get_key() < key;
}

bool operator==(const MessageKey &message_key_1, const MessageKey message_key_2) {
    return message_key_1.get_key() == message_key_2.get_key() &&
           message_key_1.get_timestamp() == message_key_2.get_timestamp();
}

//! The value class definition begins
class MessageValue {
private:
    //! each message contains two fields.
    //! The first is the opcode, and then, the new value for a given MessageKey
    int opcode;
    uint64_t value;
public:
    //! default constructor. The opcode is INSERT by default.
    MessageValue() : opcode(INSERT), value() {}

    //! parameterized constructor
    MessageValue(int opcode, const uint64_t value) : opcode(opcode), value(value) {}

    //! getters
    uint64_t get_value() const {
        return value;
    }

    int get_opcode() const {
        return opcode;
    }
};

//! operator overloads for MessageValue class.

//! equality operator overloaded
bool operator==(const MessageValue &message_value_1, const MessageValue &message_value_2) {
    return message_value_1.get_value() == message_value_2.get_value() &&
           message_value_1.get_opcode() == message_value_2.get_opcode();
}

//! betree class definition begins

class betree {
private:
    //! todo: define here
    class node; //! please see @refitem node.

    //! Class to store child node information.
    class child_info {
    public:
        //! public members
        node *child_pointer;
        uint64_t child_size;

        //! default constructor
        child_info() : child_pointer(), child_size() {}

        //! parameterized constructor
        child_info(node *child_ptr, uint64_t size) : child_pointer(child_ptr), child_size(size) {}
    };

    //! the pivots for a node.
    typedef typename std::map<uint64_t, child_info> pivot_map;

    //! the messages for a node.
    typedef typename std::map<MessageKey, MessageValue> message_map;

    //! definition of node class
    //! @xrefitem node
    class node {
    public:

        //! creating a constructor
        node() {
            dirty = true;
        }

        //! public members.
        //! each node has a list of pivots and messages
        //! however, we store a map instead of a list. Idea is the same
        pivot_map pivots;
        message_map messages;
        //! need to keep track of whether a node is dirty or not. True by default
        bool dirty = true;

        //! helper functions
        bool is_leaf() const {
            return pivots.empty();
        }

        bool is_dirty() const {
            return dirty;
        }

        //! The below function might seem complex, but all it does is this:
        //! when called from a const function, it will return a const iterator
        //! when called from a non const function, it returns a non const iterator
        template<class out_type, class in_type>
        static out_type get_pivot(in_type &pivot_map, const uint64_t &key) {
            //! if the map is empty. this should not get called.
            assert(pivot_map.size() > 0);
            //! get an iterator on the lower bound of key.
            //! lower_bound() returns an iterator to a element in the map,
            //! whose key is >= the key passed to the function.
            auto it = pivot_map.lower_bound(key);
            //! as per above comments, if this is the case, they key does not exist.
            if (it == pivot_map.begin() && key < it->first) {
                throw std::out_of_range("The key %d does not exist in the DB.");
            }
            //! if we did find such a key, decrement the iterator.
            if (it == pivot_map.end() || key < it->first) {
                //! move one step back, that is our element
                --it;
            }
            return it;
        }

        //! the below functions call the get_pivot function.
        //! Notice one of them is const, the other is not.
        typename pivot_map::const_iterator get_pivot(const uint64_t &key) const {
            //! return const iterator
            return get_pivot<typename pivot_map::const_iterator, const pivot_map>(pivots, key);
        }

        typename pivot_map::iterator get_pivot(uint64_t &key) {
            //! return non const iterator
            return get_pivot<typename pivot_map::iterator, pivot_map>(pivots, key);
        }

        //! helper functions to get iterators where message_key >= our key.
        template<class out_type, class in_type>
        static out_type get_element_begin(in_type &messages, const uint64_t &key) {
            return messages.lower_bound(MessageKey::get_default_message_key_min_value(key));
        }

        //! const and non const version of the above function
        typename message_map::iterator get_element_begin(const uint64_t &key) {
            return get_element_begin<typename message_map::iterator, message_map>(messages, key);
        }

        typename message_map::const_iterator get_element_begin(const uint64_t &key) const {
            return get_element_begin<typename message_map::const_iterator, const message_map>(messages, key);
        }

        //! below function returns an iterator to the first element that goes to the child pointer to by it.
        typename message_map::iterator
        get_element_begin(const typename pivot_map::iterator it) {
            return it == pivots.end() ? messages.end() : get_element_begin(it->first);
        }

        //! the real functions begin now.
        //! the below function takes a message and applies it to the calling node.
        void apply(const MessageKey &message_key, const MessageValue &message_value, uint64_t &default_value) {
            //! switch on opcode
            switch (message_value.get_opcode()) {
                case INSERT:
                    //! first erase all elements pointed to by range_start and range_end.
                    //! why? I don't understand this.
                    messages.erase(messages.lower_bound(message_key.range_start()),
                                   messages.upper_bound(message_key.range_end()));
                    messages[message_key] = message_value;
                    break;
                case DELETE:
                    //! erase elements in range
                    messages.erase(messages.lower_bound(message_key.range_start()),
                                   messages.upper_bound(message_key.range_end()));
                    if (!is_leaf()) {
                        messages[message_key] = message_value;
                    }
                    break;
                case UPDATE: {
                    //! get an iterator pointing to the first element which is smaller than what we have provided. (why?)
                    auto it = messages.upper_bound(message_key.range_end());
                    if (it != messages.begin()) {
                        --it;
                    }
                    if (it == messages.end() || it->first.get_key() != message_key.get_key()) {
                        //! check if the iterator points to a leaf.
                        if (is_leaf()) {
                            //! call to self, but as INSERT.
                            //! @refitem operations
                            apply(message_key, MessageValue(INSERT,
                                                            default_value + message_value.get_value()), default_value);
                        }
                            //! else, if this is still an internal nod, just place the element.
                        else {
                            messages[message_key] = message_value;
                        }
                    } else {
                        //! still need to make sense of this.
                        //! check if such  key exists first.
                        assert(it != messages.end() && it->first.get_key() == message_key.get_key());
                        if (it->second.get_opcode() == INSERT) {
                            apply(message_key, MessageValue(INSERT,
                                                            it->second.get_value() + message_value.get_value()),
                                  default_value);
                        } else {
                            messages[message_key] = message_value;
                        }
                    }
                }
                    break;
                default:
                    assert(0);
            }
        }

        //! next big function.
        //! this function handles split of a node,
        //! we will only split when number of messages is less than MIN_FLUSH_SIZE
        pivot_map split(betree &bet) {
            assert(pivots.size() + messages.size() >= bet.max_node_size); //! @xrefitem max_node_size
            //! calculate number of new leaves.
            int num_new_leaves = (pivots.size() + messages.size()) / (10 * bet.max_node_size / 24);
            int things_per_new_leaf = (pivots.size() + messages.size() + num_new_leaves - 1) / num_new_leaves;
            //! declare variable to store our result
            pivot_map result;
            auto pivot_index = pivots.begin();
            auto message_index = messages.begin();
            //! bookkeeping variables
            int things_moved = 0;
            for (int i = 0; i < num_new_leaves; i++) {
                //! if pivot index and element index are both pointing to the end.
                if (unlikely(pivot_index == pivots.end() && message_index == messages.end())) {
                    break;
                }
                //! create a new pointer to node.
                node *node_ptr = new node();
                //! this code is very confusing, here is my understand of it.
                //! if pivot index points to something, then take the first from pivot_index (which is KEY)
                //! else, take the key from the messages
                result[pivot_index != pivots.end() ? pivot_index->first : message_index->first.get_key()] =
                        child_info(node_ptr, node_ptr->messages.size() + node_ptr->pivots.size());
                while (things_moved < (i + 1) * things_per_new_leaf &&
                       (pivot_index != pivots.end() || message_index != messages.end())) {
                    if (pivot_index != pivots.end()) {
                        node_ptr->pivots[pivot_index->first] = pivot_index->second;
                        //! move iterator forward
                        ++pivot_index;
                        //! increment things moved
                        things_moved++;
                        //! get last message in message map.
                        auto elements_end = get_element_begin(pivot_index);
                        while (message_index != elements_end) {
                            node_ptr->messages[message_index->first] = message_index->second;
                            ++message_index;
                            things_moved++;
                        }
                    } else {
                        //! if no such key exists in pivot_map
                        //! means that node is a leaf
                        assert(pivots.size() == 0);
                        node_ptr->messages[message_index->first] = message_index->second;
                        ++message_index;
                        things_moved++;
                    }
                } //! end of the while loop
            }
            //! end of the for loop
            for (auto it = result.begin(); it != result.end(); ++it) {
                it->second.child_size =
                        it->second.child_pointer->messages.size() + it->second.child_pointer->pivots.size();
            }
            assert(pivot_index == pivots.end());
            assert(message_index == messages.end());
            pivots.clear();
            messages.clear();
            return result;
        }

        //! this functions merges two pivot maps (I think)
        node *merge(betree &bet, typename pivot_map::iterator begin, typename pivot_map::iterator end) {
            //! create a new pointer to a node
            node *node_ptr = new node();
            for (auto it = begin; it != end; ++it) {
                //! insert elements into the newly created node
                node_ptr->messages.insert(it->second.child_pointer->messages.begin(),
                                          it->second.child_pointer->messages.end());
                node_ptr->pivots.insert(it->second.child_pointer->pivots.begin(),
                                        it->second.child_pointer->pivots.end());
            }
            //! return new node
            return node_ptr;
        }

        //! not writing the merge small children function.

        //! the function to flush messages. If flushing results in split, return new node*, else empty map.
        pivot_map flush(betree &bet, message_map &msgs) {
            debug(printf("Flushing %p\n", this));
            //! variable to capture result
            pivot_map result;
            //! check if messages is empty
            if (msgs.size() == 0) {
                debug(printf("Done\n"));
                return result;
            }

            //! check if node is leaf.
            if (is_leaf()) {
                for (auto it = msgs.begin(); it != msgs.end(); ++it) {
                    apply(it->first, it->second, bet.default_value); //! @xrefitem default_value
                }
                if (messages.size() + pivots.size() >= bet.max_node_size) {
                    //! split if node is too big
                    result = split(bet);
                }
                return result;
            }

            //! non leaf scenarios

            //! update keys of first child (only if required)
            uint64_t old_min = pivots.begin()->first;
            MessageKey new_min = msgs.begin()->first;
            if (new_min < old_min) {
                pivots[new_min.get_key()] = pivots[old_min];
                pivots.erase(old_min);
            }

            //! our goal is always to flush to the child that is expected to receive the highest number of messages.
            //! with that in mind, if all dirty messages are going to a single child, flush
            //! this is the first element greater than or equal to our element.
            auto first_pivot_index = get_pivot<pivot_map::iterator>(pivots, msgs.begin()->first.get_key());
            //! the index of the last element smaller than or equal to our element.
            auto last_pivot_index = get_pivot<pivot_map::iterator>(pivots, (--msgs.end())->first.get_key());

            if (first_pivot_index == last_pivot_index && first_pivot_index->second.child_pointer->is_dirty()) {
                //! Check if child buffers are empty
                {
                    auto next_pivot_idx = std::next(first_pivot_index);
                    auto elt_start = get_element_begin(first_pivot_index);
                    auto elt_end = get_element_begin(next_pivot_idx);
                    assert(elt_start == elt_end);
                }
                //! flush messages and create new children
                pivot_map new_children = first_pivot_index->second.child_pointer->flush(bet, msgs);
                //! check if new_children map is empty or not
                if (!new_children.empty()) {
                    pivots.erase(first_pivot_index);
                    pivots.insert(new_children.begin(), new_children.end());
                } else {
                    first_pivot_index->second.child_size =
                            first_pivot_index->second.child_pointer->pivots.size() +
                            first_pivot_index->second.child_pointer->messages.size();
                }
            } else {
                //! apply all messages
                for (auto &msg: msgs) {
                    apply(msg.first, msg.second, bet.default_value); //! @refitem default_value
                }

                //! Flush and clean nodes
                while (messages.size() + pivots.size() >= bet.max_node_size) {
                    //! find child with largest set of messages in buffer
                    unsigned int max_size = 0;
                    //! create iterators
                    auto child_pivot = pivots.begin();
                    auto next_pivot = pivots.begin();
                    for (auto it = pivots.begin(); it != pivots.end(); ++it) {
                        auto it2 = std::next(it);
                        auto elt_it = get_element_begin(it);
                        auto elt_it2 = get_element_begin(it2);
                        //! this calculates the "distance" between 2 iterators.
                        //! https://en.cppreference.com/w/cpp/iterator/distance
                        unsigned int dist = std::distance(elt_it, elt_it2);
                        if (dist > max_size) {
                            child_pivot = it;
                            next_pivot = it2;
                            max_size = dist;
                        }
                    } //! end for
                    if (!(max_size > bet.min_flush_size || max_size > bet.min_flush_size / 2)) {
                        //! @xrefitem min_flush_size
                        break; //! why?
                    }
                    //! this is doing something
                    auto elt_child_it = get_element_begin(child_pivot);
                    auto elt_next_it = get_element_begin(next_pivot);
                    message_map child_elts(elt_child_it, elt_next_it);
                    pivot_map new_children = child_pivot->second.child_pointer->flush(bet, child_elts);
                    messages.erase(elt_child_it, elt_next_it);
                    if (!new_children.empty()) {
                        pivots.erase(child_pivot);
                        pivots.insert(new_children.begin(), new_children.end());
                    } else {
                        first_pivot_index->second.child_size =
                                child_pivot->second.child_pointer->pivots.size() +
                                child_pivot->second.child_pointer->messages.size();
                    }
                }

                //! We have too many pivots to efficiently flush stuff down, so split
                if (messages.size() + pivots.size() > bet.max_node_size) {
                    result = split(bet);
                }
            }
            debug(printf("Done flushing\n"));
            return result;
        }

        //! some internal functions

        uint64_t query(const betree &bet, const uint64_t key) const {
            //! check if node is leaf
            if (is_leaf()) {
                //! create a messagekey and get an iterator to it.
                auto it = messages.lower_bound(MessageKey::get_default_message_key_max_value(key));
                //! check if iterator is valid
                if (it != messages.end() && it->first.get_key() == key) {
                    assert(it->second.get_opcode() == INSERT);
                    return it->second.get_value();
                } else {
                    throw std::out_of_range("Key does not exist");
                }
            }

            //! for non leaf nodes
            auto message_iterator = get_element_begin(key);
            uint64_t v = bet.default_value;

            //! check if this node has the key
            if (message_iterator == messages.end() || key < message_iterator->first) {
                //! this node does not have the key, query its children
                v = get_pivot(key)->second.child_pointer->query(bet, key);
            } else if (message_iterator->second.get_opcode() == UPDATE) {
                //! there are some updates for this tree. Search further down
                //! and apply updates wherever needed.
                try {
                    uint64_t t = get_pivot(key)->second.child_pointer->query(bet, key);
                    v = t;
                } catch (std::out_of_range &e) {}
            } else if (message_iterator->second.get_opcode() == DELETE) {
                //! we have a delete. No need to look further down the tree.
                //! should return does not exist if there are no further inserts or updates.
                message_iterator++;
                if (message_iterator == messages.end() || key < message_iterator->first.get_key()) {
                    throw std::out_of_range("Key does not exist");
                }
            } else if (message_iterator->second.get_opcode() == INSERT) {
                v = message_iterator->second.get_value();
                message_iterator++;
            }

            //! apply updates
            while (message_iterator != messages.end() && message_iterator->first.get_key() == key) {
                assert(message_iterator->second.get_opcode() == UPDATE);
                v = v + message_iterator->second.get_value();
                message_iterator++;
            }

            return v;
        }

        //! below functions are realted to getting messages from the tree.
        std::pair<MessageKey, MessageValue> get_next_message_from_children(const MessageKey *message_key) const {
            if (message_key && *message_key < pivots.begin()->first) {
                message_key = NULL;
            }
            auto it = message_key ? get_pivot(message_key->get_key()) : pivots.begin();
            while (it != pivots.end()) {
                try {
                    return it->second.child_pointer->get_next_message(message_key);
                } catch (std::out_of_range &e) {}
                ++it;
            }
            throw std::out_of_range("No more messages in any children");
        }

        std::pair<MessageKey, MessageValue> get_next_message(const MessageKey *message_key) const {
            auto it = message_key ? messages.upper_bound(*message_key) : messages.begin();
            //! checking if node is leaf
            if (is_leaf()) {
                if (it == messages.end()) {
                    throw std::out_of_range("No more messages in sub-tree");
                }
                return std::make_pair(it->first, it->second);
            }

            //! non leaf scenarios
            if (it == messages.end()) {
                return get_next_message_from_children(message_key);
            }

            try {
                auto children = get_next_message_from_children(message_key);
                if (children.first < it->first) {
                    return children;
                } else return std::make_pair(it->first, it->second);
            } catch (std::out_of_range &e) {
                return std::make_pair(it->first, it->second);
            }
        }

        //! this section contains
    }; //! end of node class definition

private:
    //! private members of b epsilon tree.
    uint64_t max_node_size; //! @refitem maximum size of the node.
    uint64_t default_value; //! @refitem a default value for MessageValue
    uint64_t min_flush_size; //! @refitem minimum flush size for a node
    node *root;
    uint64_t next_timestamp = 1;
    //! creating an object of reader writer lock
    ReaderWriterLock rwlock;
public:
    //! constructors
    //! no need for a default constructor, not needed.

    betree(uint64_t max_node_size = DEFAULT_MAX_NODE_SIZE,
           uint64_t min_flush_size = DEFAULT_MIN_FLUSH_SIZE,
           uint64_t default_val = 0) :
            min_flush_size(min_flush_size),
            max_node_size(max_node_size),
            default_value(default_val) {
        root = new node();
    }

    //! APIs
    void upsert(int opcode, uint64_t key, uint64_t value) {
        message_map tmp;
        MessageKey mkey(key, next_timestamp++);
        tmp[mkey] = MessageValue(opcode, value);
        //! acquire lock before doing this
        rwlock.acquire_write_lock();
        pivot_map new_nodes = root->flush(*this, tmp);
        if (new_nodes.size() > 0) {
            root->pivots = new_nodes;
        }
        //! todo: lock guards
        //! release locks before going!
        rwlock.release_write_lock();
    }

    void insert(uint64_t key, uint64_t value) {
        upsert(INSERT, key, value);
    }

    void update(uint64_t k, uint64_t v) {
        upsert(UPDATE, k, v);
    }

    void erase(uint64_t k) {
        upsert(DELETE, k, default_value);
    }

    uint64_t query(uint64_t k) {
        //! get the calling threads id
        uint8_t cpu_id = sched_getcpu();
        //! acquire read lock
        //! debug messages
        uint64_t v;
        try {
            rwlock.acquire_read_lock(cpu_id);
            v = root->query(*this, k);
        } catch (std::exception &e) {
            rwlock.release_read_lock(cpu_id);
        }
        //! release write lock
        rwlock.release_read_lock(cpu_id);
        return v;
    }

    //! some helper functions
    void dump_messages(void) {
        std::pair<MessageKey, MessageValue> current;

        std::cout << "############### BEGIN DUMP ##############" << std::endl;
	int count = 0;

        try {
            current = root->get_next_message(NULL);
            do {
		count++;
                std::cout << current.first.get_key() << " "
                          << current.first.get_timestamp() << " "
                          << current.second.get_opcode() << " "
                          << current.second.get_value() << std::endl;
                current = root->get_next_message(&current.first);
            } while (1);
        } catch (std::out_of_range e) {
            // todo: release lock
        }
	printf("\n\nTotal count = %d", count);
    }

    //! Our own definition of iterator
    class iterator {
    public:
        const betree &bet;
        std::pair<MessageKey, MessageValue> position;
        bool is_valid;
        bool pos_is_valid;
        uint64_t first;
        uint64_t second;

        //! constructors
        iterator(const betree &bet)
                : bet(bet),
                  position(),
                  is_valid(false),
                  pos_is_valid(false),
                  first(),
                  second() {}

        iterator(const betree &bet, const MessageKey*mkey)
                : bet(bet),
                  position(),
                  is_valid(false),
                  pos_is_valid(false),
                  first(),
                  second() {
            try {
                position = bet.root->get_next_message(mkey);
                pos_is_valid = true;
                setup_next_element();
            } catch (std::out_of_range &e) {}
        }


        void apply(const MessageKey &msgkey, const MessageValue &msg) {
            switch (msg.get_opcode()) {
                case INSERT:
                    first = msgkey.get_key();
                    second = msg.get_value();
                    is_valid = true;
                    break;
                case UPDATE:
                    first = msgkey.get_key();
                    if (is_valid == false)
                        second = bet.default_value;
                    second = second + msg.get_value();
                    is_valid = true;
                    break;
                case DELETE:
                    is_valid = false;
                    break;
                default:
                    abort();
                    break;
            }
        }

        void setup_next_element(void) {
            is_valid = false;
            while (pos_is_valid && (!is_valid || position.first.get_key() == first)) {
                apply(position.first, position.second);
                try {
                    position = bet.root->get_next_message(&position.first);
                } catch (std::exception &e) {
                    pos_is_valid = false;
                }
            }
        }

        bool operator==(const iterator &other) {
            return &bet == &other.bet &&
                   is_valid == other.is_valid &&
                   pos_is_valid == other.pos_is_valid &&
                   (!pos_is_valid || position == other.position) &&
                   (!is_valid || (first == other.first && second == other.second));
        }

        bool operator!=(const iterator &other) {
            return !operator==(other);
        }

        iterator &operator++(void) {
            setup_next_element();
            return *this;
        }
    };

    //! iterator functions

    iterator begin(void) const {
        return iterator(*this, NULL);
    }

    iterator lower_bound(uint64_t key) const {
        MessageKey tmp = MessageKey::get_default_message_key_min_value(key);
        return iterator(*this, &tmp);
    }

    iterator upper_bound(uint64_t key) const {
        MessageKey tmp = MessageKey::get_default_message_key_max_value(key);
        return iterator(*this, &tmp);
    }

    iterator end(void) const {
        return iterator(*this);
    }
};

#endif CONCURRENT_BETREE_BETREE_HPP
