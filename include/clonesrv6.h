//
// Created by linhdh on 25/09/2025.
//

#ifndef ZMQ_CLONE_SERVERS_CLONESRV6_H
#define ZMQ_CLONE_SERVERS_CLONESRV6_H

#include "bstar.hpp"
#include "kvmsg.hpp"

using namespace zmqpp;
using namespace std;

typedef struct {
    context_t *ctx;  // Our context
    unordered_map<string, KVMsg*>* kvmap; // Key-value store
    bstar_t *bstar;     // Binary Star reactor core
    int64_t sequence;  // How many updates we're at
    int peer;           // Main port of our peer
    int port;          // Main port we're working on
    socket_t* publisher;   // Publish updates and hugz
    socket_t* collector;   // Collect updates from clients
    socket_t* subscriber;  // Receive updates from peer
    list<KVMsg*> pending;  // Pending updates
    bool primary;       // true if we're primary
    bool active;        // true if we're active
    bool passive;       // true if we're passive
} clonesrv_t;

static void s_snapshot(loop *loop, socket_t *socket, void *arg);
static bool s_collector(clonesrv_t *self);
static bool s_flush_ttl(clonesrv_t *self);
static bool s_send_hugz(clonesrv_t *self);
static bool s_subscriber(clonesrv_t *self);
static void s_new_active(loop *loop, socket_t *socket, void *arg);
static void s_new_passive(loop *loop, socket_t *socket, void *arg);

#endif //ZMQ_CLONE_SERVERS_CLONESRV6_H