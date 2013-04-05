// Copyright 2013 15418 Course Staff.

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sstream>
#include <glog/logging.h>
#include "server/messages.h"
#include "server/worker.h"
#include "tools/work_queue.h"

//#include <cstring>
//#include <iostream>

// Initialize the CPU and disk queues
struct Worker_state {
    WorkQueue<Request_msg> cpu_work_queue;
    WorkQueue<Request_msg> disk_work_queue;
} wstate;


// Generate a valid 'countprimes' request dictionary from integer 'n'
static void create_computeprimes_req(Request_msg& req, int n) {
  std::ostringstream oss;
  oss << n;
  req.set_arg("cmd", "countprimes");
  req.set_arg("n", oss.str());
}

// Implements logic required by primerange command for the request
// 'req' using multiple calls to execute_work.  This function fills in
// the appropriate response.

// IS NOT USED??? SHOULD BE DELETED?????

static void execute_compareprimes(const Request_msg& req, Response_msg& resp) {

    int params[4];
    int counts[4];

    // grab the four arguments defining the two ranges
    params[0] = atoi(req.get_arg("n1").c_str());
    params[1] = atoi(req.get_arg("n2").c_str());
    params[2] = atoi(req.get_arg("n3").c_str());
    params[3] = atoi(req.get_arg("n4").c_str());

    for (int i=0; i<4; i++) {
      Request_msg dummy_req(0);
      Response_msg dummy_resp(0);
      create_computeprimes_req(dummy_req, params[i]);
      execute_work(dummy_req, dummy_resp);
      counts[i] = atoi(dummy_resp.get_response().c_str());
    }

    if (counts[1]-counts[0] > counts[3]-counts[2])
      resp.set_response("There are more primes in first range.");
    else
      resp.set_response("There are more primes in second range.");
}

static void execute_compareprimes2(const Request_msg& req, Response_msg& resp) {
    int n;
    std::string result;
    // grab the four arguments defining the two ranges
    n = atoi(req.get_arg("n").c_str());
    Request_msg dummy_req(0);
    Response_msg dummy_resp(0);
    create_computeprimes_req(dummy_req, n);
    execute_work(dummy_req, dummy_resp);
    result =req.get_arg("index")+":"+dummy_resp.get_response();
    resp.set_response(result);
}

// Pop work from the CPU queue, execute, and
// send the response back to the master
void* executeWork_cpu(void* arg) {
    while(1) {
      Request_msg req = wstate.cpu_work_queue.get_work();
      Response_msg resp(req.get_tag());
      if (req.get_arg("cmd").compare("compareprimes") == 0) {
        execute_compareprimes2(req, resp);
      } else {
        execute_work(req, resp);
      }
      worker_send_response(resp);
    }
    return NULL;
}

// Pop work from the disk queue, execute, and
// send the response back to the master
void* executeWork_disk(void* arg) {
    while(1) {
      Request_msg req = wstate.disk_work_queue.get_work();
      Response_msg resp(req.get_tag());
      // There is only one type here
      execute_work(req, resp);
      worker_send_response(resp);
    }
    return NULL;
}

// Initialize three threads per worker. Two of the threads
// will execute CPU work, and one will execute disk work
void worker_node_init(const Request_msg& params) {

  fprintf( stdout, "**** Initializing worker: %s ****\n", params.get_arg("name").c_str());
  pthread_t thread_1;
  pthread_t thread_2;
  pthread_t thread_3;
  pthread_create(&thread_1, NULL, executeWork_cpu, NULL);
  pthread_create(&thread_2, NULL, executeWork_cpu, NULL);
  pthread_create(&thread_3, NULL, executeWork_disk, NULL);
}

// Execute work normally for every request type except for
// compareprimes, which calls a special function execute_compareprimes.
// This function handles the case of compareprimes requests broken
// up into four subrequests.
void* executeWork(void* arg) {
  Request_msg* req = (Request_msg*) arg;
  Response_msg resp((*req).get_tag());
  if ((*req).get_arg("cmd").compare("compareprimes") == 0) {
    // The primerange command needs to be special cased since it is
    // built on 4 calls to execute_work.  All other requests
    // from the client are one-to-one with calls to
    // execute_work.
    execute_compareprimes(*req, resp);
  } else {
    // actually perform the work.  The response string is filled in by
    // 'execute_work'
    execute_work(*req, resp);
  }
  // send a response string to the master
  free(arg);
  worker_send_response(resp);
  return NULL;
}

void worker_handle_request(const Request_msg& req) {
   if(req.get_arg("cmd").compare("mostviewed") == 0) {
        wstate.disk_work_queue.put_work(req);
        return;
   }
    wstate.cpu_work_queue.put_work(req);
}
