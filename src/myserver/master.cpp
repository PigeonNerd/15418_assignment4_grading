// Copyright 2013 Harry Q. Bovik (hbovik)
#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>
#include <map>
#include <vector>

#include "server/messages.h"
#include "server/master.h"
#include "tools/work_queue.h"
#include <iostream>

typedef struct request_Info {
    Request_msg* req;
    Client_handle client;
} reqInfo;

typedef struct worker_Info {
    int num_idle_cpu;
    int num_idle_disk;
} workerInfo;

static struct Master_state {

  // The mstate struct collects all the master node state into one
  // place.  You do not need to preserve any of the fields below, they
  // exist only to implement the basic functionality of the starter
  // code.

  bool server_ready;
  int max_num_workers;
  int num_pending_client_requests;
  int num_worker_nodes;

  std::vector<Worker_handle>cpu_workers_queue;
  std::vector<Worker_handle>disk_workers_queue;
  std::vector<Request_msg>cpu_waiting_queue;
  std::vector<Request_msg>disk_waiting_queue;
  std:: map<int, reqInfo*> requestsMap;
  std:: map<Worker_handle, workerInfo*> workersMap;

  Worker_handle my_worker;
  Client_handle waiting_client;

} mstate;

void master_node_init(int max_workers, int& tick_period) {

  // set up tick handler to fire every 5 seconds. (feel free to
  // configure as you please)
  tick_period = 5;
  //printf("The maximum number of workers %d\n", max_workers);
  // HOW TO SET THIS NUMBER ?
  mstate.max_num_workers = max_workers;
  // initially, we only setup one workers 
  mstate.num_worker_nodes = 0;
  mstate.num_pending_client_requests = 0;
  // used for debug
  
  // don't mark the server as ready until the server is ready to go.
  // This is actually when the first worker is up and running, not
  // when 'master_node_init' returnes
  mstate.server_ready = false;
  int tag = random();
  Request_msg req(tag);
  char name[20];
  sprintf(name, "my worker %d", mstate.num_worker_nodes);
  req.set_arg("name", name);
  request_new_worker_node(req);
}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {

  mstate.num_worker_nodes ++;
  // we put new worker into the map to keep track of the status of each worker node
  workerInfo* worker_info = new workerInfo();
  worker_info->num_idle_cpu = 2;
  worker_info->num_idle_disk = 1;
  mstate.workersMap[worker_handle] = worker_info;

  // 'tag' allows you to identify which worker request this response
  // corresponds to.  Since the starter code only sends off one new
  // worker request, we don't use it here.

  mstate.cpu_workers_queue.push_back( worker_handle );
  mstate.cpu_workers_queue.push_back( worker_handle );
  mstate.disk_workers_queue.push_back( worker_handle );
  
  // Now that a worker is booted, let the system know the server is
  // ready to begin handling client requests.  The test harness will
  // now start its timers and start hitting your server with requests.
  if (mstate.server_ready == false) {
    server_init_complete();
    mstate.server_ready = true;
  }
}

void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) {
  bool isDiskRequestDone = false;
  std::map<int,reqInfo*>::iterator it = mstate.requestsMap.find(resp.get_tag());
  // send the message back to the client
  send_client_response((it->second)->client, resp);
  //cout<<(*((it->second)->req)).get_arg("cmd")<<"###\n";
  if( (*((it->second)->req)).get_arg("cmd").compare("mostviewed") == 0) {
        isDiskRequestDone = true;
  }
  delete( (it->second)->req );
  delete( it->second );
  mstate.requestsMap.erase(it);
  if( isDiskRequestDone ) {
    if(mstate.disk_waiting_queue.size() == 0) {
        mstate.disk_workers_queue.push_back( worker_handle);
        mstate.workersMap[worker_handle]->num_idle_disk++;
    }else {
        Request_msg thisRequest = mstate.disk_waiting_queue.front();
        send_request_to_worker( worker_handle, thisRequest);
        mstate.disk_waiting_queue.erase(mstate.disk_waiting_queue.begin());
    }
    return;
  }

  mstate.num_pending_client_requests--;
  // here means we do not have more work right now
  if( mstate.cpu_waiting_queue.size() == 0) {
    mstate.cpu_workers_queue.push_back( worker_handle );
    mstate.workersMap[worker_handle]->num_idle_cpu++;
  }else {
    mstate.num_pending_client_requests++;
    Request_msg thisRequest = mstate.cpu_waiting_queue.front();
    send_request_to_worker( worker_handle, thisRequest);
    mstate.cpu_waiting_queue.erase(mstate.cpu_waiting_queue.begin());
  }
}

void handle_client_request(Client_handle client_handle, const Request_msg& client_req) {

  // You can assume that traces end with this special message.  It
  // exists because it might be useful for debugging to dump
  // information about the entire run here: statistics, etc.
  if (client_req.get_arg("cmd") == "lastrequest") {
    Response_msg resp(0);
    resp.set_response("ack");
    send_client_response(client_handle, resp);
    return;
  }

  int tag = random();
  Request_msg worker_req(tag, client_req);
  // store the waiting client into the map
  reqInfo* thisInfo = new reqInfo();
  thisInfo->req = new Request_msg(worker_req);

  thisInfo->client = client_handle;
  //mstate.requestsMap[tag] = client_handle;
  mstate.requestsMap[tag] = thisInfo;
  
  // we have disk intensive work
  if(worker_req.get_arg("cmd").compare("mostviewed") == 0) {
      // we have worker for it
      if( mstate.disk_workers_queue.size() != 0) {
        Worker_handle thisWorker = mstate.disk_workers_queue.front();
        send_request_to_worker(thisWorker, worker_req);
        mstate.workersMap[thisWorker]->num_idle_disk--; 
        mstate.disk_workers_queue.erase(mstate.disk_workers_queue.begin());
      }else {
        mstate.disk_waiting_queue.push_back(worker_req);
      }
    return;
  }
  // we run out of workers for cpu intensive work
  if( mstate.num_pending_client_requests == mstate.num_worker_nodes * 2) {
    mstate.cpu_waiting_queue.push_back(worker_req);
    return;
  }
  mstate.num_pending_client_requests++;
  Worker_handle thisWorker = mstate.cpu_workers_queue.front();
  send_request_to_worker(thisWorker, worker_req);
  mstate.workersMap[thisWorker]->num_idle_cpu--; 
  mstate.cpu_workers_queue.erase(mstate.cpu_workers_queue.begin());
}

void print_worker_status() {
    std::map<Worker_handle, workerInfo*>::iterator it; 
    for(it = mstate.workersMap.begin(); it != mstate.workersMap.end(); it ++) {
       printf("| cpu: %d, disk: %d", (it->second)->num_idle_cpu, (it->second)->num_idle_disk);
    }
    printf("\n");
}

void handle_tick() {

  print_worker_status(); 
  if( mstate.num_worker_nodes < mstate.max_num_workers && mstate.cpu_waiting_queue.size() >= 3){
      int tag = random();
      Request_msg req(tag);
      char name[20];
      sprintf(name, "my worker %d", mstate.num_worker_nodes);
      req.set_arg("name", name);
      request_new_worker_node(req);
  }
  // TODO: you may wish to take action here.  This method is called at
  // fixed time intervals, according to how you set 'tick_period' in
  // 'master_node_init'.
  // p
  printf("NUM OF WAITING REQUESTS: %lu\n", mstate.cpu_waiting_queue.size());
  printf("NUM OF PENDING REQUESTS: %d\n", mstate.num_pending_client_requests);
}

