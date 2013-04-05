// Copyright 2013 Harry Q. Bovik (hbovik)
#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>
#include <map>
#include <vector>
#include <string>

#include "server/messages.h"
#include "server/master.h"
#include "tools/work_queue.h"
#include "tools/cycle_timer.h"
#include <iostream>

#define CACHE_TICKETS 5
#define IDLE_ROUNDS 2

/* Each incoming request is stores the request type,
 * the client, and an array of size 5  in a Request_info
 * struct */
typedef struct Request_Info {
    Request_msg* req;
    Client_handle client;
    int counts[5];
} reqInfo;

/* Each worker that is initialized stores its tag ID,
 * the number of idle CPU threads, the number of idle disk
 * threads, and the number of ticks it has been idle for. */
typedef struct Worker_Info {
    int tag;
    int num_idle_cpu;
    int num_idle_disk;
    int idle_round;
} workerInfo;

/* Store a response and the number of ticks it has been
   in the cache, for eviction purposes. */
typedef struct cache_info {
    Response_msg res;
    int tickets;
}cacheInfo;

static struct Master_state {

  // The mstate struct collects all the master node state into one
  // place.  You do not need to preserve any of the fields below, they
  // exist only to implement the basic functionality of the starter
  // code.

  bool server_ready;
  int max_num_workers; //limited to 4 on AWS
  int num_pending_client_requests;
  int num_worker_nodes; //start with 2

  unsigned int first_call;
  int add_round;

  std::vector<Worker_handle>cpu_workers_queue; //Queue tracking the available CPU workers
  std::vector<Worker_handle>disk_workers_queue; //Queue tracking the available disk workers
  std::vector<Request_msg>cpu_waiting_queue; //Queue tracking the waiting CPU requests
  std::vector<Request_msg>disk_waiting_queue; //Queue trakcing the waiting disk requests
  std:: map<int, reqInfo*> requestsMap; //Initialize the request dictionary
  std:: map<Worker_handle, workerInfo*> workersMap; //Initialize the workers dictionary
  std::map<std::string, cacheInfo*> cache; // create the cache
  Worker_handle my_worker;
  Client_handle waiting_client;

} mstate;

//Pop the first available worker in the appropriate queue and return
Worker_handle get_worker( std::vector<Worker_handle>& queue) {
    Worker_handle thisWorker = queue.front();
    queue.erase(queue.begin());
    return thisWorker;
}

//Pop the first available request in the appropriate queue and return
Request_msg get_request( std::vector<Request_msg>& queue ) {
    Request_msg thisRequest = queue.front();
    queue.erase(queue.begin());
    return thisRequest;
}

//Give the new worker a tag and identifiable name
void request_for_worker() {
     int tag = random();
     Request_msg req(tag);
     char name[20];
     sprintf(name, "my worker %d", mstate.num_worker_nodes);
     req.set_arg("name", name);
     request_new_worker_node(req);
}



void master_node_init(int max_workers, int& tick_period) {

  // set up tick handler to fire every 1 seconds. (feel free to
  // configure as you please)
  tick_period = 1;

  mstate.max_num_workers = max_workers;

  // initially, we set up two workers instead of one. We do this because
  // we receive a request for compareprimes in the beginning, and this request
  // will be broken up into 4 subrequests. Since each worker only spawns
  // 3 threads, we will need at least two workers to process such a request.
  mstate.num_worker_nodes = 2;
  mstate.num_pending_client_requests = 0;

  mstate.first_call = 2;
  mstate.add_round = 0;

  // don't mark the server as ready until the server is ready to go.
  // This is actually when the first worker is up and running, not
  // when 'master_node_init' returns
  mstate.server_ready = false;
  request_for_worker();
  request_for_worker();
}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {

  // we put the new worker into the map to keep track of the status of each worker node
  workerInfo* worker_info = new workerInfo();
  worker_info->tag = tag;
  worker_info->num_idle_cpu = 2;
  worker_info->num_idle_disk = 1;
  worker_info->idle_round = 0;
  mstate.workersMap[worker_handle] = worker_info;

  // 'tag' allows you to identify which worker request this response
  // corresponds to.  Since the starter code only sends off one new
  // worker request, we don't use it here.

  mstate.cpu_workers_queue.push_back( worker_handle );
  mstate.cpu_workers_queue.push_back( worker_handle );
  mstate.disk_workers_queue.push_back( worker_handle );

  // Check if there are waiting CPU requests. If so, grab the first
  // worker from the workers queue and send the request to the worker.
  // Reset the worker's idle round to 0 since it is now busy, and the
  // number of idle CPU threads will decrement.
  if(mstate.cpu_waiting_queue.size() != 0) {
    Worker_handle thisWorker = get_worker(mstate.cpu_workers_queue);
    Request_msg req = get_request(mstate.cpu_waiting_queue);
    send_request_to_worker(thisWorker, req);
    mstate.num_pending_client_requests++;
    mstate.workersMap[thisWorker]->idle_round = 0;
    mstate.workersMap[thisWorker]->num_idle_cpu--;
  }

  // Check if there are waiting disk requests. If so, grab the first
  // worker from the workers disk queue and send the request to the worker.
  // Reset the worker's idle round to 0 since it is now busy, and the
  // number of idle disk threads will decrement.
  if(mstate.disk_waiting_queue.size() != 0) {
    Worker_handle thisWorker = get_worker(mstate.disk_workers_queue);
    Request_msg req = get_request(mstate.disk_waiting_queue);
    send_request_to_worker(thisWorker, req);
    mstate.workersMap[thisWorker]->idle_round = 0;
    mstate.workersMap[thisWorker]->num_idle_disk --;
  }

  // Now that a worker is booted, let the system know the server is
  // ready to begin handling client requests.  The test harness will
  // now start its timers and start hitting your server with requests.
  if (mstate.server_ready == false) {
    server_init_complete();
    mstate.server_ready = true;
  }
}

// Given a request, get its command type and use it as a key
// for the cache.
std::string build_cache_key( Request_msg& req){
    std:: string key = req.get_arg("cmd");
    if( req.get_arg("cmd").compare("countprimes") == 0 ) {
        key += ";" + req.get_arg("n");
    }
    else if( req.get_arg("cmd").compare("mostviewed") == 0 ) {
        key += ";" + req.get_arg("start") + ";" + req.get_arg("end");
    }
    else if( req.get_arg("cmd").compare("418wisdom") == 0 ) {
        key += ";" + req.get_arg("x");
    } else {
        key += ";" + req.get_arg("n1") +";" + req.get_arg("n2") + ";" + req.get_arg("n3") +";" + req.get_arg("n4");
    }
    return key;
}


void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) {
  std::map<int,reqInfo*>::iterator it = mstate.requestsMap.find(resp.get_tag());
  bool isDiskRequestDone = false;

  // do not send response until all done
  // If the request is for compareprimes, then compile the four subrequests
  // once they are finished for the answer to the original request.
  if ((*((it->second)->req)).get_arg("cmd").compare("compareprimes") == 0) {
      std::string result = resp.get_response();
      unsigned pos = result.find(":");
      int index = atoi(result.substr(0,1).c_str());
      int n = atoi(result.substr(pos+1).c_str());
      it->second->counts[index] = n;
      it->second->counts[4] ++;
      // all sub requests finished
      if( it->second->counts[4] == 4) {
          Response_msg real_resp;
        if (it->second->counts[1]-it->second->counts[0] > it->second->counts[3]-it->second->counts[2])
          real_resp.set_response("There are more primes in first range.");
        else
          real_resp.set_response("There are more primes in second range.");
        send_client_response((it->second)->client, real_resp);
        delete( (it->second)->req );
        delete( it->second );
        mstate.requestsMap.erase(it);
      }
  }
  // If it is not a "compareprimes" request, then the response is sent
  // back to the client and inserted into the cache.
  else {
    // send the message back to the client
    send_client_response((it->second)->client, resp);
    // put the reponse into a cache
    cacheInfo* cache_ele = new cacheInfo();
    cache_ele->tickets = CACHE_TICKETS;
    cache_ele->res = resp;
    std::string key = build_cache_key(*((it->second)->req));
    mstate.cache[key] = cache_ele;

    if( (*((it->second)->req)).get_arg("cmd").compare("mostviewed") == 0) {
            isDiskRequestDone = true;
      }
    delete( (it->second)->req );
    delete( it->second );
    mstate.requestsMap.erase(it);
  }

  // Once the master has handled the request, it will check the disk
  // waiting queue to see if there are any requests currently waiting
  // in the queue. If not, then the worker that just finished processing
  // the request will be pushed back into the queue and set to idle. If
  // there are workers in the queue, then the next waiting request is
  // assigned to the worker and the idle state of the worker is set back to 0.
  if( isDiskRequestDone ) {
    if(mstate.disk_waiting_queue.size() == 0) {
        mstate.disk_workers_queue.push_back( worker_handle);
        mstate.workersMap[worker_handle]->num_idle_disk++;
    }else {
        Request_msg thisRequest = get_request(mstate.disk_waiting_queue);
        send_request_to_worker( worker_handle, thisRequest);
        mstate.workersMap[worker_handle]->idle_round = 0;
    }
    return;
  }

  mstate.num_pending_client_requests--;
  // Similarly, checks the number of waiting CPU requests and either pushes
  // the worker into the queue if there are none or assigns the next waiting
  // request to the worker.
  if( mstate.cpu_waiting_queue.size() == 0) {
    mstate.cpu_workers_queue.push_back( worker_handle );
    mstate.workersMap[worker_handle]->num_idle_cpu++;
  }else {
    mstate.num_pending_client_requests++;
    Request_msg thisRequest = get_request(mstate.cpu_waiting_queue);
    send_request_to_worker( worker_handle, thisRequest);
    mstate.workersMap[worker_handle]->idle_round = 0;
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

  // First we check if the request has already been computed and stored
  // in the cache. We can just return the response instead of sending
  // the request to a worker.
  std:: string key = build_cache_key( worker_req);
  if ( mstate.cache.find(key) != mstate.cache.end() ) {
    send_client_response(client_handle, mstate.cache.find(key)->second->res);
    return;
  }

  // store the waiting client into the map
  reqInfo* thisInfo = new reqInfo();
  thisInfo->req = new Request_msg(worker_req);
  thisInfo->client = client_handle;
  mstate.requestsMap[tag] = thisInfo;

  // This value determines how many subrequests to break the
  // request up into. The value will remain 1 for all requests
  // except for compareprimes, which will be broken up in to
  // 4 subrequests.
  int isCompare = 1;

  if(worker_req.get_arg("cmd").compare("mostviewed") == 0) {
    mstate.disk_waiting_queue.push_back(worker_req);
  }

  // If the request is for compareprimes, then we must break it up
  // into 4 subrequests. They will each run compareprimes on smaller
  // ranges, and we push each subrequest back into the requests queue.
  else if (worker_req.get_arg("cmd").compare("compareprimes") == 0) {
      isCompare = 4;
      int params[4];
      params[0] = atoi(worker_req.get_arg("n1").c_str());
      params[1] = atoi(worker_req.get_arg("n2").c_str());
      params[2] = atoi(worker_req.get_arg("n3").c_str());
      params[3] = atoi(worker_req.get_arg("n4").c_str());
      // initialize the sub-requests that have been completed
      mstate.requestsMap[tag]->counts[4] = 0;
      for(int i = 0; i < 4; i++) {
          std::ostringstream convert1;
          std::ostringstream convert2;
          convert1 << params[i];
          convert2 << i;
          Request_msg dummy_req(tag);
          dummy_req.set_arg("cmd", "compareprimes");
          dummy_req.set_arg("n", convert1.str());
          dummy_req.set_arg("index", convert2.str());
          mstate.cpu_waiting_queue.push_back(dummy_req);
      }
      // special case
  }
  else {
    mstate.cpu_waiting_queue.push_back(worker_req);
  }

  // Make sure that all four subrequests are popped from the queueu
  // and computed before checking for further requests. This ensure that
  // we find the result of the original compareprimes request before
  // assigning workers to other requests.
  while(isCompare && mstate.cpu_waiting_queue.size() != 0 && mstate.cpu_workers_queue.size() != 0) {
    Worker_handle thisWorker = get_worker(mstate.cpu_workers_queue);
    Request_msg req = get_request(mstate.cpu_waiting_queue);
    send_request_to_worker(thisWorker, req);
    mstate.num_pending_client_requests++;
    mstate.workersMap[thisWorker]->idle_round = 0;
    mstate.workersMap[thisWorker]->num_idle_cpu --;
    isCompare--;
  }

  // If there are remaining workers and requests for disk accesses then send the
  // requests to the workers.
  if(mstate.disk_waiting_queue.size() != 0 && mstate.disk_workers_queue.size() != 0) {
    Worker_handle thisWorker = get_worker(mstate.disk_workers_queue);
    Request_msg req = get_request(mstate.disk_waiting_queue);
    send_request_to_worker(thisWorker, req);
    mstate.workersMap[thisWorker]->idle_round = 0;
    mstate.workersMap[thisWorker]->num_idle_disk --;
  }
}


void kill_worker(Worker_handle worker_handle) {
    // search both cpu_workers_queue and disk_workers_queue
    kill_worker_node(worker_handle);
    mstate.num_worker_nodes --;
    int i;
    for(i = mstate.cpu_workers_queue.size() - 1; i >= 0; i--) {
        if(mstate.cpu_workers_queue[i] == worker_handle ) {
            mstate.cpu_workers_queue.erase(mstate.cpu_workers_queue.begin() + i);
        }
    }
    for(i = mstate.disk_workers_queue.size() - 1; i >= 0; i--) {
        if(mstate.disk_workers_queue[i] == worker_handle ) {
            mstate.disk_workers_queue.erase(mstate.disk_workers_queue.begin() + i);
        }
    }
   std::map<Worker_handle, workerInfo*>::iterator it = mstate.workersMap.find(worker_handle);
   delete(it->second);
   mstate.workersMap.erase(it);
}

// Check each worker in the workers dictionary. If worker i has two idle CPU threads
// and one idle disk thread, then increment the number of idle rounds. If that worker
// has reached the maximum number of idle rounds and it is not the only active worker,
// kill the worker.
void check_worker_status() {
    std::map<Worker_handle, workerInfo*>::iterator it;
    for(it = mstate.workersMap.begin(); it != mstate.workersMap.end(); it ++) {
        fprintf(stdout, "| tag: %d, cpu: %d, disk: %d, round: %d", (it->second)->tag, (it->second)->num_idle_cpu,
                (it->second)->num_idle_disk, (it->second)->idle_round);
        if( (it->second)->num_idle_cpu == 2 && (it->second)->num_idle_disk == 1) {
           (it->second)->idle_round ++;
            if( (it->second)->idle_round == IDLE_ROUNDS && mstate.workersMap.size() != mstate.first_call) {
	      kill_worker(it->first);
	    }
        }
    }
    printf("\n");
}

// Compare the number of available worker nodes, and if there are less
// than the maximum number of possible workers then request a new worker.
// Then, check each worker's status to kill any idle workers.
void handle_tick() {
  mstate.add_round++;
    if( mstate.add_round == 3 || mstate.cpu_waiting_queue.size() >= 1){
        if ( mstate.num_worker_nodes < mstate.max_num_workers ) {
            request_for_worker();
            mstate.first_call = 1;
            mstate.num_worker_nodes ++;
        }
        mstate.add_round = 0;
    }
    check_worker_status();
    fprintf(stdout, "NUM OF WAITING REQUESTS: %lu\n", mstate.cpu_waiting_queue.size());
    fprintf(stdout, "NUM OF PENDING REQUESTS: %d\n", mstate.num_pending_client_requests);
    fprintf(stdout, "NUM OF WORKERS: %d \n", mstate.num_worker_nodes);
}

