# Subscriptions

## Setup and Change Flow

```
                       CLIENT                       SERVER
-----------------------------------------------------------------------------------------
*SETUP FLOW*

(observer) subscribe ─► Node        ──NodeRequest──► Node  -->  subscribe ──► Reactor
                      create sub ID                   |                         │
                      setup local sub                 ▼                         ▼
                           │              register remote sub            register predicate
                           ▼
                       Reactor

-----------------------------------------------------------------------------------------
*CHANGE FLOW*
                                                           transaction commit
                                                                   │
                                                                   ▼
Client Code ◄──notify────    Node  ◄──NodeUpdate──    Node     notify   ─►  Reactor
     ▲                         |                         ^                       │
     │                         ▼ notify                  │                       │
     │                      Reactor                      │                       ▼
     │                        │                      notify subscriber  ◄─  match predicate
     │                        ▼
   notify subscriber  ◄─  match predicate
```

- **Setup**:

  - Client node creates subscription ID
  - Client sets up local reactor subscription
  - Client sends subscription request to server node
  - Server registers subscription with its reactor as a proxy
  - Server returns initial state to client

- **Changes**:
  - Both reactors independently detect local changes
  - Reactors match changes against predicates
  - Server packages matching changes into updates
  - Server sends updates to client
  - Client validates and applies updates to local entities
  - Client reactor notifies user code via callback
