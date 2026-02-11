# spark-connect-remote Architecture

## Overview

This library provides KBase authentication for Apache Spark Connect clients. It automatically resolves the username from the KBase token to build the correct Spark Connect URL for multi-tenant environments, then passes the token via the URL for server-side validation.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Client Application                             │
├─────────────────────────────────────────────────────────────────────────────┤
│  spark = create_spark_session()                                             │
│      (Defaults to spark.berdl.kbase.us:443)                               │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                             K8s Ingress (Nginx)                             │
│  https://spark.berdl.kbase.us:443                                         │
│  Terminates TLS, routes to Service                                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Spark Connect Proxy (Service)                       │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ Async gRPC Interceptor                                              │    │
│  │ 1. Extracts `x-kbase-token` metadata                                │    │
│  │ 2. Validates token via KBase Auth2 (Async, Non-Blocking)            │    │
│  │ 3. Resolves Username -> Backend Pod (spark-connect-{username})      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                   │                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ Channel Pool (LRU)                                                  │    │
│  │ Manages connection to backend pods                                  │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       User's Spark Connect Server Pod                       │
│  (spark-connect-{username})                                                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Spark Connect Proxy
The central component that creates a unified entry point (`spark.berdl.kbase.us`) for all users. It handles authentication and routing, ensuring users are securely connected to their personal Spark driver pods without exposing individual pods publicly.

**Key Features:**
- **Async Authentication:** Validates KBase tokens without blocking the event loop.
- **Dynamic Routing:** Routes requests based on the authenticated username.
- **Connection Pooling:** Manages LRU channel pool to backend pods.
- **Health Checks:** Exposes gRPC health check and stats endpoint.

### 2. Session Helpers (`session.py`)
Client-side library to easily create authenticated sessions.
- **Default Host:** `spark.berdl.kbase.us`
- **Default Port:** `443` (Ingress)
- **Default Auth:** `https://kbase.us/services/auth/` (Prod)
- **Zero Config:** `spark = create_spark_session()` just works.

### 3. KBase Auth Integration
- **Client:** `KBaseAuthClient` (Async via `httpx`)
- **Server:** Async interceptor in Proxy matches token -> user.

## Security
1.  **TLS Termination:** Handled by Ingress.
2.  **Token Auth:** Mandatory KBase token for every request.
3.  **User Isolation:** Users can ONLY access their own pod (routed by username).
4.  **No Direct Pod Access:** User pods are cluster-internal; only Proxy is exposed.
