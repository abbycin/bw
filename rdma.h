/*********************************************************
  ┊ ┊ ┊ ┊ File Name:rdma.h
  ┊ ┊ ┊ ┊ Author: Abby Cin
  ┊ ┊ ┊ ┊ Mail: abbytsing@gmail.com
  ┊ ┊ ┊ ┊ Created Time: Sun 29 Aug 2021 12:00:50 PM CST
**********************************************************/

#ifndef NM_RDMA_H_
#define NM_RDMA_H_

#include <stdbool.h>
#include <infiniband/verbs.h>
#include <sys/uio.h>

#ifndef INET6_ADDRSTRLEN
#define INET6_ADDRSTRLEN 40
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
	uint8_t ib_dev;
	uint8_t ib_port;
	uint16_t port;
	uint32_t cap_send;
	uint32_t cap_recv;
	size_t max_wr;
	const char *ip;
} cfg_t;

typedef struct {
	char ip[INET6_ADDRSTRLEN];
	uint16_t port;
} addr_t;

typedef struct conn_t conn_t;
typedef struct accp_t accp_t;
typedef struct ctx_t ctx_t;

typedef struct {
	int (*init)(ctx_t **ctx);
	int (*exit)(ctx_t **ctx);
	int (*regmr)(ctx_t *ctx, void *addr, size_t len);
	void (*setmr)(ctx_t *ctx, conn_t *con);
	accp_t *(*server)(ctx_t *ctx, cfg_t *cfg);
	int (*listen)(accp_t *a);
	int (*accept)(accp_t *a, conn_t **con);
	int (*client)(ctx_t *ctx, cfg_t *cfg, conn_t **c);
	int (*connect)(conn_t *c);
	int (*connect_qp)(conn_t *c);
	int (*send)(conn_t *c, struct iovec *iov, int iovn, void *ctx);
	int (*recv)(conn_t *c, struct iovec *iov, int iovn, void *ctx);
	int (*poll)(conn_t *c, struct ibv_wc *wc, int nwc);
	void (*destroy_conn)(conn_t *c);
	void (*destroy_accp)(accp_t *a);
	bool (*is_connected)(struct conn_t *c);
	int (*local_addr)(conn_t *c, addr_t *addr);
	int (*remote_addr)(conn_t *c, addr_t *addr);
} rdma_t;

rdma_t *new_rdma();

#ifdef __cplusplus
}
#endif

#endif // NM_RDMA_H_
