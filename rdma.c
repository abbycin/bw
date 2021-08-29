/*********************************************************
        ┊ File Name:rdma.c
        ┊ Author: Abby Cin
        ┊ Mail: abbytsing@gmail.com
        ┊ Created Time: Sun 29 Aug 2021 12:01:14 PM CST
**********************************************************/

#include "rdma.h"
#include <assert.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <netdb.h>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_SGE_NUM 31
#define MAX_SRQ_SGL 27

#define __min__(x, y) ((x) > (y) ? (y) : (x))

#define err(fmt, ...) \
	fprintf(stderr, "[rdma] %s:%d (code %d) " fmt "\n", \
		__FILE__, __LINE__, errno, ##__VA_ARGS__)

#define oops(x) \
	do { \
		if ((x)) { \
			fprintf(stderr, "[rdma] %s:%d (code %d)\n", __func__, \
				__LINE__, (x)); \
		} \
	} while (0)

struct mr_ctx {
	struct ibv_mr *mr;
	int handle;
	uint32_t ref;
};

struct mr_queue {
	size_t cap;
	size_t size;
	struct mr_ctx *data;
	pthread_mutex_t mtx;
};

struct ctx_t {
	struct ibv_context *verbs;
	struct ibv_pd *pd;
	struct mr_queue *q;
};

struct accp_t {
	int listen_fd;
	cfg_t cfg;
	struct ibv_context *verbs;
	struct ibv_pd *pd;
	struct ibv_srq *srq;
};

struct conn_t {
	bool connected;
	int sock;
	uint32_t qp_num;
	cfg_t cfg;
	struct ibv_context *verbs;
	struct ibv_pd *pd;
	struct ibv_cq *cq;
	struct ibv_qp *qp;
	accp_t *acceptor;
	void *progress;
	void *mr;
};

#define __get_mr(x) (((struct mr_ctx *)x->mr)->mr)

static struct mr_queue *__init_queue(void)
{
	struct mr_queue *q = calloc(1, sizeof(struct mr_queue));

	q->cap = 1024;
	q->size = 0;
	q->data = calloc(q->cap, sizeof(*q->data));
	pthread_mutex_init(&q->mtx, NULL);
	return q;
}

static int __destroy_queue(struct mr_queue *q)
{
	int rc = 0;

	if (q) {
		for (size_t i = 0; i < q->size; ++i) {
			if (q->data[i].mr) {
				rc = ibv_dereg_mr(q->data[i].mr);
				if (rc) {
					errno = rc > 0 ? rc : -rc;
					return -1;
				}
				q->data[i].mr = NULL;
			}
		}
		free(q->data);
		free(q);
	}
	return rc;
}

static void __pushq(struct mr_queue *q, struct ibv_mr *mr, int *handle)
{
	assert(q);
	pthread_mutex_lock(&q->mtx);
	if (q->size + 1 == q->cap) {
		q->cap *= 2;

		size_t len = sizeof(struct mr_ctx);
		struct mr_ctx *tmp = calloc(q->cap, len);

		memcpy(tmp, q->data, q->size * len);
		free(q->data);
		q->data = tmp;
	}
	q->data[q->size].handle = *handle;
	q->data[q->size].mr = mr;
	// not set to 1, since it only incr by calling `__impl_setmr`
	// and decr by calling `__impl_deregmr`
	q->data[q->size].ref = 0;
	*handle = q->size;
	q->size += 1;
	pthread_mutex_unlock(&q->mtx);
}

static void __shareq(struct mr_queue *q, struct mr_ctx **ctx, uint32_t handle)
{
	assert(q);
	pthread_mutex_lock(&q->mtx);
	*ctx = &q->data[handle];
	(*ctx)->handle = handle;
	q->data[handle].ref += 1;
	pthread_mutex_unlock(&q->mtx);
}

static int __nullq(struct mr_queue *q, uint32_t handle)
{
	int rc = 0;

	assert(q);
	pthread_mutex_lock(&q->mtx);
	if (q->data[handle].mr) {
		rc = ibv_dereg_mr(q->data[handle].mr);
		q->data[handle].mr = NULL;
	}
	pthread_mutex_unlock(&q->mtx);
	if (rc > 0) {
		errno = rc;
		rc = -1;
	}
	return rc;
}

static int __unshareq(struct mr_queue *q, uint32_t handle)
{
	int ref = -1;

	pthread_mutex_lock(&q->mtx);
	if (q->data[handle].mr && q->data[handle].ref) {
		q->data[handle].ref -= 1;
		ref = q->data[handle].ref;
	}
	pthread_mutex_unlock(&q->mtx);
	if (ref < 0)
		err("unexpect ref, find bug in your code!");
	return ref;
}

static int __poll_ev(int fd, int count, int timeout)
{
	struct pollfd pfd = { .fd = fd, .events = POLLIN, .revents = 0 };
	int res = 0;

	do {
		res = poll(&pfd, 1, timeout);
		if (res == 0) {
			count -= 1;
			continue;
		}
		break;
	} while (count > 0);
	return res;
}

static void __gid2str(union ibv_gid *gid, char *data)
{
	uint32_t tmp[4] = { 0 };

	memcpy(tmp, gid, sizeof(tmp));
	for (int i = 0; i < 4; ++i)
		sprintf(&data[i * 8], "%08x", __bswap_32(tmp[i]));
}

static void __str2gid(const char *data, union ibv_gid *gid)
{
	char tmp[9] = { 0 };
	uint32_t tmpgid[4];
	uint32_t e = 0;

	for (int i = 0; i < 4; ++i) {
		memcpy(tmp, data + i * 8, 8);
		e = strtoul(tmp, NULL, 16);
		tmpgid[i] = __bswap_32(e);
	}
	memcpy(gid, tmpgid, sizeof(*gid));
}

// prefer RoCE v2
static int __get_gid_index(struct ibv_context *ctx, struct ibv_port_attr *attr,
			   int ib_port)
{
	int index = 0;
	union ibv_gid ga;
	union ibv_gid gb;
	struct ibv_gid_entry a;
	struct ibv_gid_entry b;
	// enum ibv_gid_type types; // IB => 0, RoCE v1 => 1, RoCE v2 => 2

	for (int i = 1; i < attr->gid_tbl_len; ++i) {
		if (ibv_query_gid(ctx, ib_port, index, &ga))
			return -1;
		if (ibv_query_gid(ctx, ib_port, i, &gb))
			return -1;

		if (ibv_query_gid_ex(ctx, ib_port, index, &a, 0))
			continue;

		if (ibv_query_gid_ex(ctx, ib_port, i, &b, 0))
			continue;

		// new RoCE version is not supported
		if (b.gid_type <= IBV_GID_TYPE_ROCE_V2 &&
		    a.gid_type <= b.gid_type) {
			index = i;
			if (b.gid_type == IBV_GID_TYPE_ROCE_V2)
				break;
		}
	}

	return index;
}

struct qp_token {
	uint16_t lid;
	uint32_t qpn;
	uint32_t psn;
	union ibv_gid gid;
	char gid_str[33];
};

enum conn_status { QP_INIT, QP_RTS, QP_VALIDATE, QP_DONE };

struct conn_progress {
	enum conn_status status;
	int src_gid_idx;
	size_t offset;
	struct qp_token self;
	struct qp_token peer;
};

static int __create_socket(bool is_server, const char *host, const char *port)
{
	struct addrinfo *current = NULL;
	struct addrinfo hints;
	struct addrinfo *result = NULL;
	int sockfd = -1;
	int option = 1;

	errno = 0;
	bzero(&hints, sizeof(struct addrinfo));
	hints.ai_canonname = NULL;
	hints.ai_addr = NULL;
	hints.ai_next = NULL;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_family = AF_UNSPEC;
	if (is_server)
		hints.ai_flags = AI_PASSIVE;

	getaddrinfo(host, port, &hints, &result);

	for (current = result; current != NULL; current = current->ai_next) {
		sockfd = socket(current->ai_family, current->ai_socktype,
				current->ai_protocol);
		if (sockfd == -1) {
			err("socket");
			continue;
		}

		if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &option,
			       sizeof(option)) == -1) {
			close(sockfd);
			freeaddrinfo(result);
			err("setsockopt");
			return -1;
		}
		if (is_server) {
			if (bind(sockfd, current->ai_addr,
				 current->ai_addrlen) == 0)
				break;
		} else {
			if (connect(sockfd, current->ai_addr,
				    current->ai_addrlen) == 0)
				break;
		}
		close(sockfd);
		sockfd = -1;
	}

	if (sockfd > 0)
		setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &option,
			   sizeof(option));
	freeaddrinfo(result);
	return sockfd;
}

static int __qp_init(struct ibv_qp *qp, uint8_t port)
{
	struct ibv_qp_attr attr;
	int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
		    IBV_QP_ACCESS_FLAGS;

	bzero(&attr, sizeof(attr));
	attr.qp_state = IBV_QPS_INIT;
	attr.pkey_index = 0;
	attr.port_num = port;
	attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE;
	return ibv_modify_qp(qp, &attr, flags);
}

static int __qp_rtr(struct ibv_qp *qp, struct qp_token *token, uint8_t sgidx,
		    int ib_port)
{
	struct ibv_qp_attr attr;
	int flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
		    IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
		    IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

	bzero(&attr, sizeof(attr));
	attr.qp_state = IBV_QPS_RTR;
	attr.path_mtu = IBV_MTU_4096;
	attr.dest_qp_num = token->qpn;
	attr.rq_psn = 0; //token->psn; // peer seq number
	attr.max_dest_rd_atomic = 1;
	attr.min_rnr_timer = 12;
	attr.ah_attr.is_global = 0;
	attr.ah_attr.dlid = token->lid;
	attr.ah_attr.sl = 0;
	attr.ah_attr.src_path_bits = 0;
	attr.ah_attr.port_num = ib_port;

	if (token->gid.global.interface_id) {
		attr.ah_attr.is_global = 1; // RoCE v2
		attr.ah_attr.grh.hop_limit = 0xff;
		attr.ah_attr.grh.dgid = token->gid;
		attr.ah_attr.grh.sgid_index = sgidx; // self gid index
	}

	return ibv_modify_qp(qp, &attr, flags);
}

static int __qp_rts(struct ibv_qp *qp, struct qp_token *token)
{
	struct ibv_qp_attr attr;
	int flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
		    IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;

	bzero(&attr, sizeof(attr));
	attr.qp_state = IBV_QPS_RTS;
	attr.timeout = 12;
	// 7 means retry infinitely
	attr.retry_cnt = 7;
	attr.rnr_retry = 7;
	(void)token;
	attr.sq_psn = 0; //token->psn; // self seq number
	attr.max_rd_atomic = 1;

	return ibv_modify_qp(qp, &attr, flags);
}

static int __send_token(conn_t *ctx, struct conn_progress *p)
{
	int nbytes = 0;
	int size = sizeof(p->self);
	int ib_port = ctx->cfg.ib_port;
	int sock = ctx->sock;
	struct ibv_qp *qp = ctx->qp;
	struct ibv_port_attr attr;

	bzero(&p->self, sizeof(p->self));
	bzero(&attr, sizeof(attr));
	if (ibv_query_port(qp->context, ib_port, &attr)) {
		err("ibv_query_port");
		return -1;
	}

	p->src_gid_idx = __get_gid_index(qp->context, &attr, ib_port);
	if (p->src_gid_idx < 0) {
		fprintf(stderr,
			"rdma %s:%d => can't get gid index of ib_port: %d\n",
			__func__, __LINE__, ib_port);
		return -1;
	}

	if (ibv_query_gid(qp->context, ib_port, p->src_gid_idx, &p->self.gid)) {
		err("ibv_query_gid");
		return -1;
	}

	p->self.lid = htons(attr.lid);
	p->self.psn = htonl(lrand48() & 0xffffff);
	p->self.qpn = htonl(qp->qp_num);

	__gid2str(&p->self.gid, p->self.gid_str);
	nbytes = write(sock, &p->self, size);
	if (nbytes < size) {
		err("incomplete write");
		return -1;
	}

	return 0;
}

static int __recv_token(conn_t *ctx, struct conn_progress *p)
{
	uint8_t ib_port = ctx->cfg.ib_port;
	int n = 0;
	size_t size = sizeof(p->peer);
	char *peer = (char *)&p->peer;

	n = read(ctx->sock, peer + p->offset, size - p->offset);
	if (n < 0) {
		if (errno != EAGAIN && errno != EWOULDBLOCK) {
			err("fd: %d, error: %s", ctx->sock, strerror(errno));
			return -1;
		}
	} else
		p->offset += n;

	if (n == 0) {
		errno = ENOTCONN;
		err("fd: %d, error: %s", ctx->sock, strerror(errno));
		return -1;
	}

	assert(p->offset <= size);
	if (p->offset == size) {
		p->peer.lid = htons(p->peer.lid);
		p->peer.psn = htonl(p->peer.psn);
		p->peer.qpn = htonl(p->peer.qpn);
		__str2gid(p->peer.gid_str, &p->peer.gid);
		if (__qp_init(ctx->qp, ib_port)) {
			err("modify qp to init");
			return -1;
		}
		if (__qp_rtr(ctx->qp, &p->peer, p->src_gid_idx, ib_port)) {
			err("modify qp to rtr");
			return -1;
		}
		if (__qp_rts(ctx->qp, &p->self)) {
			err("modify qp to rts");
			return -1;
		}
	}
	return p->offset == size ? 1 : 0;
}

static int __validate_token(conn_t *ctx)
{
	struct conn_progress *p = ctx->progress;
	int rc = 0;
	char *qpn = (char *)&p->peer.qpn;
	size_t size = sizeof(p->peer.qpn);

	errno = 0;
	if (ctx->acceptor) {
		rc = write(ctx->sock, &p->peer.qpn, sizeof(p->peer.qpn));
		// most likely EPIPE if error occured
		if (errno) {
			err("incompete write");
			return -1;
		}
	} else {
		rc = read(ctx->sock, qpn + p->offset, size - p->offset);
		if (rc < 0) {
			if (errno != EAGAIN && errno != EWOULDBLOCK) {
				err("fd: %d, error: %s", ctx->sock,
					strerror(errno));
				return -1;
			}
			return 0;
		}

		if (rc == 0) {
			errno = ENOTCONN;
			err("fd: %d's peer disconncted", ctx->sock);
			return -1;
		}

		p->offset += rc;
		if (p->offset < size) {
			// continue read
			return 0;
		}
		if (p->peer.qpn != htonl(p->self.qpn)) {
			errno = EBADE;
			err("unexpect token");
			return -1;
		}
	}
	return 1;
}

static int __impl_init(struct ctx_t **ctx)
{
	int num = 0;
	struct ibv_device **devs = NULL;

	if (*ctx)
		return 0;

	*ctx = calloc(1, sizeof(struct ctx_t));
	devs = ibv_get_device_list(&num);
	if (num == 0) {
		err("ibv_get_device_list");
		goto failed;
	}

	(*ctx)->verbs = ibv_open_device(devs[0]);
	if (!(*ctx)->verbs) {
		err("ibv_open_device");
		goto failed;
	}

	(*ctx)->pd = ibv_alloc_pd((*ctx)->verbs);
	if (!(*ctx)->pd) {
		err("ibv_alloc_pd");
		goto failed;
	}

	ibv_free_device_list(devs);
	(*ctx)->q = __init_queue();

	return 0;
failed:
	if (devs)
		ibv_free_device_list(devs);
	if ((*ctx)->pd)
		ibv_dealloc_pd((*ctx)->pd);
	if ((*ctx)->verbs)
		ibv_close_device((*ctx)->verbs);
	free(*ctx);
	*ctx = NULL;
	return -1;
}

static int __impl_exit(struct ctx_t **ctx)
{
	int rc = 0;

	rc = __destroy_queue((*ctx)->q);
	if (rc) {
		err("ibv_dereg_mr");
		return -1;
	}
	if ((*ctx)->pd) {
		rc = ibv_dealloc_pd((*ctx)->pd);
		if (rc) {
			errno = rc > 0 ? rc : -rc;
			err("ibv_dealloc_pd");
			goto failed;
		}
		(*ctx)->pd = NULL;
	}

	if ((*ctx)->verbs) {
		rc = ibv_close_device((*ctx)->verbs);
		if (rc) {
			err("ibv_close_device");
			goto failed;
		}
		(*ctx)->verbs = NULL;
	}
	free(*ctx);
	*ctx = NULL;

	return 0;
failed:
	return -1;
}

static int __validate_cfg(cfg_t *cfg)
{
	int res = 0;
	int n = 0;
	int min_wr = 0;
	struct ibv_device_attr attr;
	struct ibv_context *ctx = NULL;
	struct ibv_device **devices = NULL;

	devices = ibv_get_device_list(&n);
	assert(n > 0);
	if (!devices) {
		err("ibv_get_device_list");
		res = -1;
		goto failed;
	}

	if (n < cfg->ib_dev) {
		err("ib_dev out of range");
		res = -1;
		goto failed;
	}
	ctx = ibv_open_device(devices[0]);

	if (!ctx) {
		res = -1;
		err("ibv_open_devices");
		goto failed;
	}

	bzero(&attr, sizeof(attr));
	res = ibv_query_device(ctx, &attr);
	if (res) {
		err("ibv_query_device");
		goto failed;
	}

	if (attr.phys_port_cnt < cfg->ib_port) {
		err("ib_port out of range");
		res = -1;
		goto failed;
	}

	min_wr = __min__(attr.max_qp_wr, attr.max_srq_wr);
	if (cfg->max_wr * 2 >= (size_t)min_wr)
		cfg->max_wr = min_wr / 2 - 1;

	if (cfg->cap_send == 0)
		cfg->cap_send = 100;

failed:
	if (ctx)
		ibv_close_device(ctx);

	if (devices)
		ibv_free_device_list(devices);

	return res;
}

static int __impl_addrinfo(conn_t *ctx, addr_t *res, bool local)
{
	static __thread struct sockaddr_in addr;
	static uint32_t size = sizeof(addr);
	static __thread char tmp[INET6_ADDRSTRLEN];
	int rc = 0;

	if (local)
		rc = getsockname(ctx->sock, (struct sockaddr *)&addr, &size);
	else
		rc = getpeername(ctx->sock, (struct sockaddr *)&addr, &size);

	if (rc)
		return rc;
	bzero(tmp, sizeof(tmp));
	inet_ntop(AF_INET, &addr.sin_addr.s_addr, tmp, sizeof(tmp));
	memcpy(res->ip, tmp, sizeof(res->ip));
	res->port = htons(addr.sin_port);
	return 0;
}

static inline int __impl_localinfo(conn_t *ctx, addr_t *res)
{
	return __impl_addrinfo(ctx, res, true);
}

static inline int __impl_peerinfo(conn_t *ctx, addr_t *res)
{
	return __impl_addrinfo(ctx, res, false);
}


static int __impl_regmr(ctx_t *ctx, void *addr, size_t len, int *handle)
{
	assert(handle);
	if (!handle) {
		errno = EINVAL;
		err("handle can't be NULL");
		return -1;
	}

	*handle = UINT32_MAX;

	struct ibv_mr *mr = NULL;

	// Send op requires only LOCAL_WRITE
	mr = ibv_reg_mr(ctx->pd, addr, len, IBV_ACCESS_LOCAL_WRITE);
	if (!mr) {
		err("ibv_reg_mr");
		return -1;
	}

	__pushq(ctx->q, mr, handle);
	return 0;
}

static int __impl_unsetmr(struct ctx_t *ctx, conn_t *con)
{
	struct mr_ctx *mr = con->mr;

	if (!mr || mr->handle < 0)
		return -1;

	int rc = -1;
	int ref = __unshareq(ctx->q, mr->handle);

	if (ref < 0)
		return -1;
	if (ref == 0)
		rc = __nullq(ctx->q, mr->handle);
	else
		return 0; // still in use
	return rc == 0 ? 1 : -1;
}

static int __impl_setmr(struct ctx_t *ctx, conn_t *con,	int handle)
{
	int rc = 0;

	if (handle < 0) {
		errno = EINVAL;
		err("invalid handle");
		return -1;
	}
	if (con->mr)
		rc = __impl_unsetmr(ctx, con);
	if (rc < 0)
		return rc;
	__shareq(ctx->q, (struct mr_ctx **)&con->mr, handle);
	return 0;
}

static accp_t *__impl_server(ctx_t *z, cfg_t *cfg)
{
	int res = 0;
	char port_s[6] = { 0 };
	struct ibv_srq_init_attr srq_attr;
	accp_t *ctx = NULL;

	if (__validate_cfg(cfg))
		return NULL;

	assert(z->verbs && z->pd);
	ctx = calloc(1, sizeof(accp_t));
	ctx->verbs = z->verbs;
	snprintf(port_s, sizeof(port_s), "%d", cfg->port);
	res = __create_socket(true, cfg->ip, port_s);
	if (res == -1) {
		err("create_socket, ip:port => %s:%d", cfg->ip, cfg->port);
		goto failed;
	}
	ctx->listen_fd = res;
	ctx->pd = z->pd;
	bzero(&srq_attr, sizeof(srq_attr));
	srq_attr.attr.max_wr = cfg->max_wr;
	srq_attr.attr.max_sge = MAX_SRQ_SGL;
	ctx->srq = ibv_create_srq(ctx->pd, &srq_attr);
	if (!ctx->srq) {
		err("ibv_create_srq");
		goto failed;
	}

	memcpy(&ctx->cfg, cfg, sizeof(cfg_t));

	srand48(getpid() * time(NULL));
	return ctx;

failed:
	if (ctx->srq)
		ibv_destroy_srq(ctx->srq);

	if (ctx->listen_fd)
		close(ctx->listen_fd);

	free(ctx);

	return NULL;
}

static int __impl_listen(accp_t *ctx)
{
	int res = 0;
	int flags = 0;

	res = listen(ctx->listen_fd, SOMAXCONN);
	if (res) {
		err("listen");
		return -1;
	}

	flags = fcntl(ctx->listen_fd, F_GETFL);
	fcntl(ctx->listen_fd, F_SETFL, flags | O_NONBLOCK);
	flags = fcntl(ctx->verbs->async_fd, F_GETFL);
	fcntl(ctx->verbs->async_fd, F_SETFL, flags | O_NONBLOCK);

	return 0;
}

static int __impl_accept(accp_t *ctx, conn_t **conn)
{
	int res = -1;
	int flags = 0;
	conn_t *tmp = NULL;
	struct ibv_qp_init_attr qp_attr;
	struct conn_progress *prog = NULL;
	addr_t peer;

	res = accept(ctx->listen_fd, NULL, NULL);
	if (res <= 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			return 0;
		err("accept");
		goto failed;
	}

	tmp = calloc(1, sizeof(conn_t));
	memcpy(&tmp->cfg, &ctx->cfg, sizeof(cfg_t));
	tmp->sock = res;
	tmp->acceptor = ctx;
	tmp->cq = ibv_create_cq(ctx->verbs, ctx->cfg.max_wr * 2, NULL, NULL, 0);
	if (!tmp->cq) {
		err("ibv_create_cq");
		goto failed;
	}
	tmp->verbs = ctx->verbs;
	bzero(&qp_attr, sizeof(qp_attr));
	qp_attr.qp_context = tmp;
	qp_attr.qp_type = IBV_QPT_RC;
	qp_attr.cap.max_send_wr = ctx->cfg.cap_send;
	qp_attr.cap.max_send_sge = MAX_SGE_NUM;
	qp_attr.cap.max_recv_sge = MAX_SGE_NUM;
	qp_attr.cap.max_inline_data = 0;
	qp_attr.recv_cq = tmp->cq;
	qp_attr.srq = ctx->srq;
	qp_attr.send_cq = tmp->cq;
	qp_attr.sq_sig_all = 0;
	tmp->qp = ibv_create_qp(ctx->pd, &qp_attr);
	if (!tmp->qp) {
		res = -1;
		err("ibv_create_qp");
		goto failed;
	}

	flags = fcntl(tmp->sock, F_GETFL);
	fcntl(tmp->sock, F_SETFL, flags | O_NONBLOCK);

	tmp->qp_num = tmp->qp->qp_num;
	prog = calloc(1, sizeof(*prog));
	prog->status = QP_INIT;
	tmp->progress = prog;
	*conn = tmp;

	bzero(&peer, sizeof(peer));
	__impl_peerinfo(tmp, &peer);
	tmp->cfg.port = peer.port;

	return 1;
failed:
	close(tmp->sock);
	if (tmp->qp)
		ibv_destroy_qp(tmp->qp);
	if (tmp->cq)
		ibv_destroy_cq(tmp->cq);

	if (tmp)
		free(tmp);

	return res;
}

static int __impl_client(struct ctx_t *z, cfg_t *cfg,  conn_t **conn)
{
	int res = -1;
	int flags = 0;
	conn_t *tmp = NULL;
	char port_s[6] = { 0 };

	if (__validate_cfg(cfg))
		return -1;

	assert(z->verbs && z->pd);
	tmp = calloc(1, sizeof(conn_t));
	tmp->verbs = z->verbs;

	snprintf(port_s, sizeof(port_s), "%d", cfg->port);
	res = __create_socket(false, cfg->ip, port_s);
	if (res == -1) {
		err("create_socket, ip:port => %s:%d", cfg->ip, cfg->port);
		goto failed;
	}
	tmp->sock = res;
	res = -1;
	tmp->pd = z->pd;

	tmp->cq = ibv_create_cq(tmp->verbs, cfg->max_wr * 2, NULL, NULL, 0);
	if (!tmp->cq) {
		err("ibv_create_cq");
		goto failed;
	}
	memcpy(&tmp->cfg, cfg, sizeof(cfg_t));
	srand48(getpid() * time(NULL));

	flags = fcntl(tmp->verbs->async_fd, F_GETFL);
	fcntl(tmp->verbs->async_fd, F_SETFL, flags | O_NONBLOCK);
	flags = fcntl(tmp->sock, F_GETFL);
	fcntl(tmp->sock, F_SETFL, flags | O_NONBLOCK);

	*conn = tmp;

	return 0;

failed:
	if (tmp->cq)
		ibv_destroy_cq(tmp->cq);

	if (tmp->sock)
		close(tmp->sock);

	if (tmp)
		free(tmp);

	return res;
}

static int __impl_connect(conn_t *ctx)
{
	int res = -1;
	struct ibv_qp_init_attr qp_attr;
	struct conn_progress *prog = NULL;

	bzero(&qp_attr, sizeof(qp_attr));
	qp_attr.qp_context = ctx;
	qp_attr.qp_type = IBV_QPT_RC;
	qp_attr.cap.max_send_wr = ctx->cfg.cap_send;
	qp_attr.cap.max_recv_wr = ctx->cfg.cap_recv;
	qp_attr.cap.max_send_sge = MAX_SGE_NUM;
	qp_attr.cap.max_recv_sge = MAX_SGE_NUM;
	qp_attr.cap.max_inline_data = 0;
	qp_attr.recv_cq = ctx->cq;
	qp_attr.send_cq = ctx->cq;
	qp_attr.sq_sig_all = 0;

	ctx->qp = ibv_create_qp(ctx->pd, &qp_attr);
	if (!ctx->qp) {
		err("ibv_create_qp");
		goto failed;
	}

	ctx->qp_num = ctx->qp->qp_num;
	prog = calloc(1, sizeof(*prog));
	prog->status = QP_INIT;
	ctx->progress = prog;
	return 0;

failed:
	close(ctx->sock);
	if (ctx->qp) {
		ibv_destroy_qp(ctx->qp);
		ctx->qp = NULL;
	}
	return res;
}

static int __impl_connect_qp(conn_t *ctx)
{
	struct conn_progress *prog = ctx->progress;
	int res = 0;

	switch (prog->status) {
	case QP_INIT:
		res = __send_token(ctx, prog);
		prog->status = QP_RTS;
		break;
	case QP_RTS:
		res = __recv_token(ctx, prog);
		if (res == 1) {
			prog->offset = 0; // reset for validating
			prog->status = QP_VALIDATE;
			res = 0; // next state
		}
		break;
	case QP_VALIDATE:
		res = __validate_token(ctx);
		if (res == -1)
			return res;

		if (ctx->acceptor) {
			prog->status = QP_DONE;
			ctx->connected = true;
			return 1; // passive side connected
		}

		if (res == 1) {
			prog->status = QP_DONE;
			ctx->connected = true;
		}
		break;
	case QP_DONE:
		res = 1;
		break;
	}
	return res;
}

static bool __impl_is_connected(conn_t *ctx)
{
	if (!ctx->connected)
		return false;

	return __poll_ev(ctx->sock, 0, 0) == 0;
}

static void __impl_destroy_conn(conn_t *ctx)
{
	if (ctx->progress)
		free(ctx->progress);

	if (ctx->qp)
		ibv_destroy_qp(ctx->qp);

	close(ctx->sock);

	if (ctx->cq)
		oops(ibv_destroy_cq(ctx->cq));
	free(ctx);
}

static void __impl_destroy_accp(accp_t *ctx)
{
	close(ctx->listen_fd);
	if (ctx->srq)
		oops(ibv_destroy_srq(ctx->srq));

	free(ctx);
}

static int __impl_send(conn_t *ctx, struct iovec *iov, int n, void *data)
{
	int rc = 0;
	static __thread struct ibv_sge sgl[MAX_SGE_NUM];
	struct ibv_send_wr wr;
	struct ibv_send_wr *bad_wr = NULL;

	assert(n <= MAX_SGE_NUM);
	for (int i = 0; i < n; ++i) {
		sgl[i].addr = (uint64_t)iov[i].iov_base;
		sgl[i].length = iov[i].iov_len;
		sgl[i].lkey = __get_mr(ctx)->lkey;
	}

	bzero(&wr, sizeof(wr));
	wr.opcode = IBV_WR_SEND;
	wr.send_flags = IBV_SEND_SIGNALED;
	wr.sg_list = sgl;
	wr.num_sge = n;
	wr.next = NULL;
	wr.wr_id = (uint64_t)data;

	rc = ibv_post_send(ctx->qp, &wr, &bad_wr);

	if (rc) {
		errno = rc > 0 ? rc : -rc;
		err("ibv_post_send");
		return -1;
	}
	return 0;
}

static int __impl_recv(conn_t *ctx, struct iovec *iov, int n, void *data)
{
	int rc = 0;
	static __thread struct ibv_sge sgl[MAX_SGE_NUM];
	struct ibv_recv_wr wr;
	struct ibv_recv_wr *bad_wr = NULL;

	assert(n <= MAX_SGE_NUM);
	for (int i = 0; i < n; ++i) {
		sgl[i].addr = (uint64_t)iov[i].iov_base;
		sgl[i].length = iov[i].iov_len;
		sgl[i].lkey = __get_mr(ctx)->lkey;
	}

	bzero(&wr, sizeof(wr));
	wr.next = NULL;
	wr.num_sge = n;
	wr.sg_list = sgl;
	wr.wr_id = (uint64_t)data;

	if (ctx->acceptor)
		rc = ibv_post_srq_recv(ctx->acceptor->srq, &wr, &bad_wr);
	else
		rc = ibv_post_recv(ctx->qp, &wr, &bad_wr);

	if (rc) {
		errno = rc > 0 ? rc : -rc;
		err("ibv_post_recv");
		return -1;
	}
	return 0;
}

static inline int __impl_poll(conn_t *conn, struct ibv_wc *wc, int nwc)
{
	return ibv_poll_cq(conn->cq, nwc, wc);
}

rdma_t *new_rdma()
{
	rdma_t *r = calloc(1, sizeof(rdma_t));

	if (!r)
		return NULL;

	r->init = __impl_init;
	r->exit = __impl_exit;
	r->regmr = __impl_regmr;
	r->setmr = __impl_setmr;
	r->server = __impl_server;
	r->listen = __impl_listen;
	r->accept = __impl_accept;
	r->client = __impl_client;
	r->connect = __impl_connect;
	r->connect_qp = __impl_connect_qp;
	r->send = __impl_send;
	r->recv = __impl_recv;
	r->poll = __impl_poll;
	r->destroy_conn = __impl_destroy_conn;
	r->destroy_accp = __impl_destroy_accp;
	r->is_connected = __impl_is_connected;
	r->local_addr = __impl_localinfo;
	r->remote_addr = __impl_peerinfo;

	return r;
}