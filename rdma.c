/*********************************************************
	┊ File Name:rdma.c
	┊ Author: Abby Cin
	┊ Mail: abbytsing@gmail.com
	┊ Created Time: Sun 29 Aug 2021 12:01:14 PM CST
**********************************************************/

#include "rdma.h"
#include <rdma/rdma_verbs.h>
#include <assert.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <netdb.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef MULTI_SGE
#define MAX_SGE_NUM 31
#define MAX_SRQ_SGL 27
#else
#define MAX_SGE_NUM 1
#define MAX_SRQ_SGL 1
#endif

#define __min__(x, y) ((x) > (y) ? (y) : (x))

#define err(fmt, ...)                                                          \
	fprintf(stderr, "%s:%d " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__)

#define oops(x)                                                                \
	do {                                                                   \
		if ((x)) {                                                     \
			fprintf(stderr,                                        \
				"%s:%d (code %d)\n",                           \
				__func__,                                      \
				__LINE__,                                      \
				(x));                                          \
		}                                                              \
	}                                                                      \
	while (0)

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
static int __get_gid_index(struct ibv_context *ctx,
			   struct ibv_port_attr *attr,
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
	int dev;
};

enum conn_status {
	QP_INIT,
	QP_RTS,
	QP_VALIDATE,
	QP_DONE
};

struct conn_progress {
	enum conn_status status;
	int src_gid_idx;
	size_t offset;
	struct qp_token self;
	struct qp_token peer;
};

static int __create_socket(bool is_server,
			   const char *host,
			   const char *local,
			   const char *port)
{
	struct addrinfo *current = NULL;
	struct addrinfo hints;
	struct addrinfo *result = NULL;
	struct sockaddr_in addr = { 0 };
	int sockfd = -1;
	int option = 1;
	int rc;

	errno = 0;
	bzero(&hints, sizeof(struct addrinfo));
	hints.ai_canonname = NULL;
	hints.ai_addr = NULL;
	hints.ai_next = NULL;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_family = AF_UNSPEC;
	if (is_server)
		hints.ai_flags = AI_PASSIVE;

	rc = getaddrinfo(host, port, &hints, &result);
	if (rc != 0) {
		err("getaddrinfo: %s\n", gai_strerror(rc));
		return -1;
	}

	for (current = result; current != NULL; current = current->ai_next) {
		sockfd = socket(current->ai_family,
				current->ai_socktype,
				current->ai_protocol);
		if (sockfd == -1) {
			err("socket");
			continue;
		}

		if (setsockopt(sockfd,
			       SOL_SOCKET,
			       SO_REUSEADDR,
			       &option,
			       sizeof(option)) == -1) {
			close(sockfd);
			freeaddrinfo(result);
			err("setsockopt");
			return -1;
		}
		if (is_server) {
			if (bind(sockfd,
				 current->ai_addr,
				 current->ai_addrlen) == 0)
				break;
		} else {
			inet_pton(AF_INET, local, &addr.sin_addr);
			addr.sin_port = 0;
			addr.sin_family = AF_INET;
			rc = bind(
				sockfd, (struct sockaddr *)&addr, sizeof(addr));
			if (rc) {
				err("bind rc %d errno %d", rc, errno);
				close(sockfd);
				return -1;
			}
			if (connect(sockfd,
				    current->ai_addr,
				    current->ai_addrlen) == 0)
				break;
		}
		close(sockfd);
		sockfd = -1;
	}

	if (sockfd > 0)
		setsockopt(sockfd,
			   IPPROTO_TCP,
			   TCP_NODELAY,
			   &option,
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

static int
__qp_rtr(struct ibv_qp *qp, struct qp_token *token, uint8_t sgidx, int ib_port)
{
	struct ibv_qp_attr attr;
	int flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
		IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
		IBV_QP_MIN_RNR_TIMER;

	bzero(&attr, sizeof(attr));
	attr.qp_state = IBV_QPS_RTR;
	attr.path_mtu = IBV_MTU_4096;
	attr.dest_qp_num = token->qpn;
	attr.rq_psn = 0; // token->psn; // peer seq number
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
	attr.sq_psn = 0; // token->psn; // self seq number
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
			__func__,
			__LINE__,
			ib_port);
		return -1;
	}

	if (ibv_query_gid(qp->context, ib_port, p->src_gid_idx, &p->self.gid)) {
		err("ibv_query_gid");
		return -1;
	}

	p->self.lid = htons(attr.lid);
	p->self.psn = htonl(lrand48() & 0xffffff);
	p->self.qpn = htonl(qp->qp_num);
	p->self.dev = ctx->dev;

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
		ctx->rdev = p->peer.dev;
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
				err("fd: %d, error: %s",
				    ctx->sock,
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

static int build_map(ctx_t *z, char *ip, int tmo)
{
	struct rdma_cm_id *cm_id = NULL;
	struct rdma_event_channel *ch = NULL;
	struct rdma_cm_event *ev = NULL;
	struct rdma_addrinfo *ai = NULL;
	int rc = 0;

	errno = 0;
	rc = rdma_getaddrinfo(ip, NULL, NULL, &ai);
	if (rc) {
		err("rdma_getaddrinfo %s, rc %d errno %d", ip, rc, errno);
		return -1;
	}
	ch = rdma_create_event_channel();
	assert(ch);
	rc = rdma_create_id(ch, &cm_id, NULL, RDMA_PS_TCP);
	if (rc) {
		err("rdma_create_id %s, rc %d errno %d", ip, rc, errno);
		rc = -1;
		goto err;
	}

	for (struct rdma_addrinfo *i = ai; i; i = i->ai_next) {
		rc = rdma_resolve_addr(cm_id, NULL, i->ai_dst_addr, tmo);
		if (rc) {
			err("rdma_resolve_addr %s, rc %d errno %d",
			    ip,
			    rc,
			    errno);
			rc = -1;
			goto err;
		}
		break;
	}
	if (rdma_get_cm_event(ch, &ev) == -1) {
		err("rdma_get_cm_event %s, rc %d errno %d", ip, rc, errno);
		rc = -1;
		goto err;
	}
	if (ev->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
		// err("resolve addr fail %s, expect %s get %s", ip,
		// 	rdma_event_str(RDMA_CM_EVENT_ADDR_RESOLVED),
		// 	rdma_event_str(ev->event));
		rdma_ack_cm_event(ev);
		rc = -1;
		goto err;
	}
	rdma_ack_cm_event(ev);

	for (int i = 0; i < MAX_IB_DEV; ++i) {
		if (!z->verbs[i])
			continue;
		if (z->verbs[i]->device == cm_id->verbs->device) {
			err("%s => %d", ip, i);
			z->map[i].dev = i;
			bzero(z->map[i].ip, sizeof(z->map[i].ip));
			memcpy(z->map[i].ip, ip, strlen(ip));
			break;
		}
	}

err:
	if (ai)
		rdma_freeaddrinfo(ai);
	if (cm_id)
		rdma_destroy_id(cm_id);
	if (ch)
		rdma_destroy_event_channel(ch);
	return rc;
}

static int init_map(ctx_t *z)
{
	struct ifaddrs *ifaddr = NULL;
	struct ifaddrs *addr = NULL;
	int rc = 0;
	char ip[NI_MAXHOST];
	char buf[256] = { 0 };
	FILE *fp = popen("ibdev2netdev 2>/dev/null", "r");

	assert(fp);
	rc = fread(buf, sizeof(buf), 1, fp);
	rc = getifaddrs(&ifaddr);
	if (rc) {
		err("getifaddr rc %d errno %d", rc, errno);
		return 1;
	}

	addr = ifaddr;
	while (addr) {
		if (!addr->ifa_addr || addr->ifa_addr->sa_family != AF_INET)
			goto next;

		bzero(ip, sizeof(ip));
		rc = getnameinfo(addr->ifa_addr,
				 sizeof(struct sockaddr_in),
				 ip,
				 sizeof(ip),
				 NULL,
				 0,
				 NI_NUMERICHOST);
		if (rc != 0) {
			err("getnameinfo %s\n", gai_strerror(rc));
			goto err;
		}
		if (strstr(buf, addr->ifa_name)) {
			// err("%s => %s", addr->ifa_name, ip);
			build_map(z, ip, 1000);
		}
	next:
		addr = addr->ifa_next;
	}

	pclose(fp);
	freeifaddrs(ifaddr);
	return 0;
err:
	pclose(fp);
	freeifaddrs(ifaddr);
	return -1;
}

static int __impl_init(struct ctx_t **ctx)
{
	int num = 0;
	struct ibv_device **devs = NULL;
	struct ctx_t *z;

	if (*ctx)
		return 0;

	z = calloc(1, sizeof(struct ctx_t));
	devs = ibv_get_device_list(&num);
	if (num == 0) {
		err("ibv_get_device_list");
		goto failed;
	}

	errno = 0;
	if (num > MAX_IB_DEV)
		num = MAX_IB_DEV;
	for (int i = 0; i < num; ++i) {
		z->verbs[i] = ibv_open_device(devs[i]);
		if (!z->verbs[i]) {
			err("ibv_open_device[%d] errno %d", i, errno);
			continue;
		}
		z->nr_dev += 1;
	}

	if (z->nr_dev == 0) {
		err("no IB device available");
		goto failed;
	}

	for (int i = 0; i < MAX_IB_DEV; ++i) {
		if (!z->verbs[i])
			continue;
		z->pd[i] = ibv_alloc_pd(z->verbs[i]);
		if (!z->pd[i]) {
			err("ibv_alloc_pd errno %d", errno);
			goto failed;
		}
	}

	ibv_free_device_list(devs);
	if (init_map(z))
		goto failed;
	*ctx = z;
	return 0;
failed:
	if (devs)
		ibv_free_device_list(devs);
	for (int i = 0; i < MAX_IB_DEV; ++i) {
		if (z->pd[i])
			ibv_dealloc_pd(z->pd[i]);
		if (z->verbs[i])
			ibv_close_device(z->verbs[i]);
	}
	free(z);
	return -1;
}

static int __impl_exit(struct ctx_t **ctx)
{
	struct ctx_t *z = *ctx;

	for (int i = 0; i < MAX_IB_DEV; ++i) {
		if (z->mr[i])
			ibv_dereg_mr(z->mr[i]);
		if (z->pd[i])
			ibv_dealloc_pd(z->pd[i]);
		if (z->verbs[i])
			ibv_close_device(z->verbs[i]);
	}
	free(z);
	*ctx = NULL;

	return 0;
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

	if (n < 1) {
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

	if (attr.phys_port_cnt < 1) {
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

static int __impl_addrinfo(int sock, addr_t *res, bool local)
{
	static __thread struct sockaddr_in addr;
	static uint32_t size = sizeof(addr);
	static __thread char tmp[INET6_ADDRSTRLEN];
	int rc = 0;

	if (local)
		rc = getsockname(sock, (struct sockaddr *)&addr, &size);
	else
		rc = getpeername(sock, (struct sockaddr *)&addr, &size);

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
	return __impl_addrinfo(ctx->sock, res, true);
}

static inline int __impl_peerinfo(conn_t *ctx, addr_t *res)
{
	return __impl_addrinfo(ctx->sock, res, false);
}

static int __impl_regmr(ctx_t *ctx, void *addr, size_t len)
{
	struct ibv_mr *mr = NULL;

	// Send op requires only LOCAL_WRITE
	for (int i = 0; i < MAX_IB_DEV; ++i) {
		if (!ctx->pd[i])
			continue;
		mr = ibv_reg_mr(ctx->pd[i], addr, len, IBV_ACCESS_LOCAL_WRITE);
		if (!mr) {
			err("ibv_reg_mr");
			return -1;
		}
		ctx->mr[i] = mr;
	}
	return 0;
}

static void __impl_setmr(struct ctx_t *ctx, conn_t *con)
{
	assert(ctx->mr);
	con->mr = ctx->mr[con->dev];
}

static accp_t *__impl_server(ctx_t *z, cfg_t *cfg)
{
	int res = 0;
	char port_s[6] = { 0 };
	struct ibv_srq_init_attr srq_attr;
	accp_t *ctx = NULL;

	if (__validate_cfg(cfg))
		return NULL;

	ctx = calloc(1, sizeof(accp_t));
	snprintf(port_s, sizeof(port_s), "%d", cfg->port);
	res = __create_socket(true, cfg->ip, NULL, port_s);
	if (res == -1) {
		err("create_socket, ip:port => %s:%d", cfg->ip, cfg->port);
		goto failed;
	}
	ctx->z = z;
	ctx->listen_fd = res;
	if (cfg->use_srq) {
		bzero(&srq_attr, sizeof(srq_attr));
		srq_attr.attr.max_wr = cfg->max_wr;
		srq_attr.attr.max_sge = MAX_SRQ_SGL;
		for (int i = 0; i < MAX_IB_DEV; ++i) {
			if (!z->pd[i])
				continue;
			ctx->srq[i] = ibv_create_srq(z->pd[i], &srq_attr);
			if (!ctx->srq[i]) {
				err("ibv_create_srq");
				goto failed;
			}
		}
	}

	memcpy(&ctx->cfg, cfg, sizeof(cfg_t));

	srand48(getpid() * time(NULL));
	return ctx;

failed:
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

	return 0;
}

static int get_dev(ctx_t *z, const char *ip)
{
	for (int i = 0; i < MAX_IB_DEV; ++i) {
		if (strncmp(ip, z->map[i].ip, strlen(z->map[i].ip)) == 0)
			return z->map[i].dev;
	}
	return -1;
}

static int __impl_accept(accp_t *ctx, conn_t **conn)
{
	int res = -1;
	int flags = 0;
	conn_t *tmp = NULL;
	struct ibv_qp_init_attr qp_attr;
	struct conn_progress *prog = NULL;
	addr_t peer;
	int dev = 0;

	res = accept(ctx->listen_fd, NULL, NULL);
	if (res <= 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			return 0;
		err("accept");
		goto failed;
	}

	if (ctx->cfg.ib_dev > -1) {
		dev = ctx->cfg.ib_dev;
	} else {
		__impl_addrinfo(res, &peer, true);
		dev = get_dev(ctx->z, peer.ip);
	}
	assert(dev >= 0);

	tmp = calloc(1, sizeof(conn_t));
	memcpy(&tmp->cfg, &ctx->cfg, sizeof(cfg_t));
	tmp->sock = res;
	tmp->acceptor = ctx;
	tmp->dev = dev;
	tmp->send_cq = ibv_create_cq(
		ctx->z->verbs[dev], ctx->cfg.max_wr * 2, NULL, NULL, 0);
	if (!tmp->send_cq) {
		err("ibv_create_cq");
		goto failed;
	}
	tmp->recv_cq = ibv_create_cq(
		ctx->z->verbs[dev], ctx->cfg.max_wr * 2, NULL, NULL, 0);
	if (!tmp->recv_cq) {
		err("ibv_create_cq");
		goto failed;
	}
	bzero(&qp_attr, sizeof(qp_attr));
	qp_attr.qp_context = tmp;
	qp_attr.qp_type = IBV_QPT_RC;
	qp_attr.cap.max_send_wr = ctx->cfg.cap_send;
	qp_attr.cap.max_recv_wr = ctx->cfg.cap_recv;
	qp_attr.cap.max_send_sge = MAX_SGE_NUM;
	qp_attr.cap.max_recv_sge = MAX_SGE_NUM;
	qp_attr.cap.max_inline_data = 0;
	qp_attr.recv_cq = tmp->recv_cq;
	if (ctx->cfg.use_srq)
		qp_attr.srq = ctx->srq[dev];
	qp_attr.send_cq = tmp->send_cq;
	qp_attr.sq_sig_all = 0;
	tmp->qp = ibv_create_qp(ctx->z->pd[dev], &qp_attr);
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
	if (ctx->cfg.use_srq)
		tmp->srq = ctx->srq[dev];
	*conn = tmp;

	bzero(&peer, sizeof(peer));
	__impl_peerinfo(tmp, &peer);
	tmp->cfg.port = peer.port;

	return 1;
failed:
	close(tmp->sock);
	if (tmp->qp)
		ibv_destroy_qp(tmp->qp);
	if (tmp->send_cq)
		ibv_destroy_cq(tmp->send_cq);
	if (tmp->recv_cq)
		ibv_destroy_cq(tmp->recv_cq);

	if (tmp)
		free(tmp);

	return res;
}

static int __impl_client(struct ctx_t *z, cfg_t *cfg, conn_t **conn)
{
	int res = -1;
	int flags = 0;
	conn_t *tmp = NULL;
	int dev = 0;
	char port_s[6] = { 0 };

	if (__validate_cfg(cfg))
		return -1;
	if (cfg->ib_dev > -1)
		dev = cfg->ib_dev;
	else
		dev = get_dev(z, cfg->local_ip);
	assert(dev >= 0);

	tmp = calloc(1, sizeof(conn_t));

	snprintf(port_s, sizeof(port_s), "%d", cfg->port);
	res = __create_socket(false, cfg->ip, cfg->local_ip, port_s);
	if (res == -1) {
		err("create_socket, ip:port => %s:%d", cfg->ip, cfg->port);
		goto failed;
	}
	tmp->sock = res;
	res = -1;
	tmp->dev = dev;
	tmp->pd = z->pd[dev];

	tmp->send_cq =
		ibv_create_cq(z->verbs[dev], cfg->max_wr * 2, NULL, NULL, 0);
	if (!tmp->send_cq) {
		err("ibv_create_cq");
		goto failed;
	}
	tmp->recv_cq =
		ibv_create_cq(z->verbs[dev], cfg->max_wr * 2, NULL, NULL, 0);
	if (!tmp->recv_cq) {
		err("ibv_create_cq");
		goto failed;
	}
	memcpy(&tmp->cfg, cfg, sizeof(cfg_t));
	srand48(getpid() * time(NULL));

	flags = fcntl(tmp->sock, F_GETFL);
	fcntl(tmp->sock, F_SETFL, flags | O_NONBLOCK);

	*conn = tmp;

	return 0;

failed:
	if (tmp->send_cq)
		ibv_destroy_cq(tmp->send_cq);
	if (tmp->recv_cq)
		ibv_destroy_cq(tmp->recv_cq);

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
	qp_attr.recv_cq = ctx->recv_cq;
	qp_attr.send_cq = ctx->send_cq;
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
		close(ctx->sock);
		ctx->sock = -1;
		break;
	}
	return res;
}

static void __impl_destroy_conn(conn_t *ctx)
{
	if (ctx->progress)
		free(ctx->progress);

	if (ctx->qp)
		ibv_destroy_qp(ctx->qp);

	if (ctx->sock > 0)
		close(ctx->sock);

	if (ctx->send_cq)
		oops(ibv_destroy_cq(ctx->send_cq));
	if (ctx->recv_cq)
		oops(ibv_destroy_cq(ctx->recv_cq));
	free(ctx);
}

static void __impl_destroy_accp(accp_t *ctx)
{
	close(ctx->listen_fd);
	for (int i = 0; i < MAX_IB_DEV; ++i) {
		if (ctx->srq[i])
			oops(ibv_destroy_srq(ctx->srq[i]));
	}
	free(ctx);
}

static int
__impl_send(conn_t *ctx, struct iovec *iov, int n, void *data, bool notify)
{
	int rc = 0;
	static __thread struct ibv_sge sgl[MAX_SGE_NUM];
	struct ibv_send_wr wr;
	struct ibv_send_wr *bad_wr = NULL;

	assert(n <= MAX_SGE_NUM);
	for (int i = 0; i < n; ++i) {
		sgl[i].addr = (uint64_t)iov[i].iov_base;
		sgl[i].length = iov[i].iov_len;
		sgl[i].lkey = ctx->mr->lkey;
	}

	bzero(&wr, sizeof(wr));
	wr.opcode = IBV_WR_SEND;
	wr.send_flags = notify ? IBV_SEND_SIGNALED : 0;
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
		sgl[i].lkey = ctx->mr->lkey;
	}

	bzero(&wr, sizeof(wr));
	wr.next = NULL;
	wr.num_sge = n;
	wr.sg_list = sgl;
	wr.wr_id = (uint64_t)data;

	if (ctx->srq)
		rc = ibv_post_srq_recv(ctx->srq, &wr, &bad_wr);
	else
		rc = ibv_post_recv(ctx->qp, &wr, &bad_wr);

	if (rc) {
		errno = rc > 0 ? rc : -rc;
		err("ibv_post_recv srq %p", ctx->srq);
		return -1;
	}
	return 0;
}

static inline int __impl_poll_send(conn_t *conn, struct ibv_wc *wc, int nwc)
{
	return ibv_poll_cq(conn->send_cq, nwc, wc);
}

static inline int __impl_poll_recv(conn_t *conn, struct ibv_wc *wc, int nwc)
{
	return ibv_poll_cq(conn->recv_cq, nwc, wc);
}

rdma_t *new_rdma()
{
	static rdma_t r = {
		.init = __impl_init,
		.exit = __impl_exit,
		.regmr = __impl_regmr,
		.setmr = __impl_setmr,
		.server = __impl_server,
		.listen = __impl_listen,
		.accept = __impl_accept,
		.client = __impl_client,
		.connect = __impl_connect,
		.connect_qp = __impl_connect_qp,
		.send = __impl_send,
		.recv = __impl_recv,
		.poll_send = __impl_poll_send,
		.poll_recv = __impl_poll_recv,
		.destroy_conn = __impl_destroy_conn,
		.destroy_accp = __impl_destroy_accp,
		.local_addr = __impl_localinfo,
		.remote_addr = __impl_peerinfo,
	};

	return &r;
}
