/*********************************************************
	┊ File Name:bw.c
	┊ Author: Abby Cin
	┊ Mail: abbytsing@gmail.com
	┊ Created Time: Sun 29 Aug 2021 12:00:12 PM CST
**********************************************************/

#include "rdma.h"
#include <assert.h>
#include <bits/getopt_core.h>
#include <infiniband/verbs.h>
#include <sched.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <getopt.h>
#include <sys/time.h>
#include <sys/syscall.h>

/*
Server: ib_send_bw -D5 -F -s 4096 -b
---------------------------------------------------------------------------------------
		    Send Bidirectional BW Test
 Dual-port       : OFF          Device         : mlx5_0
 Number of qps   : 1            Transport type : IB
 Connection type : RC           Using SRQ      : OFF
 PCIe relax order: ON
 ibv_wr* API     : ON
 TX depth        : 128
 RX depth        : 512
 CQ Moderation   : 100
 Mtu             : 4096[B]
 Link type       : IB
 Max inline data : 0[B]
 rdma_cm QPs     : OFF
 Data ex. method : Ethernet
---------------------------------------------------------------------------------------
 local address: LID 0x18 QPN 0x04da PSN 0x8635a
 remote address: LID 0x13 QPN 0x16490 PSN 0xd64f07
---------------------------------------------------------------------------------------
 #bytes     #iterations    BW peak[MB/sec]    BW average[MB/sec]   MsgRate[Mpps]
 4096       8758300          0.00 11403.46                   2.919285
---------------------------------------------------------------------------------------
*/

/*
Client: ib_send_bw -D5 -F -s 4096 -b 1.1.1.42
---------------------------------------------------------------------------------------
		    Send Bidirectional BW Test
 Dual-port       : OFF          Device         : mlx5_0
 Number of qps   : 1            Transport type : IB
 Connection type : RC           Using SRQ      : OFF
 PCIe relax order: ON
 ibv_wr* API     : ON
 TX depth        : 128
 RX depth        : 512
 CQ Moderation   : 100
 Mtu             : 4096[B]
 Link type       : IB
 Max inline data : 0[B]
 rdma_cm QPs     : OFF
 Data ex. method : Ethernet
---------------------------------------------------------------------------------------
 local address: LID 0x13 QPN 0x16490 PSN 0xd64f07
 remote address: LID 0x18 QPN 0x04da PSN 0x8635a
---------------------------------------------------------------------------------------
 #bytes     #iterations    BW peak[MB/sec]    BW average[MB/sec]   MsgRate[Mpps]
 4096       8768313          0.00 11402.81                   2.919120
---------------------------------------------------------------------------------------
*/

/*
# server
# ./bw -c 0xc -n 2 -d 10 -s 4096
      size      4096 byte(s)
  duration      10 second(s)
      core      [2, 3]
     conns      2
      port      8888
--------------------------------------------
rdma.c:492 1.1.2.5 => 0
rdma.c:492 1.1.1.5 => 1
bw.c:460 982907 conn 0xb2a680 ldev 0 rdev 0
bw.c:460 982942 conn 0xb2bec0 ldev 1 rdev 1
tx: 256317431808, rx: 260526317568, bw: 44809.13MB/s iops: 11471137
*/

/*
# client
# ./bw -c 0xc -n 2 -d 10 -s 4096 1.1.2.5 1.1.1.5
      size      4096 byte(s)
  duration      10 second(s)
      core      [2, 3]
     conns      2
      port      8888
     ip[0]      1.1.2.5
     ip[1]      1.1.1.5
--------------------------------------------
rdma.c:492 1.1.1.6 => 0
rdma.c:492 1.1.2.6 => 1
bw.c:460 924352 conn 0xa0e280 ldev 0 rdev 0
bw.c:460 924388 conn 0xa0fac0 ldev 1 rdev 1
tx: 260527063040, rx: 256318402560, bw: 44809.27MB/s iops: 11471173
*/

#define TX_DEPTH 128
#define RX_DEPTH 256
#define POLL_SZ 32
#define MAX_CORE 8
#define MAX_IP 2
#define MAX_CONN 10
#define CQ_MOD 100
#define PAGE_SZ 4096UL
#define MAX_SZ (1UL << 20)

#define min(x, y) ((x) > (y) ? (y) : (x))
#define tid() ((int)syscall(SYS_gettid))
static pthread_spinlock_t g_lk;

#define debug(f, ...)                                                          \
	do {                                                                   \
		pthread_spin_lock(&g_lk);                                      \
		fprintf(stderr,                                                \
			"%s:%d %d " f "\n",                                    \
			__FILE__,                                              \
			__LINE__,                                              \
			tid(),                                                 \
			##__VA_ARGS__);                                        \
		pthread_spin_unlock(&g_lk);                                    \
	}                                                                      \
	while (0)

volatile int done;
struct timeval e;

struct param {
	uint32_t dur;
	size_t size;
	int cq_mod;
	bool use_seq;
	int card; // < 0 has no effect
	int port;
	int core;
	int conns;
	int nr_ip;
	int mask[MAX_CORE];
	char *ip[MAX_IP];
};

enum conn_state {
	CS_INIT,
	CS_DONE
};

struct conn_ctx {
	ctx_t *z;
	conn_t *con;
	char *tx_mem;
	char *rx_mem;
	enum conn_state state;
	int count;
	uint64_t cq_mod;
	uint64_t scnt;
	uint64_t ccnt;
	struct iovec rx_ctx[RX_DEPTH];
	struct iovec tx_ctx[TX_DEPTH];
	struct conn_ctx *next;
};

struct conn_mgr {
	struct conn_ctx *conns[MAX_CONN];
	ctx_t *z;
	int nr_conn;
};

struct thrd_msg {
	struct conn_ctx *head;
	size_t size;
	uint64_t io;
	uint64_t tx_sz;
	uint64_t rx_sz;
};

void ring(int sig)
{
	(void)sig;
	done = 1;
	gettimeofday(&e, NULL);
}

static void *client_func(void *arg);

void launch_server(struct param *param)
{
	struct conn_mgr m;
	accp_t *acceptor = NULL;
	conn_t *conn = NULL;
	int rc = 0;
	cfg_t cfg = {
		.use_srq = param->use_seq,
		.ib_dev = param->card,
		.cap_send = TX_DEPTH,
		.cap_recv = RX_DEPTH, // ignored
		.ib_port = 1,
		.ip = "0.0.0.0",
		.max_wr = RX_DEPTH * MAX_CONN,
		.port = param->port,
	};
	if (!param->use_seq)
		cfg.max_wr = RX_DEPTH;

	rdma_t *rdma = new_rdma();
	uint64_t per_conn_mem = param->size * (RX_DEPTH + TX_DEPTH);
	size_t mem_sz = MAX_CONN * per_conn_mem;
	void *mem = NULL;
	struct conn_ctx *worker[MAX_CORE] = { 0 };
	struct conn_ctx *tail[MAX_CORE] = { 0 };
	struct thrd_msg *msg[MAX_CORE] = { 0 };
	pthread_t thrd[MAX_CORE] = { 0 };
	pthread_attr_t attr;
	int nr_thrd = 0;
	struct timeval b;
	uint64_t tx = 0;
	uint64_t rx = 0;
	double sum = .0;
	double elapsed = .0;
	uint64_t io = 0;
	int nr_conn = 0;
	cpu_set_t mask;
	bool server_ready = false;

	bzero(&b, sizeof(b));
	bzero(&e, sizeof(e));
	bzero(&m, sizeof(m));

	CPU_ZERO(&mask);
	CPU_SET(param->mask[0], &mask);
	pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask);

	errno = 0;
	rc = posix_memalign(&mem, PAGE_SZ, mem_sz);
	if (rc) {
		debug("posix_memalign rc %d errno %d", rc, errno);
		return;
	}

	for (int i = 0; i < MAX_CONN; ++i) {
		m.conns[i] = calloc(1, sizeof(struct conn_ctx));
		assert(m.conns[i]);
		m.conns[i]->state = CS_INIT;
		m.conns[i]->tx_mem = (char *)mem + per_conn_mem * i;
		m.conns[i]->rx_mem =
			m.conns[i]->tx_mem + param->size * TX_DEPTH;
		m.conns[i]->count = TX_DEPTH;
		m.conns[i]->cq_mod = param->cq_mod;
	}

	rc = rdma->init(&m.z);
	if (rc) {
		debug("init failed");
		return;
	}

	rc = rdma->regmr(m.z, mem, mem_sz);
	if (rc) {
		debug("regmr failed");
		goto err;
	}

	acceptor = rdma->server(m.z, &cfg);
	if (!acceptor) {
		debug("server failed");
		goto err;
	}
	rc = rdma->listen(acceptor);
	if (rc) {
		debug("listen failed");
		goto err;
	}

	while (!server_ready) {
		rc = rdma->accept(acceptor, &conn);
		if (rc < 0) {
			debug("accept rc %d errno %d", rc, errno);
			goto err;
		}
		if (rc == 1)
			m.conns[m.nr_conn++]->con = conn;
		for (int i = 0; i < m.nr_conn; ++i) {
			if (m.conns[i]->state == CS_INIT) {
				rc = rdma->connect_qp(m.conns[i]->con);
				if (rc < 0) {
					debug("connect_qp rc %d errno %d",
					      rc,
					      errno);
					goto err;
				}
				if (rc == 1) {
					rdma->setmr(m.z, m.conns[i]->con);
					m.conns[i]->state = CS_DONE;
					nr_conn += 1;
					if (nr_conn == param->conns) {
						server_ready = true;
						break;
					}
				}
			}
		}
	}

	for (int i = 0, c = 0; i < m.nr_conn; ++i) {
		c %= param->core;
		if (!worker[c]) {
			worker[c] = m.conns[i];
			tail[c] = worker[i];
		} else {
			tail[c]->next = m.conns[i];
			tail[c] = tail[c]->next;
		}
		c += 1;
	}

	alarm(param->dur + 1);
	gettimeofday(&b, NULL);
	for (int i = 0; i < param->core; ++i) {
		if (!worker[i])
			continue;
		msg[i] = calloc(1, sizeof(struct thrd_msg));
		assert(msg[i]);
		msg[i]->head = worker[i];
		msg[i]->size = param->size;
		if (i == 0)
			continue;
		nr_thrd += 1;
		CPU_ZERO(&mask);
		CPU_SET(param->mask[i], &mask);
		pthread_attr_init(&attr);
		pthread_attr_setaffinity_np(&attr, sizeof(mask), &mask);
		pthread_create(&thrd[i], &attr, client_func, msg[i]);
		pthread_attr_destroy(&attr);
	}

	client_func(msg[0]);

	for (int i = 1; i < nr_thrd; ++i)
		pthread_join(thrd[i], NULL);

	for (int i = 0; i < param->core; ++i) {
		if (!msg[i])
			continue;
		tx += msg[i]->tx_sz;
		rx += msg[i]->rx_sz;
		io += msg[i]->io;
	}

	elapsed = (e.tv_sec - b.tv_sec) + (e.tv_usec - b.tv_usec) / 1e6;
	sum = (double)(tx + rx) / (1 << 20);
	printf("tx: %lu, rx: %lu, bw: %.2fMB/s iops: %2.f\n",
	       tx,
	       rx,
	       sum / elapsed,
	       io / elapsed);
err:
	free(mem);
	for (int i = 0; i < MAX_CONN; ++i) {
		if (m.conns[i]->con)
			rdma->destroy_conn(m.conns[i]->con);
		free(m.conns[i]);
	}
	for (int i = 0; i < MAX_CORE; ++i) {
		if (msg[i])
			free(msg[i]);
	}
	if (acceptor)
		rdma->destroy_accp(acceptor);
	if (m.z)
		rdma->exit(&m.z);
}

static int client_recv(rdma_t *rdma, struct conn_ctx *c, size_t size)
{
	int rc = 0;

	for (int i = 0; i < RX_DEPTH; ++i) {
		c->rx_ctx[i].iov_base = c->rx_mem + size * i;
		c->rx_ctx[i].iov_len = size;
		rc = rdma->recv(c->con, &c->rx_ctx[i], 1, &c->rx_ctx[i]);
		if (rc) {
			debug("recv wr %d rc %d errno %d", i, rc, errno);
			return -1;
		}
	}
	return 0;
}

static int client_send(rdma_t *rdma, struct conn_ctx *c, size_t size)
{
	int rc;
	bool notify;

	for (int i = 0; i < c->count; ++i) {
		c->tx_ctx[i].iov_base = c->tx_mem + size * i;
		c->tx_ctx[i].iov_len = size;
		notify = false;
		if (c->cq_mod == 0)
			notify = true;
		else if (c->scnt % c->cq_mod == c->cq_mod - 1)
			notify = true;
		rc = rdma->send(
			c->con, &c->tx_ctx[i], 1, &c->tx_ctx[i], notify);
		if (rc) {
			debug("send rc %d errno %d", rc, errno);
			return -1;
		}
		c->scnt += 1;
	}
	c->count = 0;
	return 0;
}

static int poll_send(rdma_t *rdma, struct conn_ctx *c, struct thrd_msg *m)
{
	static __thread struct ibv_wc wc[POLL_SZ];
	int n, delta = 1;

	if (c->cq_mod) {
		if (c->ccnt == c->scnt)
			return 0;
		delta = c->cq_mod;
	}
	n = rdma->poll_send(c->con, wc, POLL_SZ);
	if (n < 0) {
		debug("poll_send rc %d errno %d", n, errno);
		return -1;
	}
	for (int i = 0; i < n; ++i) {
		if (wc[i].status != IBV_WC_SUCCESS) {
			debug("send wc[%d] %s",
			      i,
			      ibv_wc_status_str(wc[i].status));
			return -1;
		}
		assert(wc[i].opcode == IBV_WC_SEND);
		m->io += delta;
		c->ccnt += delta;
		c->count += delta;
		m->tx_sz += (m->size * delta);
	}
	return 0;
}

static int poll_recv(rdma_t *rdma, struct conn_ctx *c, struct thrd_msg *m)
{
	static __thread struct ibv_wc wc[POLL_SZ];
	struct iovec *iov;
	int n, rc;

	n = rdma->poll_recv(c->con, wc, POLL_SZ);
	if (n < 0) {
		debug("poll_recv rc %d errno %d", n, errno);
		return -1;
	}

	for (int i = 0; i < n; ++i) {
		if (wc[i].status != IBV_WC_SUCCESS) {
			debug("recv wc[%d] %s",
			      i,
			      ibv_wc_status_str(wc[i].status));
			return -1;
		}
		m->io += 1;
		m->rx_sz += m->size;
		iov = (void *)wc[i].wr_id;
		rc = rdma->recv(c->con, iov, 1, iov);
		assert(rc == 0);
	}
	return 0;
}

static int
client_poll(rdma_t *rdma, struct conn_ctx *head, struct thrd_msg *msg)
{
	int rc = poll_send(rdma, head, msg);

	if (rc)
		return rc;
	return poll_recv(rdma, head, msg);
}

static struct conn_ctx *remove_node(struct conn_ctx *head, struct conn_ctx **n)
{
	struct conn_ctx dummy;
	struct conn_ctx *p = &dummy;
	p->next = head;

	while (p) {
		if (p->next == *n) {
			p->next = p->next->next;
			*n = p->next;
			break;
		}
		p = p->next;
	}
	return dummy.next;
}

static void *client_func(void *arg)
{
	struct thrd_msg *msg = arg;
	struct conn_ctx *head;
	int rc;
	rdma_t *rdma = new_rdma();
	size_t size = msg->size;

	head = msg->head;
	while (head) {
		debug("conn %p ldev %d rdev %d",
		      head,
		      head->con->dev,
		      head->con->rdev);
		rc = client_recv(rdma, head, size);
		if (rc)
			msg->head = remove_node(msg->head, &head);
		else
			head = head->next;
	}

	while (!done) {
		head = msg->head;
		while (head) {
			rc = client_send(rdma, head, size);
			if (rc)
				msg->head = remove_node(msg->head, &head);
			else
				head = head->next;
		}
		head = msg->head;
		while (head) {
			rc = client_poll(rdma, head, msg);
			if (rc)
				msg->head = remove_node(msg->head, &head);
			else
				head = head->next;
		}
	}
	return NULL;
}

static void launch_client(struct param *param)
{
	struct conn_mgr m;
	rdma_t *rdma = new_rdma();
	void *mem = NULL;
	int rc;
	uint64_t per_con_mem = param->size * (TX_DEPTH + RX_DEPTH);
	uint64_t mem_sz = param->conns * per_con_mem;
	cpu_set_t mask;
	struct conn_ctx *worker[MAX_CORE] = { 0 };
	struct conn_ctx *tail[MAX_CORE] = { 0 };
	struct thrd_msg *msg[MAX_CORE] = { 0 };
	pthread_t thrds[MAX_CORE] = { 0 };
	pthread_attr_t attr;
	struct timeval b;
	int nr_thrd = 0, nr_conn = 0;
	double elapsed, sum;

	bzero(&m, sizeof(m));
	CPU_ZERO(&mask);
	CPU_SET(param->mask[0], &mask);
	pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask);

	rc = posix_memalign(&mem, PAGE_SZ, mem_sz);
	if (rc) {
		debug("posix_memalign rc %d errno %d", rc, errno);
		return;
	}
	errno = 0;
	rc = rdma->init(&m.z);
	if (rc) {
		debug("init rc %d errno %d", rc, errno);
		goto err;
	}
	rc = rdma->regmr(m.z, mem, mem_sz);
	if (rc) {
		debug("regmr rc %d errno %d", rc, errno);
		goto err;
	}
	for (int i = 0; i < param->conns; ++i) {
		m.conns[i] = calloc(1, sizeof(*m.conns[i]));
		m.conns[i]->tx_mem = (char *)mem + i * per_con_mem;
		m.conns[i]->rx_mem =
			m.conns[i]->tx_mem + param->size * TX_DEPTH;
		m.conns[i]->count = TX_DEPTH;
		m.conns[i]->state = CS_INIT;
		m.conns[i]->cq_mod = param->cq_mod;
		assert(m.conns[i]);
	}

	for (int i = 0; i < param->conns; ++i) {
		cfg_t cfg = {
			.use_srq = param->use_seq,
			.ib_dev = param->card,
			.cap_send = TX_DEPTH,
			.cap_recv = RX_DEPTH,
			.ib_port = 1,
			.local_ip = m.z->map[i % MAX_IP].ip,
			.ip = param->ip[i % param->nr_ip],
			.max_wr = TX_DEPTH + RX_DEPTH,
			.port = param->port,
		};
		if (cfg.ib_dev > -1) {
			cfg.local_ip = m.z->map[0].ip;
			cfg.ip = param->ip[0];
		}
		rc = rdma->client(m.z, &cfg, &m.conns[i]->con);
		if (rc) {
			debug("client rc %d errno %d", rc, errno);
			goto err;
		}
		rdma->connect(m.conns[i]->con);
	}
	while (nr_conn < param->conns) {
		for (int i = 0; i < param->conns; ++i) {
			if (m.conns[i]->state != CS_INIT)
				continue;
			rc = rdma->connect_qp(m.conns[i]->con);
			if (rc < 0) {
				debug("connect_qp rc %d errno %d", rc, errno);
				goto err;
			}
			if (rc == 1) {
				nr_conn += 1;
				m.conns[i]->state = CS_DONE;
				rdma->setmr(m.z, m.conns[i]->con);
			}
		}
	}

	for (int i = 0, c = 0; i < param->conns; ++i) {
		c %= param->core;
		if (!worker[c]) {
			worker[c] = m.conns[i];
			tail[c] = worker[i];
		} else {
			tail[c]->next = m.conns[i];
			tail[c] = tail[c]->next;
		}
		c += 1;
	}

	alarm(param->dur + 1);
	gettimeofday(&b, NULL);
	for (int i = 0; i < param->core; ++i) {
		if (!worker[i])
			continue;
		msg[i] = calloc(1, sizeof(struct thrd_msg));
		assert(msg[i]);
		msg[i]->head = worker[i];
		msg[i]->size = param->size;
		if (i == 0)
			continue;
		nr_thrd += 1;
		CPU_ZERO(&mask);
		CPU_SET(param->mask[i], &mask);
		pthread_attr_init(&attr);
		pthread_attr_setaffinity_np(&attr, sizeof(mask), &mask);
		pthread_create(&thrds[i], &attr, client_func, msg[i]);
		pthread_attr_destroy(&attr);
	}

	client_func(msg[0]);

	uint64_t tx = 0, rx = 0, io = 0;

	for (int i = 1; i < nr_thrd; ++i)
		pthread_join(thrds[i], NULL);

	for (int i = 0; i < param->core; ++i) {
		if (!msg[i])
			continue;
		tx += msg[i]->tx_sz;
		rx += msg[i]->rx_sz;
		io += msg[i]->io;
	}
	elapsed = (e.tv_sec - b.tv_sec) + (e.tv_usec - b.tv_usec) / 1e6;
	sum = (double)(tx + rx) / (1 << 20);
	printf("tx: %lu, rx: %lu, bw: %.2fMB/s iops: %2.f\n",
	       tx,
	       rx,
	       sum / elapsed,
	       io / elapsed);

err:
	for (int i = 0; i < param->conns; ++i) {
		if (m.conns[i]->con)
			rdma->destroy_conn(m.conns[i]->con);
		free(m.conns[i]);
	}
	for (int i = 0; i < MAX_CORE; ++i) {
		if (msg[i])
			free(msg[i]);
	}

	free(mem);
	rdma->exit(&m.z);
}

static int parse_core(uint64_t core, int *mask)
{
	int i = 0;
	int idx = 0;

	do {
		if (core & 1)
			mask[idx++] = i;
		i += 1;
	}
	while (core >>= 1 && idx < MAX_CORE);
	return idx;
}

__attribute__((constructor)) static void __lockk_init(void)
{
	pthread_spin_init(&g_lk, 0);
}

__attribute__((destructor)) static void __lock_destroy(void)
{
	pthread_spin_destroy(&g_lk);
}

static void help(const char *cmd)
{
	printf("%s [-d dur] [-i dev] -c core -n conn [-p port] [-S] [-C] ip0 "
	       "[ip1]\n",
	       cmd);
	printf(" -h\tfor this help\n");
	printf(" -d\tduration, optional, 5s by default\n");
	printf(" -i\tdev, optional, round-robin all devices -1 by default\n");
	printf(" -c\tcores, required cpu mask of threads\n");
	printf(" -n\tnumber of connections, required\n");
	printf(" -p\tport, optional, 8888 by default\n");
	printf(" -S\tuse srq, optional, server side only, disabled by "
	       "default\n");
	printf(" -C\tcq_mod, optional, signal each send by default\n");
	printf(" ip0\tip of first IB device, required for client\n");
	printf(" ip1\tip of second IB device, optional for client\n");
}

int main(int argc, char *argv[])
{
	int opt = 0;
	char *endp = NULL;
	uint64_t core = 0;
	struct param param = {
		.dur = 5,
		.size = PAGE_SZ,
		.use_seq = false,
		.cq_mod = 0,
		.core = 1,
		.card = -1, // not specified
		.mask = { 0 },
		.port = 8888,
		.conns = 1,
		.ip = { 0, 0 },
	};

	while ((opt = getopt(argc, argv, "s:d:c:n:p:i:SCh")) != -1) {
		switch (opt) {
		case 's':
			param.size = strtol(optarg, &endp, 10);
			if (param.size < PAGE_SZ || param.size > MAX_SZ) {
				debug("size out of range, expect [%lu, %lu]",
				      PAGE_SZ,
				      MAX_SZ);
				return 1;
			}
			break;
		case 'S':
			param.use_seq = true;
			break;
		case 'd':
			param.dur = strtol(optarg, &endp, 10);
			if (param.dur > 60) {
				debug("duration out of range, expect [1, 60]");
				return 1;
			}
			break;
		case 'c':
			core = strtoul(optarg, &endp, 16);
			param.core = parse_core(core, param.mask);
			if (param.core == 0) {
				debug("invalid c, expect mask [0, 10]");
				return 1;
			}
			break;
		case 'C':
			param.cq_mod = CQ_MOD;
			break;
		case 'i':
			param.card = atoi(optarg);
			if (param.card >= MAX_IB_DEV || param.card < 0) {
				debug("invalid card, expect [0, 1]");
				return 1;
			}
			break;
		case 'n':
			// if connection greater than 1
			// round-robin ALL IB DEVICE and IP (when -i was not
			// speicified)
			param.conns = strtol(optarg, &endp, 10);
			if (param.conns > MAX_CONN || param.conns < 1) {
				debug("invalid n, expect [1, %d]", MAX_CONN);
				return 1;
			}
			break;
		case 'p':
			param.port = strtol(optarg, &endp, 10);
			if (param.port > 65000 || param.port < 1000) {
				debug("invalid p, expect [1000, 65000]");
				return 1;
			}
			break;
		case 'h':
		default:
			help(argv[0]);
			return 1;
		}
	}

	signal(SIGALRM, ring);
	for (int idx = optind, i = 0; i < MAX_IP && idx < argc; ++i, ++idx) {
		if (argv[idx])
			param.nr_ip += 1;
		param.ip[i] = argv[idx];
	}

	printf("%10s\t%lu byte(s)\n%10s\t%u second(s)\n",
	       "size",
	       param.size,
	       "duration",
	       param.dur);
	printf("%10s\t%s", "core", "[");
	for (int i = 0; i < param.core; ++i) {
		printf("%d", param.mask[i]);
		if (i + 1 != param.core)
			printf(", ");
	}
	printf("]\n");
	printf("%10s\t%d\n%10s\t%d\n",
	       "conns",
	       param.conns,
	       "port",
	       param.port);
	if (param.ip[0])
		printf("%10s\t%s\n", "ip[0]", param.ip[0]);
	if (param.ip[1])
		printf("%10s\t%s\n", "ip[1]", param.ip[1]);
	if (!param.ip[0])
		printf("%10s\t%s\n", "use_seq", param.use_seq ? "yes" : "no");
	printf("%10s\t%d\n", "IB", param.card);
	printf("%10s\t%d\n", "cq_mod", param.cq_mod);

	printf("--------------------------------------------\n");
	if (param.ip[0])
		launch_client(&param);
	else
		launch_server(&param);

	return 0;
}
