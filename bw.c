/*********************************************************
        ┊ File Name:bw.c
        ┊ Author: Abby Cin
        ┊ Mail: abbytsing@gmail.com
        ┊ Created Time: Sun 29 Aug 2021 12:00:12 PM CST
**********************************************************/

#include "rdma.h"
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <getopt.h>
#include <sys/time.h>

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
 4096       8758300          0.00               11403.46                   2.919285
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
 4096       8768313          0.00               11402.81                   2.919120
---------------------------------------------------------------------------------------
*/

#define TX_DEPTH 128
#define RX_DEPTH 512
#define POLL_SZ 32

#define debug(f, ...) fprintf(stderr, "%s:%d => " f "\n", __FILE__, \
        __LINE__, ##__VA_ARGS__)

volatile int done;
struct timeval e;

void ring(int sig)
{
        (void)sig;
        done = 1;
        gettimeofday(&e, NULL);
}

void server(uint32_t dur, size_t size)
{
        ctx_t *ctx = NULL;
        accp_t *acceptor = NULL;
        conn_t *conn = NULL;
        int rc = 0;
        cfg_t cfg = {
                .cap_send = TX_DEPTH,
                .cap_recv = TX_DEPTH, // ignored since SRQ is enabled
                .ib_dev = 0,
                .ib_port = 1,
                .ip = "0.0.0.0",
                .max_wr = 1024,
                .port = 8888
        };
        rdma_t *rdma = new_rdma();

        size_t memsize = size * (RX_DEPTH + TX_DEPTH);
        char *mem = NULL;
        int handle = -1;
        struct timeval b;
        struct ibv_wc wc[POLL_SZ];
        int n = 0;
        uint64_t tx = 0;
        uint64_t rx = 0;
        char *txmem = NULL;
        double sum = .0;
        double elapsed = .0;
        int count = TX_DEPTH;
        uint64_t io = 0;
        struct iovec rx_ctx[RX_DEPTH];
        struct iovec tx_ctx[TX_DEPTH];

        bzero(&b, sizeof(b));
        bzero(&e, sizeof(e));

        rc = rdma->init(&ctx);
        if (rc) {
                debug("init failed");
                return;
        }
        mem = malloc(memsize);
        if (!mem) {
                debug("malloc failed");
                goto out;
        }
        txmem = mem + size * RX_DEPTH;

        rc = rdma->regmr(ctx, mem, memsize, &handle);
        if (rc) {
                debug("regmr failed");
                goto out;
        }

        acceptor = rdma->server(ctx, &cfg);
        if (!acceptor) {
                debug("server failed");
                goto out;
        }
        rc = rdma->listen(acceptor);
        if (rc) {
                debug("listen failed");
                goto out;
        }
        printf("waiting for connection...");
        fflush(stdout);
        do {
                rc = rdma->accept(acceptor, &conn);
                if (rc < 0) {
                        debug("accept failed");
                        goto out;
                }
        } while (rc == 0);

        do {
                rc = rdma->connect_qp(conn);
                if (rc < 0) {
                        debug("connect qp failed");
                        goto out;
                }
        } while (rc == 0);

        printf(" connected.\n");
        rc = rdma->setmr(ctx, conn, handle);
        if (rc) {
                debug("setmr failed");
                goto out;
        }

        for (int i = 0; i < RX_DEPTH; ++i) {
                rx_ctx[i].iov_base = mem + size * i;
                rx_ctx[i].iov_len = size;
                rc = rdma->recv(conn, &rx_ctx[i], 1, &rx_ctx[i]);
                if (rc) {
                        debug("%d post recv failed", i);
                        goto out;
                }
        }

        alarm(dur);
        gettimeofday(&b, NULL);
        for (;!done;) {
                for (int i = 0; i < count && !done; ++i) {
                        tx_ctx[i].iov_base = txmem + size * i;
                        tx_ctx[i].iov_len = size;
                        rc = rdma->send(conn, &tx_ctx[i], 1, &tx_ctx[i]);
                        if (rc) {
                                debug("%d post send failed", i);
                                goto out;
                        }
                }

                do {
                        n = rdma->poll(conn, wc, POLL_SZ);
                        if (n < 0) {
                                debug("poll failed");
                                goto out;
                        }
                } while (n == 0 && !done);

                io += n;
                count = 0;
                for (int i = 0; i < n && !done; ++i) {
                        if (wc[i].status != IBV_WC_SUCCESS) {
                                debug("%s", ibv_wc_status_str(wc[i].status));
                                goto out;
                        }

                        if (wc[i].opcode == IBV_WC_SEND) {
                                tx += size;
                                count += 1;
                                continue;
                        }
                        rx += size;
                        struct iovec *iov = (void *)wc[i].wr_id;

                        rc = rdma->recv(conn, iov, 1, iov);
                        if (rc) {
                                debug("%d post recv failed", i);
                                goto out;
                        }
                }
        }

        elapsed = (e.tv_sec - b.tv_sec) + (e.tv_usec - b.tv_usec) / 1e6;
        sum = (tx + rx) / (1 << 20);
        printf("tx: %lu, rx: %lu, bw: %.2fMB/s iops: %2.f\n", tx, rx,
                sum / elapsed, io / elapsed);
out:
        free(mem);
        if (conn)
                rdma->destroy_conn(conn);
        if (acceptor)
                rdma->destroy_accp(acceptor);
        if (ctx)
                rdma->exit(&ctx);
}

void client(uint32_t dur, size_t size, char *ip)
{
        int rc = 0;
        cfg_t cfg = {
                .cap_send = TX_DEPTH,
                .cap_recv = RX_DEPTH,
                .ib_dev = 0,
                .ib_port = 1,
                .ip = ip,
                .max_wr = 1024,
                .port = 8888
        };

        rdma_t *rdma = new_rdma();
        ctx_t *ctx = NULL;
        conn_t *conn = NULL;
        size_t memsize = size * (RX_DEPTH + TX_DEPTH);
        char *mem = NULL;
        int handle = -1;
        struct timeval b;
        char *txmem = NULL;
        struct ibv_wc wc[POLL_SZ];
        int n = 0;
        uint64_t tx = 0;
        uint64_t rx = 0;
        double sum = .0;
        double elapsed = .0;
        int count = TX_DEPTH;
        uint64_t io = 0;
        struct iovec rx_ctx[RX_DEPTH];
        struct iovec tx_ctx[TX_DEPTH];

        bzero(&b, sizeof(b));
        bzero(&e, sizeof(e));

        rc = rdma->init(&ctx);
        if (rc) {
                debug("init failed");
                return;
        }
        mem = malloc(memsize);
        if (!mem) {
                debug("malloc failed");
                goto out;
        }
        txmem = mem + size * RX_DEPTH;

        rc = rdma->regmr(ctx, mem, memsize, &handle);
        if (rc) {
                debug("regmr failed");
                goto out;
        }
        rc = rdma->client(ctx, &cfg, &conn);
        if (rc) {
                debug("client failed");
                goto out;
        }
        printf("connecting to server...");
        fflush(stdout);
        rc = rdma->connect(conn);
        if (rc) {
                debug("connect failed");
                goto out;
        }

        do  {
                rc = rdma->connect_qp(conn);
                if (rc < 0) {
                        debug("connect_qp failed");
                        goto out;
                }
        } while (rc == 0);

        printf(" connected.\n");
        rc = rdma->setmr(ctx, conn, handle);
        if (rc) {
                debug("setmr failed");
                goto out;
        }

        for (int i = 0; i < RX_DEPTH; ++i) {
                rx_ctx[i].iov_base = mem + size * i;
                rx_ctx[i].iov_len = size;
                rc = rdma->recv(conn, &rx_ctx[i], 1, &rx_ctx[i]);
                if (rc) {
                        debug("%d post recv failed", i);
                        goto out;
                }
        }

        alarm(dur);
        gettimeofday(&b, NULL);
        for (;!done;) {
                for (int i = 0; i < count && !done; ++i) {
                        tx_ctx[i].iov_base = txmem + size * i;
                        tx_ctx[i].iov_len = size;
                        rc = rdma->send(conn, &tx_ctx[i], 1, &tx_ctx[i]);
                        if (rc) {
                                debug("%d post send failed", i);
                                goto out;
                        }
                }

                do {
                        n = rdma->poll(conn, wc, POLL_SZ);
                        if (n < 0) {
                                debug("poll failed");
                                goto out;
                        }
                } while (n == 0 && !done);

                io += n;
                count = 0;
                for (int i = 0; i < n && !done; ++i) {
                        if (wc[i].status != IBV_WC_SUCCESS) {
                                debug("%s", ibv_wc_status_str(wc[i].status));
                                goto out;
                        }

                        if (wc[i].opcode == IBV_WC_SEND) {
                                tx += size;
                                count += 1;
                                continue;
                        }
                        rx += size;
                        struct iovec *iov = (void *)wc[i].wr_id;

                        rc = rdma->recv(conn, iov, 1, iov);
                        if (rc) {
                                debug("%d post recv failed", i);
                                goto out;
                        }
                }
        }

        elapsed = (e.tv_sec - b.tv_sec) + (e.tv_usec - b.tv_usec) / 1e6;
        sum = (tx + rx) / (1 << 20);
        printf("tx: %lu, rx: %lu, bw: %.2fMB/s iops: %2.f\n", tx, rx,
                sum / elapsed, io / elapsed);

out:
        free(mem);
        if (conn)
                rdma->destroy_conn(conn);
        if (ctx)
                rdma->exit(&ctx);
}

int main(int argc, char *argv[])
{
        size_t size = 4096;
        uint32_t dur = 5;
        int opt = 0;
        char *endp = NULL;

        while ((opt = getopt(argc, argv, "d:s:h")) != -1) {
                switch (opt)
                {
                case 'd':
                        dur = strtol(optarg, &endp, 10);
                        if (dur > INT_MAX) {
                                debug("duration out of range");
                                return 1;
                        }
                        break;
                case 's':
                        size = strtoul(optarg, &endp, 10);
                        if (size == ULONG_MAX) {
                                debug("size out of range");
                                return 1;
                        }
                        break;
                case 'h':
                default:
                        printf("%s [-s size] [-h] [-d duration] [ip]", argv[0]);
                        return 1;
                }
        }

        printf("size: %lu byte(s), duration: %u second(s)\n", size, dur);
        signal(SIGALRM, ring);
        if (optind == argc - 1)
                client(dur, size, argv[optind]);
        else
                server(dur, size);

        return 0;
}
