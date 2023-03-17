#!/bin/bash

ok=$(ibv_devinfo 2>&1 | grep No)
if [ "$ok" != "" ]
then
	modprobe rdma_rxe
	rdma link add rxe_0 type rxe netdev wlp37s0
fi
