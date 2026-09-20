# Makefile for compiling HuddleCluster eBPF XDP program
# Target: Linux Kernel with clang/llvm and libbpf

CLANG ?= clang
LLVM_STRIP ?= llvm-strip
ARCH := 

BPF_CFLAGS ?= -O2 -g -Wall -target bpf -D__TARGET_ARCH_

SRC := huddle_xdp.bpf.c
OBJ := huddle_xdp.bpf.o

.PHONY: all clean

all: 

: 
	  -c $< -o $@
	 -g $@

clean:
	rm -f 
